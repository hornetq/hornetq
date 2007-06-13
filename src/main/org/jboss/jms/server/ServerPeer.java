/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.server;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.management.Attribute;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.xa.Xid;

import org.jboss.aop.AspectXmlLoader;
import org.jboss.jms.server.connectionfactory.ConnectionFactoryJNDIMapper;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.jms.server.connectormanager.SimpleConnectorManager;
import org.jboss.jms.server.destination.ManagedQueue;
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.messagecounter.MessageCounterManager;
import org.jboss.jms.server.plugin.contract.JMSUserManager;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.jms.server.security.SecurityMetadataStore;
import org.jboss.jms.wireformat.JMSWireFormat;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.memory.SimpleMemoryManager;
import org.jboss.messaging.core.message.SimpleMessageStore;
import org.jboss.messaging.core.plugin.IDManager;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.contract.ReplicationListener;
import org.jboss.messaging.core.plugin.contract.Replicator;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.FailoverStatus;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.Util;
import org.jboss.messaging.util.Version;
import org.jboss.mx.loading.UnifiedClassLoader3;
import org.jboss.remoting.marshal.MarshalFactory;
import org.jboss.system.ServiceCreator;
import org.jboss.system.ServiceMBeanSupport;
import org.jboss.util.JBossStringBuilder;
import org.w3c.dom.Element;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * A JMS server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * 
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerPeer extends ServiceMBeanSupport implements ServerPeerMBean
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerPeer.class);

   // The "subsystem" label this ServerPeer uses to register its ServerInvocationHandler with the
   // Remoting connector
   public static final String REMOTING_JMS_SUBSYSTEM = "JMS";

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private int serverPeerID = -1;
   private byte[] clientAOPStack;
   private Version version;

   private String defaultQueueJNDIContext = "";
   private String defaultTopicJNDIContext = "";

   private boolean started;

   private int objectIDSequence = 1;

   // The default maximum number of delivery attempts before sending to DLQ - can be overridden on
   // the destination
   private int defaultMaxDeliveryAttempts = 10;

   private Object failoverStatusLock;
   
   // Default is 1 minute
   private long failoverStartTimeout = 60 * 1000;
   
   // Default is 5 minutes
   private long failoverCompleteTimeout = 5 * 60 * 1000;
   
   private Map sessions;
   
   private long defaultRedeliveryDelay;
   
   private long queueStatsSamplePeriod = 10000;
   
   private int defaultMessageCounterHistoryDayLimit;
      
   // wired components

   private DestinationJNDIMapper destinationJNDIMapper;
   private SecurityMetadataStore securityStore;
   private ConnectionFactoryJNDIMapper connFactoryJNDIMapper;
   private TransactionRepository txRepository;
   private ConnectionManager connectionManager;
   private ConnectorManager connectorManager;
   private IDManager messageIDManager;
   private IDManager channelIDManager;
   private IDManager transactionIDManager;
   private MemoryManager memoryManager;  
   private MessageStore messageStore;
   private MessageCounterManager messageCounterManager;

   // plugins

   protected ObjectName persistenceManagerObjectName;
   protected PersistenceManager persistenceManager;

   protected ObjectName postOfficeObjectName;
   protected PostOffice postOffice;

   protected ObjectName jmsUserManagerObjectName;
   protected JMSUserManager jmsUserManager;
   
   protected ObjectName defaultDLQObjectName;
   protected Queue defaultDLQ;
   
   protected ObjectName defaultExpiryQueueObjectName;
   protected Queue defaultExpiryQueue;

   // Constructors ---------------------------------------------------------------------------------
   public ServerPeer() throws Exception
   {
      log.info("creating server peer");

      // Some wired components need to be started here
      securityStore = new SecurityMetadataStore();

      version = Version.instance();
      
      failoverStatusLock = new Object();
      
      sessions = new ConcurrentReaderHashMap();

      started = false;
   }

   public ServerPeer(int serverPeerID,
                     String defaultQueueJNDIContext,
                     String defaultTopicJNDIContext) throws Exception
   {
      this();

      if (serverPeerID < 0)
      {
         throw new IllegalArgumentException("ID cannot be negative");
      }
      
      setServerPeerID(serverPeerID);
      this.defaultQueueJNDIContext = defaultQueueJNDIContext;
      this.defaultTopicJNDIContext = defaultTopicJNDIContext;
   }      

   // ServiceMBeanSupport overrides ----------------------------------------------------------------

   public synchronized void startService() throws Exception
   {
      try
      {
         log.debug("starting ServerPeer");

         if (started)
         {
            return;
         }
         
         if (serverPeerID < 0)
         {
            throw new IllegalStateException(" ServerPeerID not set");
         }

         log.debug(this + " starting");

         loadClientAOPConfig();

         loadServerAOPConfig();

         MBeanServer mbeanServer = getServer();

         // Acquire references to plugins. Each plug-in will be accessed directly via a reference
         // circumventing the MBeanServer. However, they are installed as services to take advantage
         // of their automatically-creating management interface.

         persistenceManager = (PersistenceManager)mbeanServer.
            getAttribute(persistenceManagerObjectName, "Instance");

         jmsUserManager = (JMSUserManager)mbeanServer.
            getAttribute(jmsUserManagerObjectName, "Instance");

         // We get references to some plugins lazily to avoid problems with circular MBean
         // dependencies

         // Create the wired components
         messageIDManager = new IDManager("MESSAGE_ID", 4096, persistenceManager);
         channelIDManager = new IDManager("CHANNEL_ID", 10, persistenceManager);
         transactionIDManager = new IDManager("TRANSACTION_ID", 1024, persistenceManager);
         destinationJNDIMapper = new DestinationJNDIMapper(this);
         connFactoryJNDIMapper = new ConnectionFactoryJNDIMapper(this);
         connectionManager = new SimpleConnectionManager();
         connectorManager = new SimpleConnectorManager();
         memoryManager = new SimpleMemoryManager();
         messageStore = new SimpleMessageStore();
         txRepository =
            new TransactionRepository(persistenceManager, messageStore, transactionIDManager);
         messageCounterManager = new MessageCounterManager(queueStatsSamplePeriod);

         // Start the wired components

         messageIDManager.start();
         channelIDManager.start();
         transactionIDManager.start();
         destinationJNDIMapper.start();
         connFactoryJNDIMapper.start();
         connectionManager.start();
         connectorManager.start();
         memoryManager.start();
         messageStore.start();
         securityStore.start();
         txRepository.start();
         
         // Note we do not start the message counter manager by default. This must be done
         // explicitly by the user by calling enableMessageCounters(). This is because message
         // counter history takes up growing memory to store the stats and could theoretically
         // eventually cause the server to run out of RAM
         
         txRepository.loadPreparedTransactions();
         
         JMSWireFormat wf = new JMSWireFormat();         
         MarshalFactory.addMarshaller("jms", wf, wf);      
         
         //Now everything is started we can tell the invocation handler to start handling invocations
         //We do this right at the end otherwise it can start handling invocations before we are properly started
         JMSServerInvocationHandler.setClosed(false);

         started = true;

         log.info("JBoss Messaging " + getVersion().getProviderVersion() + " server [" +
            getServerPeerID()+ "] started");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " startService");
      }
   }

   public synchronized void stopService() throws Exception
   {
      try
      {
         if (!started)
         {
            return;
         }

         log.debug(this + " stopping");

         started = false;
         
         //Tell the invocation handler we are closed - this is so we don't attempt to handle
         //any invocations when we are in a partial closing down state - which can give strange
         //"object not found with id" exceptions and stuff like that
         JMSServerInvocationHandler.setClosed(true);

         // Stop the wired components

         messageIDManager.stop();
         messageIDManager = null;
         channelIDManager.stop();
         channelIDManager = null;
         transactionIDManager.stop();
         transactionIDManager = null;
         destinationJNDIMapper.stop();
         destinationJNDIMapper = null;
         connFactoryJNDIMapper.stop();
         connFactoryJNDIMapper = null;
         connectionManager.stop();
         connectionManager = null;
         connectorManager.start();
         connectorManager = null;
         memoryManager.stop();
         memoryManager = null;
         messageStore.stop();
         messageStore = null;
         securityStore.stop();
         securityStore = null;
         txRepository.stop();
         txRepository = null;
         messageCounterManager.stop();
         messageCounterManager = null;

         unloadServerAOPConfig();

         // TODO unloadClientAOPConfig();

         MessagingTimeoutFactory.instance.reset();

         log.info("JMS " + this + " stopped");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " stopService");
      }
   }

   // JMX Attributes -------------------------------------------------------------------------------

   // Plugins
   
   public synchronized ObjectName getPersistenceManager()
   {
      return persistenceManagerObjectName;
   }

   public synchronized void setPersistenceManager(ObjectName on)
   {
      if (started)
      {
         log.warn("Cannot set persistence manager on server peer when server peer is started");
         return;         
      }
      persistenceManagerObjectName = on;
   }

   public synchronized ObjectName getPostOffice()
   {
      return postOfficeObjectName;
   }

   public synchronized void setPostOffice(ObjectName on)
   {
      if (started)
      {
         log.warn("Cannot set post office on server peer when server peer is started");
         return;         
      }
      postOfficeObjectName = on;
   }

   public synchronized ObjectName getJmsUserManager()
   {
      return jmsUserManagerObjectName;
   }

   public synchronized void setJMSUserManager(ObjectName on)
   {
      if (started)
      {
         log.warn("Cannot set jms user manager on server peer when server peer is started");
         return;         
      }
      jmsUserManagerObjectName = on;
   }
   
   public synchronized ObjectName getDefaultDLQ()
   {
      return defaultDLQObjectName;
   }

   public synchronized void setDefaultDLQ(ObjectName on)
   {
      defaultDLQObjectName = on;
   }
   
   public synchronized ObjectName getDefaultExpiryQueue()
   {
      return defaultExpiryQueueObjectName;
   }

   public synchronized void setDefaultExpiryQueue(ObjectName on)
   {
      this.defaultExpiryQueueObjectName = on;
   }     
      
   // Instance access

   public Object getInstance()
   {
      return this;
   }
   
   //read only JMX attributes

   public String getJMSVersion()
   {
      return version.getJMSVersion();
   }

   public int getJMSMajorVersion()
   {
      return version.getJMSMajorVersion();
   }

   public int getJMSMinorVersion()
   {
      return version.getJMSMinorVersion();
   }

   public String getJMSProviderName()
   {
      return version.getJMSProviderName();
   }

   public String getProviderVersion()
   {
      return version.getProviderVersion();
   }

   public int getProviderMajorVersion()
   {
      return version.getProviderMajorVersion();
   }

   public int getProviderMinorVersion()
   {
      return version.getProviderMinorVersion();
   }
   
   //Read - write attributes

   public synchronized void setSecurityDomain(String securityDomain) throws Exception
   {
      try
      {
         securityStore.setSecurityDomain(securityDomain);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " setSecurityDomain");
      }
   }

   public synchronized String getSecurityDomain()
   {
      return securityStore.getSecurityDomain();
   }

   public synchronized void setDefaultSecurityConfig(Element conf) throws Exception
   {
      securityStore.setDefaultSecurityConfig(conf);
   }

   public synchronized Element getDefaultSecurityConfig()
   {
      return securityStore.getDefaultSecurityConfig();
   }
        
   public synchronized long getFailoverStartTimeout()
   {
      return this.failoverStartTimeout;
   }
   
   public synchronized void setFailoverStartTimeout(long timeout)
   {
      this.failoverStartTimeout = timeout;
   }
   
   public synchronized long getFailoverCompleteTimeout()
   {
      return this.failoverCompleteTimeout;
   }
   
   public synchronized void setFailoverCompleteTimeout(long timeout)
   {
      this.failoverCompleteTimeout = timeout;
   }
   
   public synchronized int getDefaultMaxDeliveryAttempts()
   {
      return defaultMaxDeliveryAttempts;
   }

   public synchronized void setDefaultMaxDeliveryAttempts(int attempts)
   {
      this.defaultMaxDeliveryAttempts = attempts;
   }
   
   public synchronized long getQueueStatsSamplePeriod()
   {
      return queueStatsSamplePeriod;
   }

   public synchronized void setQueueStatsSamplePeriod(long newPeriod)
   {
      if (newPeriod < 1000)
      {
         throw new IllegalArgumentException("Cannot set QueueStatsSamplePeriod < 1000 ms");
      }
      
      if (messageCounterManager != null && newPeriod != queueStatsSamplePeriod)
      {
         messageCounterManager.reschedule(newPeriod);
      }            
      
      this.queueStatsSamplePeriod = newPeriod;
   }
   
   public synchronized long getDefaultRedeliveryDelay()
   {
      return defaultRedeliveryDelay;
   }
   
   public synchronized void setDefaultRedeliveryDelay(long delay)
   {
      this.defaultRedeliveryDelay = delay;
   }
   
   public synchronized int getDefaultMessageCounterHistoryDayLimit()
   {
      return defaultMessageCounterHistoryDayLimit;
   }
   
   public void setDefaultMessageCounterHistoryDayLimit(int limit)
   {
      if (limit < -1)
      {
         limit = -1;
      }
      
      this.defaultMessageCounterHistoryDayLimit = limit;
   }
   
   public synchronized void setServerPeerID(int serverPeerID)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set ServerPeerID while the service is running");
      }
      this.serverPeerID = serverPeerID;
      log.info("ServerPeerID set to " + serverPeerID);
   }

   public int getServerPeerID()
   {
      return serverPeerID;
   }

   public String getDefaultQueueJNDIContext()
   {
      return defaultQueueJNDIContext;
   }
   
   public synchronized void setDefaultQueueJNDIContext(String defaultQueueJNDIContext)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set DefaultQueueJNDIContext while the service is running");
      }

      this.defaultQueueJNDIContext = defaultQueueJNDIContext;
   }

   public String getDefaultTopicJNDIContext()
   {
      return defaultTopicJNDIContext;
   }

   public synchronized void setDefaultTopicJNDIContext(String defaultTopicJNDIContext)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set DefaultTopicJNDIContext while the service is running");
      }

      this.defaultTopicJNDIContext = defaultTopicJNDIContext;
   }
   
   public void enableMessageCounters()
   {      
      messageCounterManager.start();
   }
   
   public void disableMessageCounters()
   {
      messageCounterManager.stop();
      
      messageCounterManager.resetAllCounters();
      
      messageCounterManager.resetAllCounterHistories();
   }
   
   // JMX Operations -------------------------------------------------------------------------------

   public String deployQueue(String name, String jndiName) throws Exception
   {
      try
      {
         return deployDestinationDefault(true, name, jndiName);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " createQueue");
      }
   }

   public String deployQueue(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      try
      {
         return deployDestination(true, name, jndiName, fullSize, pageSize, downCacheSize);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " createQueue(2)");
      }
   }

   public boolean destroyQueue(String name) throws Exception
   {
      try
      {
         return destroyDestination(true, name);        
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " destroyQueue");
      }
   }
   
   public boolean undeployQueue(String name) throws Exception
   {
      try
      {
         return undeployDestination(true, name);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " destroyQueue");
      }
   }

   public String deployTopic(String name, String jndiName) throws Exception
   {
      try
      {
         return deployDestinationDefault(false, name, jndiName);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " createTopic");
      }
   }

   public String deployTopic(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      try
      {
         return deployDestination(false, name, jndiName, fullSize, pageSize, downCacheSize);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " createTopic(2)");
      }
   }

   public boolean destroyTopic(String name) throws Exception
   {
      try
      {
         return destroyDestination(false, name);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " destroyTopic");
      }
   }
   
   public boolean undeployTopic(String name) throws Exception
   {
      try
      {
         return undeployDestination(false, name);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " destroyTopic");
      }
   }

   public Set getDestinations() throws Exception
   {
      try
      {
         return destinationJNDIMapper.getDestinations();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " getDestinations");
      }
   }
   
   public List getMessageCounters() throws Exception
   {
      Collection counters = messageCounterManager.getMessageCounters();
      
      return new ArrayList(counters);
   }

   public List getMessageStatistics() throws Exception
   {
      return MessageCounter.getMessageStatistics(getMessageCounters());
   }

   public String listMessageCountersAsHTML() throws Exception
   {
      List counters = getMessageCounters();

      String ret =
         "<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">"
            + "<tr>"
            + "<th>Type</th>"
            + "<th>Name</th>"
            + "<th>Subscription</th>"
            + "<th>Durable</th>"
            + "<th>Count</th>"
            + "<th>CountDelta</th>"
            + "<th>Depth</th>"
            + "<th>DepthDelta</th>"
            + "<th>Last Add</th>"
            + "</tr>";

      String strNameLast = null;
      String strTypeLast = null;
      String strDestLast = null;

      String destData = "";
      int destCount = 0;

      int countTotal = 0;
      int countDeltaTotal = 0;
      int depthTotal = 0;
      int depthDeltaTotal = 0;

      int i = 0; // define outside of for statement, so variable
      // still exists after for loop, because it is
      // needed during output of last module data string

      Iterator iter = counters.iterator();
      
      while (iter.hasNext())
      {
         MessageCounter counter = (MessageCounter)iter.next();
         
         // get counter data
         StringTokenizer tokens = new StringTokenizer(counter.getCounterAsString(), ",");

         String strType = tokens.nextToken();
         String strName = tokens.nextToken();
         String strSub = tokens.nextToken();
         String strDurable = tokens.nextToken();

         String strDest = strType + "-" + strName;

         String strCount = tokens.nextToken();
         String strCountDelta = tokens.nextToken();
         String strDepth = tokens.nextToken();
         String strDepthDelta = tokens.nextToken();
         String strDate = tokens.nextToken();

         // update total count / depth values
         countTotal += Integer.parseInt(strCount);
         depthTotal += Integer.parseInt(strDepth);

         countDeltaTotal += Integer.parseInt(strCountDelta);
         depthDeltaTotal += Integer.parseInt(strDepthDelta);

         if (strCountDelta.equalsIgnoreCase("0"))
            strCountDelta = "-"; // looks better

         if (strDepthDelta.equalsIgnoreCase("0"))
            strDepthDelta = "-"; // looks better

         // output destination counter data as HTML table row
         // ( for topics with multiple subscriptions output
         //   type + name field as rowspans, looks better )
         if (strDestLast != null && strDestLast.equals(strDest))
         {
            // still same destination -> append destination subscription data
            destData += "<tr bgcolor=\"#" + ((i % 2) == 0 ? "FFFFFF" : "F0F0F0") + "\">";
            destCount += 1;
         }
         else
         {
            // start new destination data
            if (strDestLast != null)
            {
               // store last destination data string
               ret += "<tr bgcolor=\"#"
                  + ((i % 2) == 0 ? "FFFFFF" : "F0F0F0")
                  + "\"><td rowspan=\""
                  + destCount
                  + "\">"
                  + strTypeLast
                  + "</td><td rowspan=\""
                  + destCount
                  + "\">"
                  + strNameLast
                  + "</td>"
                  + destData;

               destData = "";
            }

            destCount = 1;
         }

         // counter data row
         destData += "<td>"
            + strSub
            + "</td>"
            + "<td>"
            + strDurable
            + "</td>"
            + "<td>"
            + strCount
            + "</td>"
            + "<td>"
            + strCountDelta
            + "</td>"
            + "<td>"
            + strDepth
            + "</td>"
            + "<td>"
            + strDepthDelta
            + "</td>"
            + "<td>"
            + strDate
            + "</td>";

         // store current destination data for change detection
         strTypeLast = strType;
         strNameLast = strName;
         strDestLast = strDest;
      }

      if (strDestLast != null)
      {
         // store last module data string
         ret += "<tr bgcolor=\"#"
            + ((i % 2) == 0 ? "FFFFFF" : "F0F0F0")
            + "\"><td rowspan=\""
            + destCount
            + "\">"
            + strTypeLast
            + "</td><td rowspan=\""
            + destCount
            + "\">"
            + strNameLast
            + "</td>"
            + destData;
      }

      // append summation info
      ret += "<tr>"
         + "<td><![CDATA[ ]]></td><td><![CDATA[ ]]></td>"
         + "<td><![CDATA[ ]]></td><td><![CDATA[ ]]></td><td>"
         + countTotal
         + "</td><td>"
         + (countDeltaTotal == 0 ? "-" : Integer.toString(countDeltaTotal))
         + "</td><td>"
         + depthTotal
         + "</td><td>"
         + (depthDeltaTotal == 0 ? "-" : Integer.toString(depthDeltaTotal))
         + "</td><td>Total</td></tr></table>";

      return ret;
   }

   public void resetAllMessageCounters()
   {
      messageCounterManager.resetAllCounters();
   }
   
   public void resetAllMessageCounterHistories()
   {
      messageCounterManager.resetAllCounterHistories();
   }
   
   public List retrievePreparedTransactions()
   {
      return txRepository.getPreparedTransactions();
   }

   public String showPreparedTransactionsAsHTML()
   {
      List txs = txRepository.getPreparedTransactions();
      JBossStringBuilder buffer = new JBossStringBuilder();
      buffer.append("<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">");
      buffer.append("<tr><th>Xid</th></tr>");
      for (Iterator i = txs.iterator(); i.hasNext();)
      {
         Xid xid = (Xid)i.next();
         if (xid != null)
         {
            buffer.append("<tr><td>");
            buffer.append(xid);
            buffer.append("</td></tr>");
         }
      }
      buffer.append("</table>");
      return buffer.toString();
   }
   
   // Public ---------------------------------------------------------------------------------------
   
   public byte[] getClientAOPStack()
   {
      return clientAOPStack;
   }
   
   public MessageCounterManager getMessageCounterManager()
   {
      return messageCounterManager;
   }
   
   public IDManager getMessageIDManager()
   {
      return messageIDManager;
   }

   public IDManager getChannelIDManager()
   {
      return channelIDManager;
   }
   
   public ServerSessionEndpoint getSession(Integer sessionID)
   {
      return (ServerSessionEndpoint)sessions.get(sessionID);
   }
   
   public void addSession(Integer id, ServerSessionEndpoint session)
   {
      sessions.put(id, session);      
   }
   
   public void removeSession(Integer id)
   {
      if (sessions.remove(id) == null)
      {
         throw new IllegalStateException("Cannot find session with id " + id + " to remove");
      }
   }

   public synchronized Queue getDefaultDLQInstance() throws Exception
   {
      Queue dlq = null;
      
      if (defaultDLQObjectName != null)
      { 
         ManagedQueue dest = null;
         
         try
         {         
            dest = (ManagedQueue)getServer().
               getAttribute(defaultDLQObjectName, "Instance");
         }
         catch (InstanceNotFoundException e)
         {
            //Ok
         }
         
         if (dest != null)
         {            
            PostOffice po = getPostOfficeInstance();
            
            Binding binding = po.getBindingForQueueName(dest.getName());
            
            if (binding != null && binding.getQueue().isActive())
            {
               dlq =  binding.getQueue();
            }
         }
      }
      
      return dlq;
   }
   
   public synchronized Queue getDefaultExpiryQueueInstance() throws Exception
   {
      Queue expiryQueue = null;
      
      if (defaultExpiryQueueObjectName != null)
      {
         ManagedQueue dest = null;
         
         try
         {         
            dest = (ManagedQueue)getServer().
               getAttribute(defaultExpiryQueueObjectName, "Instance");
         }
         catch (InstanceNotFoundException e)
         {
            //Ok
         }

         if (dest != null)
         {            
            PostOffice po = getPostOfficeInstance();
            
            Binding binding = po.getBindingForQueueName(dest.getName());
            
            if (binding != null && binding.getQueue().isActive())
            {
               expiryQueue =  binding.getQueue();
            }
         }
      }
      
      return expiryQueue;
   }

   public TransactionRepository getTxRepository()
   {
      return txRepository;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   public Version getVersion()
   {
      return version;
   }

   // access to hard-wired server extensions

   public SecurityManager getSecurityManager()
   {
      return securityStore;
   }

   public DestinationManager getDestinationManager()
   {
      return destinationJNDIMapper;
   }

   public ConnectionFactoryManager getConnectionFactoryManager()
   {
      return connFactoryJNDIMapper;
   }

   public ConnectionManager getConnectionManager()
   {
      return connectionManager;
   }

   public ConnectorManager getConnectorManager()
   {
      return connectorManager;
   }

   public MessageStore getMessageStore()
   {
      return messageStore;
   }

   public MemoryManager getMemoryManager()
   {
      return memoryManager;
   }

   // access to plugin references

   public PersistenceManager getPersistenceManagerInstance()
   {
      return persistenceManager;
   }

   public JMSUserManager getJmsUserManagerInstance()
   {
      return jmsUserManager;
   }

   public PostOffice getPostOfficeInstance() throws Exception
   {
      // We get the reference lazily to avoid problems with MBean circular dependencies
      if (postOffice == null)
      {
         postOffice = (PostOffice)getServer().getAttribute(postOfficeObjectName, "Instance");

         // We also inject the replicator dependency into the ConnectionFactoryJNDIMapper. This is
         // a bit messy but we have a circular dependency POJOContainer should be able to help us
         // here. Yes, this is nasty.

         if (!postOffice.isLocal())
         {
            Replicator rep = (Replicator)postOffice;
            connFactoryJNDIMapper.injectReplicator(rep);            
            rep.registerListener(new FailoverListener());
         }
         
         // Also need to inject into txRepository
         txRepository.injectPostOffice(postOffice);
      }
      return postOffice;
   }

   public Replicator getReplicator() throws Exception
   {
      PostOffice postOffice = getPostOfficeInstance();
      if (!(postOffice instanceof Replicator))
      {
         throw new  IllegalAccessException("This operations is only legal on clustering configurations");
      }
      return (Replicator)postOffice;
   }

   public synchronized int getNextObjectID()
   {
      return objectIDSequence++;
   }

   /*
    * Wait for failover from the specified node to complete.
    */
   public int waitForFailover(int failedNodeID) throws Exception
   {
      // This node may be failing over for another node - in which case we must wait for that to be
      // complete.
      
      log.debug(this + " waiting for server-side failover for failed node " + failedNodeID + " to complete");
      
      Replicator replicator = getReplicator();

      long startToWait = getFailoverStartTimeout();
      long completeToWait = getFailoverCompleteTimeout();
                     
      // Must lock here
      synchronized (failoverStatusLock)
      {         
         while (true)
         {         
            //TODO we shouldn't have a dependency on DefaultClusteredPostOffice - where should we put the constants?

            Map replicants = replicator.get(DefaultClusteredPostOffice.FAILED_OVER_FOR_KEY);
            
            boolean foundEntry = false;
                        
            if (replicants != null)
            {
               for(Iterator i = replicants.entrySet().iterator(); i.hasNext(); )
               {
                  Map.Entry entry = (Map.Entry)i.next();
                  Integer nid = (Integer)entry.getKey();
                  FailoverStatus status = (FailoverStatus)entry.getValue();
                  
                  if (status.isFailedOverForNode(failedNodeID))
                  {
                     log.debug(this + ": failover is complete on node " + nid);
                     return nid.intValue();
                  }
                  else if (status.isFailingOverForNode(failedNodeID))
                  {
                     log.debug(this + ": fail over is in progress on node " + nid);
                     
                     // A server has started failing over for the failed node, but not completed.
                     // If it's not this node then we immediately return so the connection can be
                     // redirected to another node.
                     if (nid.intValue() != this.getServerPeerID())
                     {
                        return nid.intValue();
                     }
                     
                     // Otherwise we wait for failover to complete
                     
                     if (completeToWait <= 0)
                     {
                        // Give up now
                        log.debug(this + " already waited long enough for failover to complete, giving up");
                        return -1;
                     }
                     
                     // Note - we have to count the time since other unrelated nodes may fail and
                     // wake up the lock - in this case we don't want to give up too early.
                     long start = System.currentTimeMillis();

                     try
                     {
                        log.debug(this + " blocking on the failover lock, waiting for failover to complete");
                        failoverStatusLock.wait(completeToWait);
                        log.debug(this + " releasing the failover lock, checking again whether failover completed ...");
                     }
                     catch (InterruptedException ignore)
                     {                  
                     }
                     completeToWait -= System.currentTimeMillis() - start;
                     foundEntry = true;
                  }
               }        
            }
            
            if (!foundEntry)
            {              
               // No trace of failover happening so we wait a maximum of FAILOVER_START_TIMEOUT for
               // some replicated data to arrive. This should arrive fairly quickly since this is
               // added at the beginning of the failover process. If it doesn't arrive it would
               // imply that no failover has actually happened on the server or the timeout is too
               // short. It is possible that no failover has actually happened on the server, if for
               // example there is a problem with the client side network but the server side
               // network is ok.
   
               if (startToWait <= 0)
               {
                  // Don't want to wait again
                  log.debug(this + " already waited long enough for failover to start, giving up");
                  return -1;
               }
               
               // Note - we have to count the time since other unrelated nodes may fail and wake
               // up the lock - in this case we don't want to give up too early.
               long start = System.currentTimeMillis(); 
               try
               {
                  log.debug(this + " blocking on the failover lock, waiting for failover to start");
                  failoverStatusLock.wait(startToWait);
                  log.debug(this + " releasing the failover lock, checking again whether failover started ...");
               }
               catch (InterruptedException ignore)
               {                  
               }
               startToWait -= System.currentTimeMillis() - start;              
            }
         }        
      }
   }
   
   public String toString()
   {
      return "ServerPeer[" + getServerPeerID() + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
  
   private void loadServerAOPConfig() throws Exception
   {
      URL url = this.getClass().getClassLoader().getResource("aop-messaging-server.xml");
      AspectXmlLoader.deployXML(url, this.getClass().getClassLoader());
   }

   private void unloadServerAOPConfig() throws Exception
   {
      URL url = this.getClass().getClassLoader().getResource("aop-messaging-server.xml");
      AspectXmlLoader.undeployXML(url);
   }

   private void loadClientAOPConfig() throws Exception
   {
      // Note the file is called aop-messaging-client.xml NOT messaging-client-aop.xml. This is
      // because the JBoss will automatically deploy any files ending with aop.xml; we do not want
      // this to happen for the client config

      URL url = this.getClass().getClassLoader().getResource("aop-messaging-client.xml");
      InputStream is = null;
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      try
      {
         is = url.openStream();
         int b;
         while ((b = is.read()) != -1)
         {
            os.write(b);
         }
         os.flush();
         clientAOPStack = os.toByteArray();
      }
      finally
      {
         if (is != null)
         {
            is.close();
         }
         if (os != null)
         {
            os.close();
         }
      }
   }

   private String deployDestinationDefault(boolean isQueue, String name, String jndiName)
      throws Exception
   {
      //
      // TODO - THIS IS A TEMPORARY IMPLEMENTATION; WILL BE REPLACED WITH INTEGRATION-CONSISTENT ONE
      // TODO - if I find a way not using UnifiedClassLoader3 directly, then get rid of
      //        <path refid="jboss.jmx.classpath"/> from jms/build.xml dependentmodule.classpath
      //

      //TODO - Yes this is super-ugly - there must be an easier way of doing it
      //also in LocalTestServer is doing the same thing in a slightly different way
      //this should be combined

      String destType = isQueue ? "Queue" : "Topic";
      String className = "org.jboss.jms.server.destination." + destType + "Service";
      String ons ="jboss.messaging.destination:service="+ destType + ",name=" + name;
      ObjectName on = new ObjectName(ons);

      String destinationMBeanConfig =
         "<mbean code=\"" + className + "\" " +
         "       name=\"" + ons + "\" " +
         "       xmbean-dd=\"xmdesc/" + destType + "-xmbean.xml\">\n" +
         "    <constructor>" +
         "        <arg type=\"boolean\" value=\"true\"/>" +
         "    </constructor>" +
         "</mbean>";

      return deployDestinationInternal(destinationMBeanConfig, on, jndiName, false, -1, -1, -1);
   }
   
   private String deployDestination(boolean isQueue, String name, String jndiName, int fullSize,
            int pageSize, int downCacheSize) throws Exception
   {
      //    
      //    TODO - THIS IS A TEMPORARY IMPLEMENTATION; WILL BE REPLACED WITH INTEGRATION-CONSISTENT ONE
      //    TODO - if I find a way not using UnifiedClassLoader3 directly, then get rid of
      //    <path refid="jboss.jmx.classpath"/> from jms/build.xml dependentmodule.classpath
      //    
      
      String destType = isQueue ? "Queue" : "Topic";
      String className = "org.jboss.jms.server.destination." + destType + "Service";
      
      String ons ="jboss.messaging.destination:service="+ destType + ",name=" + name;
      ObjectName on = new ObjectName(ons);
      
      String destinationMBeanConfig =
         "<mbean code=\"" + className + "\" " +
         "       name=\"" + ons + "\" " +
         "       xmbean-dd=\"xmdesc/" + destType + "-xmbean.xml\">\n" +
         "    <constructor>" +
         "        <arg type=\"boolean\" value=\"true\"/>" +
         "    </constructor>" +
         "    <attribute name=\"FullSize\">" + fullSize + "</attribute>" +
         "    <attribute name=\"PageSize\">" + pageSize + "</attribute>" +
         "    <attribute name=\"DownCacheSize\">" + downCacheSize + "</attribute>" +
         "</mbean>";
      
      return deployDestinationInternal(destinationMBeanConfig, on, jndiName, true, fullSize,
               pageSize, downCacheSize);
   }

   private String deployDestinationInternal(String destinationMBeanConfig, ObjectName on,
                                            String jndiName, boolean params, int fullSize,
                                            int pageSize, int downCacheSize) throws Exception
   {
      MBeanServer mbeanServer = getServer();

      Element element = Util.stringToElement(destinationMBeanConfig);

      ServiceCreator sc = new ServiceCreator(mbeanServer);

      ClassLoader cl = this.getClass().getClassLoader();
      ObjectName loaderObjectName = null;
      if (cl instanceof UnifiedClassLoader3)
      {
         loaderObjectName = ((UnifiedClassLoader3)cl).getObjectName();
      }

      sc.install(on, loaderObjectName, element);

      // inject dependencies
      mbeanServer.setAttribute(on, new Attribute("ServerPeer", getServiceName()));
      mbeanServer.setAttribute(on, new Attribute("JNDIName", jndiName));
      if (params)
      {
         mbeanServer.setAttribute(on, new Attribute("FullSize", new Integer(fullSize)));
         mbeanServer.setAttribute(on, new Attribute("PageSize", new Integer(pageSize)));
         mbeanServer.setAttribute(on, new Attribute("DownCacheSize", new Integer(downCacheSize)));
      }
      mbeanServer.invoke(on, "create", new Object[0], new String[0]);
      mbeanServer.invoke(on, "start", new Object[0], new String[0]);

      return (String)mbeanServer.getAttribute(on, "JNDIName");
   }

   

   /*
    * Undeploy the MBean but don't delete the underlying data
    */
   private boolean undeployDestination(boolean isQueue, String name) throws Exception
   {
      String destType = isQueue ? "Queue" : "Topic";
      String ons ="jboss.messaging.destination:service=" + destType + ",name=" + name;
      ObjectName on = new ObjectName(ons);

      MBeanServer mbeanServer = getServer();

      // we can only undeploy destinations that exist AND that have been created programatically
      if (!mbeanServer.isRegistered(on))
      {
         return false;
      }
      Boolean b = (Boolean)mbeanServer.getAttribute(on, "CreatedProgrammatically");
      if (!b.booleanValue())
      {
         log.warn("Cannot undeploy a destination that has not been created programatically");
         return false;
      }
      mbeanServer.invoke(on, "stop", new Object[0], new String[0]);
      mbeanServer.invoke(on, "destroy", new Object[0], new String[0]);
      mbeanServer.unregisterMBean(on);
      return true;
   }
   
   /*
    * Undeploy the MBean and delete the underlying data
    */
   private boolean destroyDestination(boolean isQueue, String name) throws Exception
   {
      String destType = isQueue ? "Queue" : "Topic";
      String ons ="jboss.messaging.destination:service=" + destType + ",name=" + name;
      ObjectName on = new ObjectName(ons);

      MBeanServer mbeanServer = getServer();

      // we can only destroy destinations that exist AND that have been created programatically
      if (!mbeanServer.isRegistered(on))
      {
         return false;
      }
                  
      //First deactivate
      
      if (isQueue)
      {
         Binding binding = postOffice.getBindingForQueueName(name);
         
         if (binding != null)
         {
            binding.getQueue().deactivate();
         }
      }
      else
      {
         JMSCondition topicCond = new JMSCondition(false, name);    
         
         Collection bindings = postOffice.getBindingsForCondition(topicCond);
         
         Iterator iter = bindings.iterator();
         while (iter.hasNext())            
         {
            Binding binding = (Binding)iter.next();
            
            binding.getQueue().deactivate();
         }
      }
            
      //Delete any message data
      
      mbeanServer.invoke(on, "removeAllMessages", null, null);
      
      //undeploy the mbean
      if (!undeployDestination(isQueue, name))
      {
         return false;
      }
            
      //Unbind from the post office
      
      if (isQueue)
      {
         Binding binding = postOffice.getBindingForQueueName(name);
         
         if (binding != null)
         {
            try
            {
               Queue queue = binding.getQueue();
               if (!queue.isClustered())
               {
                  postOffice.unbindQueue(queue.getName());
               }
               else
               {
                  ((ClusteredPostOffice)postOffice).unbindClusteredQueue(queue.getName());
               }
            }
            catch (Throwable t)
            {
               throw new Exception("Failed to unbind queue", t);
            }
         }
      }
      else
      {
         JMSCondition topicCond = new JMSCondition(false, name);    
         
         Collection bindings = postOffice.getBindingsForCondition(topicCond);
         
         Iterator iter = bindings.iterator();
         while (iter.hasNext())            
         {
            Binding binding = (Binding)iter.next();
            
            try
            {
               postOffice.unbindQueue(binding.getQueue().getName());
            }
            catch (Throwable t)
            {
               throw new Exception("Failed to unbind queue", t);
            }
         }
      }
      return true;
   }
   

   // Inner classes --------------------------------------------------------------------------------
   
   private class FailoverListener implements ReplicationListener
   {
      public void onReplicationChange(Serializable key, Map updatedReplicantMap,
                                      boolean added, int originatingNodeId)
      {
         if (key.equals(DefaultClusteredPostOffice.FAILED_OVER_FOR_KEY))
         {
            if (updatedReplicantMap != null && originatingNodeId == serverPeerID)
            {
               FailoverStatus status =
                  (FailoverStatus)updatedReplicantMap.get(new Integer(serverPeerID));
               
               if (status != null && status.isFailedOver())
               {                     
                  // We prompt txRepository to load any prepared txs - so we can take over
                  // responsibility for in doubt transactions from other nodes
                  try
                  {
                     txRepository.loadPreparedTransactions();
                  }
                  catch (Exception e)
                  {
                     log.error("Failed to load prepared transactions", e);
                  }
               }
            }
            
            synchronized (failoverStatusLock)
            {
               log.debug(ServerPeer.this +
                  ".FailoverListener got failover event, notifying those waiting on lock");
               
               failoverStatusLock.notifyAll();
            }
         }         
      }      
   }
}
