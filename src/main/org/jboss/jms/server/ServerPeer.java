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
import java.io.CharArrayWriter;
import java.io.InputStream;
import java.io.PrintWriter;
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
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.messagecounter.MessageCounterManager;
import org.jboss.jms.server.plugin.contract.JMSUserManager;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.jms.server.security.SecurityMetadataStore;
import org.jboss.jms.wireformat.JMSWireFormat;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.ClusterNotifier;
import org.jboss.messaging.core.contract.MemoryManager;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.contract.Replicator;
import org.jboss.messaging.core.impl.DefaultClusterNotifier;
import org.jboss.messaging.core.impl.FailoverWaiter;
import org.jboss.messaging.core.impl.IDManager;
import org.jboss.messaging.core.impl.JDBCPersistenceManager;
import org.jboss.messaging.core.impl.clusterconnection.ClusterConnectionManager;
import org.jboss.messaging.core.impl.memory.SimpleMemoryManager;
import org.jboss.messaging.core.impl.message.SimpleMessageStore;
import org.jboss.messaging.core.impl.postoffice.MessagingPostOffice;
import org.jboss.messaging.core.impl.tx.TransactionRepository;
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
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * 
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerPeer extends ServiceMBeanSupport
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

   private boolean supportsFailover = true;

   // The default maximum number of delivery attempts before sending to DLQ - can be overridden on
   // the destination
   private int defaultMaxDeliveryAttempts = 10;

   // Default is 1 minute
   private long failoverStartTimeout = 60 * 1000;
   
   // Default is 5 minutes
   private long failoverCompleteTimeout = 5 * 60 * 1000;
   
   private Map sessions;
   
   private long defaultRedeliveryDelay;
   
   private long messageCounterSamplePeriod = 10000;
   
   private int defaultMessageCounterHistoryDayLimit;
   
   private String clusterPullConnectionFactoryName;
   
   private boolean useXAForMessagePull;
   
   private boolean defaultPreserveOrdering;
   
   private long recoverDeliveriesTimeout = 5 * 60 * 1000;
      
   // wired components

   private DestinationJNDIMapper destinationJNDIMapper;
   private SecurityMetadataStore securityStore;
   private ConnectionFactoryJNDIMapper connFactoryJNDIMapper;
   private TransactionRepository txRepository;
   private SimpleConnectionManager connectionManager;
   private ConnectorManager connectorManager;
   private IDManager messageIDManager;
   private IDManager channelIDManager;
   private IDManager transactionIDManager;
   private MemoryManager memoryManager;  
   private MessageStore messageStore;
   private MessageCounterManager messageCounterManager;
   private ClusterConnectionManager clusterConnectionManager;
   private ClusterNotifier clusterNotifier;
   private FailoverWaiter failoverWaiter;   

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
      // Some wired components need to be started here
      securityStore = new SecurityMetadataStore();

      version = Version.instance();
      
      sessions = new ConcurrentReaderHashMap();

      started = false;
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
            throw new IllegalStateException("ServerPeerID not set");
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
         ((JDBCPersistenceManager)persistenceManager).injectNodeID(serverPeerID);

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
         messageCounterManager = new MessageCounterManager(messageCounterSamplePeriod);
                
         clusterNotifier = new DefaultClusterNotifier();      
         clusterNotifier.registerListener(connectionManager);
         clusterNotifier.registerListener(connFactoryJNDIMapper);
         failoverWaiter = new FailoverWaiter(serverPeerID, failoverStartTimeout, failoverCompleteTimeout, txRepository);
         clusterNotifier.registerListener(failoverWaiter);         
         
         if (clusterPullConnectionFactoryName != null)
         {         
	         clusterConnectionManager = new ClusterConnectionManager(useXAForMessagePull, serverPeerID, clusterPullConnectionFactoryName, defaultPreserveOrdering);
	         clusterNotifier.registerListener(clusterConnectionManager);
         }
         
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
         clusterConnectionManager.start();
         
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

         log.info(this + " is Stopping. NOTE! Stopping the server peer cleanly will NOT cause failover to occur");
         
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
         clusterConnectionManager.stop();
         clusterConnectionManager = null;
         
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
   
   public synchronized long getMessageCounterSamplePeriod()
   {
      return messageCounterSamplePeriod;
   }

   public synchronized void setMessageCounterSamplePeriod(long newPeriod)
   {
      if (newPeriod < 1000)
      {
         throw new IllegalArgumentException("Cannot set MessageCounterSamplePeriod < 1000 ms");
      }
      
      if (messageCounterManager != null && newPeriod != messageCounterSamplePeriod)
      {
         messageCounterManager.reschedule(newPeriod);
      }            
      
      this.messageCounterSamplePeriod = newPeriod;
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
   
   public String getClusterPullConnectionFactoryName()
   {
   	return clusterPullConnectionFactoryName;
   }
   
   public void setClusterPullConnectionFactoryName(String name)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set ClusterPullConnectionFactoryName while the service is running");
      }
   	this.clusterPullConnectionFactoryName = name;
   }
   
   public boolean isUseXAForMessagePull()
   {
   	return useXAForMessagePull;
   }
   
   public void setUseXAForMessagePull(boolean useXA) throws Exception
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set UseXAForMessagePull while the service is running");
      }
      
   	this.useXAForMessagePull = useXA;   	
   }
   
   public boolean isDefaultPreserveOrdering()
   {
   	return defaultPreserveOrdering;
   }
   
   public void setDefaultPreserveOrdering(boolean preserve) throws Exception
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set DefaultPreserveOrdering while the service is running");
      }
      
   	this.defaultPreserveOrdering = preserve;
   }
   
   public long getRecoverDeliveriesTimeout()
   {
   	return this.recoverDeliveriesTimeout;
   }
   
   public void setRecoverDeliveriesTimeout(long timeout)
   {
   	this.recoverDeliveriesTimeout = timeout;
   }
   
   public synchronized void setServerPeerID(int serverPeerID)
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set ServerPeerID while the service is running");
      }
      if (serverPeerID < 0)
      {
         throw new IllegalArgumentException("Attempt to set negative ServerPeerID: " + serverPeerID);
      }
      this.serverPeerID = serverPeerID;
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

   public String showActiveClientsAsHTML() throws Exception
   {
      CharArrayWriter charArray = new CharArrayWriter();
      PrintWriter out = new PrintWriter(charArray);

      List endpoints = connectionManager.getActiveConnections();

      out.println("<table><tr><td>ID</td><td>Host</td><td>User</td><td>#Sessions</td></tr>");
      for (Iterator iter = endpoints.iterator(); iter.hasNext();)
      {
         ServerConnectionEndpoint endpoint = (ServerConnectionEndpoint) iter.next();

         out.println("<tr>");
         out.println("<td>" + endpoint.toString() + "</td>");
         out.println("<td>" + endpoint.getCallbackHandler().getCallbackClient().getInvoker().getLocator().getHost() + "</td>");
         out.println("<td>" + endpoint.getUsername() + "</td>");
         out.println("<td>" + endpoint.getSessions().size() + "</td>");
         out.println("</tr>");
      }

      out.println("</table>");


      return charArray.toString();
   }

   // Public ---------------------------------------------------------------------------------------
   
   public void resetAllSuckers()
   {
   	clusterConnectionManager.resetAllSuckers();
   }
     
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
   
   public ServerSessionEndpoint getSession(String sessionID)
   {
      return (ServerSessionEndpoint)sessions.get(sessionID);
   }
   
   public Collection getSessions()
   {
   	return sessions.values();
   }
   
   public void addSession(String id, ServerSessionEndpoint session)
   {
      sessions.put(id, session);      
   }
   
   public void removeSession(String id)
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
            dest = (ManagedQueue)getServer().getAttribute(defaultDLQObjectName, "Instance");
         }
         catch (InstanceNotFoundException e)
         {
            //Ok
         }
         
         if (dest != null && dest.getName() != null)
         {            
            Binding binding = postOffice.getBindingForQueueName(dest.getName());
            
            if (binding == null)
            {
            	throw new IllegalStateException("Cannot find binding for queue " + dest.getName());
            }
            
            Queue queue = binding.queue;
            
            if (queue.isActive())
            {
            	dlq = queue;
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
            dest = (ManagedQueue)getServer().getAttribute(defaultExpiryQueueObjectName, "Instance");
         }
         catch (InstanceNotFoundException e)
         {
            //Ok
         }

         if (dest != null && dest.getName() != null)
         {            
         	Binding binding = postOffice.getBindingForQueueName(dest.getName());
            
            if (binding == null)
            {
            	throw new IllegalStateException("Cannot find binding for queue " + dest.getName());
            }
            
            Queue queue = binding.queue;            
         	
            if (queue.isActive())
            {
            	expiryQueue = queue;
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

         if (postOffice.isClustered())
         {
            Replicator rep = (Replicator)postOffice;
            
            connFactoryJNDIMapper.injectReplicator(rep);          
            
            // Also inject into the cluster connection manager
            
            this.clusterConnectionManager.injectPostOffice(postOffice);
            
            this.clusterConnectionManager.injectReplicator((Replicator)postOffice);
            
            this.connectionManager.injectReplicator((Replicator)postOffice);
            
            ((MessagingPostOffice)postOffice).injectServerPeer(this);
         }
         
         // Also need to inject into txRepository
         txRepository.injectPostOffice(postOffice);                          
      }
      return postOffice;
   }

   public ClusterNotifier getClusterNotifier()
   {
   	return clusterNotifier;
   }
   
   public FailoverWaiter getFailoverWaiter()
   {
   	return failoverWaiter;
   }

   public boolean isSupportsFailover()
   {
      return supportsFailover;
   }

   public void setSupportsFailover(boolean supportsFailover) throws Exception
   {
      if (started)
      {
         throw new IllegalAccessException("supportsFailover can only be changed when " +
                                          "connection factory is stopped");
      }
      this.supportsFailover = supportsFailover;
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
   private boolean destroyDestination(boolean isQueue, String name) throws Throwable
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
      
      JMSCondition condition = new JMSCondition(isQueue, name);  
      
      Collection queues = postOffice.getQueuesForCondition(condition, true);
      
      Iterator iter = queues.iterator();
      
      while (iter.hasNext())            
      {
         Queue queue = (Queue)iter.next();
         
         queue.removeAllReferences();
      }
                       
      //undeploy the mbean
      if (!undeployDestination(isQueue, name))
      {
         return false;
      }
            
      //Unbind the destination's queues
      
      log.info("Destroying destination " + name);
      
      log.info("Got queues " + queues.size());

      while (iter.hasNext())            
      {
         Queue queue = (Queue)iter.next();
         
         log.info("Queue is " + queue);
         
         queue.removeAllReferences();
         
         //Durable subs need to be removed on all nodes
         boolean all = !isQueue && queue.isRecoverable();
         
         postOffice.removeBinding(queue.getName(), all);
      }
      
      return true;
   }
   

   // Inner classes --------------------------------------------------------------------------------
   
  
}
