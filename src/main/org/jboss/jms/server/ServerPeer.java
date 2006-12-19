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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.jboss.aop.AspectXmlLoader;
import org.jboss.jms.server.connectionfactory.ConnectionFactoryJNDIMapper;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.jms.server.connectormanager.SimpleConnectorManager;
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.jms.server.plugin.contract.JMSUserManager;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.server.security.SecurityMetadataStore;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.memory.SimpleMemoryManager;
import org.jboss.messaging.core.plugin.IDManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.contract.ReplicationListener;
import org.jboss.messaging.core.plugin.contract.Replicator;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.FailoverStatus;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.util.Util;
import org.jboss.mx.loading.UnifiedClassLoader3;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.marshal.MarshalFactory;
import org.jboss.system.ServiceCreator;
import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * A JMS server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerPeer extends ServiceMBeanSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerPeer.class);

   public static final String RECOVERABLE_CTX_NAME = "jms-recoverables";

   // The "subsystem" label this ServerPeer uses to register its ServerInvocationHandler with the
   // Remoting connector
   public static final String REMOTING_JMS_SUBSYSTEM = "JMS";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private int serverPeerID;
   private byte[] clientAOPConfig;
   private Version version;

   private String defaultQueueJNDIContext;
   private String defaultTopicJNDIContext;

   private int queuedExecutorPoolSize = 200;

   private boolean started;

   private int objectIDSequence = 1;

   private int maxDeliveryAttempts = 10;

   private String dlqName;

   private Object failoverStatusLock;
   
   private long failoverStartTimeout = 3000;
   
   private long failoverCompleteTimeout = 12000;
   
   private Map sessions;
      
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
   private QueuedExecutorPool queuedExecutorPool;
   private MessageStore messageStore;

   // plugins

   protected ObjectName persistenceManagerObjectName;
   protected PersistenceManager persistenceManager;

   protected ObjectName postOfficeObjectName;
   protected PostOffice postOffice;

   protected ObjectName jmsUserManagerObjectName;
   protected JMSUserManager jmsUserManager;

   //Other stuff

   private JMSServerInvocationHandler handler;

   // Constructors --------------------------------------------------

   public ServerPeer(int serverPeerID,
                     String defaultQueueJNDIContext,
                     String defaultTopicJNDIContext) throws Exception
   {
      this.serverPeerID = serverPeerID;
      this.defaultQueueJNDIContext = defaultQueueJNDIContext;
      this.defaultTopicJNDIContext = defaultTopicJNDIContext;

      // Some wired components need to be started here
      securityStore = new SecurityMetadataStore();

      version = Version.instance();
      
      failoverStatusLock = new Object();
      
      sessions = new ConcurrentReaderHashMap();

      started = false;
   }      

   // ServiceMBeanSupport overrides ---------------------------------

   public synchronized void startService() throws Exception
   {
      try
      {
         log.debug("starting ServerPeer");

         if (started)
         {
            return;
         }

         log.debug(this + " starting");

         if (queuedExecutorPoolSize < 1)
         {
            throw new IllegalArgumentException("queuedExecutorPoolSize must be > 0");
         }
         queuedExecutorPool = new QueuedExecutorPool(queuedExecutorPoolSize);

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

         //We get references to some plugins lazily to avoid problems with circular
         //MBean dependencies

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
         txRepository = new TransactionRepository(persistenceManager, transactionIDManager);

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

         initializeRemoting(mbeanServer);

         //createRecoverable();

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

         //removeRecoverable();

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

         unloadServerAOPConfig();

         // TODO unloadClientAOPConfig();

         queuedExecutorPool.shutdown();

         log.info("JMS " + this + " stopped");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " stopService");
      }
   }

   // JMX Attributes ------------------------------------------------

   public String getDLQName()
   {
      return dlqName;
   }

   public void setDLQName(String dlqName)
   {
      this.dlqName = dlqName;
   }

   public int getMaxDeliveryAttempts()
   {
      return maxDeliveryAttempts;
   }

   public void setMaxDeliveryAttempts(int attempts)
   {
      this.maxDeliveryAttempts = attempts;
   }

   public ObjectName getPersistenceManager()
   {
      return persistenceManagerObjectName;
   }

   public void setPersistenceManager(ObjectName on)
   {
      persistenceManagerObjectName = on;
   }

   public ObjectName getPostOffice()
   {
      return postOfficeObjectName;
   }

   public void setPostOffice(ObjectName on)
   {
      postOfficeObjectName = on;
   }

   public ObjectName getJmsUserManager()
   {
      return jmsUserManagerObjectName;
   }

   public void setJMSUserManager(ObjectName on)
   {
      jmsUserManagerObjectName = on;
   }

   public Object getInstance()
   {
      return this;
   }

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

   public int getServerPeerID()
   {
      return serverPeerID;
   }

   public String getDefaultQueueJNDIContext()
   {
      return defaultQueueJNDIContext;
   }

   public String getDefaultTopicJNDIContext()
   {
      return defaultTopicJNDIContext;
   }

   public void setSecurityDomain(String securityDomain) throws Exception
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

   public String getSecurityDomain()
   {
      return securityStore.getSecurityDomain();
   }

   public void setDefaultSecurityConfig(Element conf) throws Exception
   {
      securityStore.setDefaultSecurityConfig(conf);
   }

   public Element getDefaultSecurityConfig()
   {
      return securityStore.getDefaultSecurityConfig();
   }

   public IDManager getMessageIDManager()
   {
      return messageIDManager;
   }

   public IDManager getChannelIDManager()
   {
      return channelIDManager;
   }

   public ServerInvocationHandler getInvocationHandler()
   {
      return handler;
   }

   public int getQueuedExecutorPoolSize()
   {
      return queuedExecutorPoolSize;
   }

   public void setQueuedExecutorPoolSize(int poolSize)
   {
      this.queuedExecutorPoolSize = poolSize;
   }
   
   public long getFailoverStartTimeout()
   {
      return this.failoverStartTimeout;
   }
   
   public void setFailoverStartTimeout(long timeout)
   {
      this.failoverStartTimeout = timeout;
   }
   
   public long getFailoverCompleteTimeout()
   {
      return this.failoverCompleteTimeout;
   }
   
   public void setFailoverCompleteTimeout(long timeout)
   {
      this.failoverCompleteTimeout = timeout;
   }
   
   

   // JMX Operations ------------------------------------------------

   public String createQueue(String name, String jndiName) throws Exception
   {
      try
      {
         return createDestinationDefault(true, name, jndiName);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " createQueue");
      }
   }

   public String createQueue(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      try
      {
         return createDestination(true, name, jndiName, fullSize, pageSize, downCacheSize);
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

   public String createTopic(String name, String jndiName) throws Exception
   {
      try
      {
         return createDestinationDefault(false, name, jndiName);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " createTopic");
      }
   }

   public String createTopic(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      try
      {
         return createDestination(false, name, jndiName, fullSize, pageSize, downCacheSize);
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

   // Public --------------------------------------------------------
   
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

   public Queue getDLQ() throws Exception
   {
      if (dlqName == null)
      {
         //No DLQ name specified so there is no DLQ
         return null;
      }

      PostOffice postOffice = getPostOfficeInstance();

      Binding binding = postOffice.getBindingForQueueName(dlqName);
      
      if (binding != null && binding.getQueue().isActive())
      {
         return binding.getQueue();
      }
      else
      {
         return null;
      }   
   }

   public TransactionRepository getTxRepository()
   {
      return txRepository;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   public byte[] getClientAOPConfig()
   {
      return clientAOPConfig;
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
         postOffice = (PostOffice)getServer().
            getAttribute(postOfficeObjectName, "Instance");

         // We also inject the replicator dependency into the ConnectionFactoryJNDIMapper. This is
         // a bit messy but we have a circular dependency POJOContainer should be able to help us
         // here. Yes, this is nasty.

         if (!postOffice.isLocal())
         {
            Replicator rep = (Replicator)postOffice;
            connFactoryJNDIMapper.injectReplicator(rep);
            rep.registerListener(new FailoverListener());

         }
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

   public QueuedExecutorPool getQueuedExecutorPool()
   {
      return queuedExecutorPool;
   }
   
   /*
    * Wait for failover from the specified node to complete.
    */
   public int waitForFailover(int failedNodeID) throws Exception
   {
      // This node may be failing over for another node - in which case we must wait for that to be
      // complete.
      
      log.info(this + " waiting for server-side failover for failed node " + failedNodeID + " to complete");
      
      Replicator replicator = getReplicator();

      long startToWait = failoverStartTimeout;
      long completeToWait = failoverCompleteTimeout;
                     
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
                     log.info(this + ": failover is complete on node " + nid);
                     return nid.intValue();
                  }
                  else if (status.isFailingOverForNode(failedNodeID))
                  {
                     log.info(this + ": fail over is in progress on node " + nid);
                     
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
                        log.info(this + " already waited long enough for failover to complete, giving up");
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
                  log.info(this + " already waited long enough for failover to start, giving up");
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * Place a Recoverable instance in the JNDI tree. This can be used by a transaction manager in
    * order to obtain an XAResource so it can perform XA recovery.
    */

   //Commented out until XA Recovery is complete

//   private void createRecoverable() throws Exception
//   {
//      //Disabled until XA Recovery is complete with Arjuna transaction integration
//
//      InitialContext ic = new InitialContext();
//
//      int connFactoryID = connFactoryJNDIMapper.registerConnectionFactory(null, null);
//
//      XAConnectionFactory xaConnFactory =
//         (XAConnectionFactory)connFactoryJNDIMapper.getConnectionFactory(connFactoryID);
//
//      JMSRecoverable recoverable = new JMSRecoverable(serverPeerID, xaConnFactory);
//
//      Context recCtx = null;
//      try
//      {
//         recCtx = (Context)ic.lookup(RECOVERABLE_CTX_NAME);
//      }
//      catch (NamingException e)
//      {
//         //Ignore
//      }
//
//      if (recCtx == null)
//      {
//         recCtx = ic.createSubcontext(RECOVERABLE_CTX_NAME);
//      }
//
//      recCtx.rebind(this.serverPeerID, recoverable);
//   }
//
//   private void removeRecoverable() throws Exception
//   {
//      InitialContext ic = new InitialContext();
//
//      Context recCtx = null;
//      try
//      {
//         recCtx = (Context)ic.lookup(RECOVERABLE_CTX_NAME);
//         recCtx.unbind(serverPeerID);
//      }
//      catch (NamingException e)
//      {
//         //Ignore
//      }
//   }

   private void initializeRemoting(MBeanServer mbeanServer) throws Exception
   {
      JMSWireFormat wf = new JMSWireFormat();

      MarshalFactory.addMarshaller("jms", wf, wf);

      handler = new JMSServerInvocationHandler();
   }

   private void loadServerAOPConfig() throws Exception
   {
      URL url = this.getClass().getClassLoader().getResource("aop-messaging-server.xml");
      AspectXmlLoader.deployXML(url);
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
         clientAOPConfig = os.toByteArray();
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

   private String createDestinationDefault(boolean isQueue, String name, String jndiName)
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

      return createDestinationInternal(destinationMBeanConfig, on, jndiName, false, -1, -1, -1);
   }

   private String createDestinationInternal(String destinationMBeanConfig, ObjectName on,
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

      //
      // end of TODO
      //
   }

   private String createDestination(boolean isQueue, String name, String jndiName, int fullSize,
                                    int pageSize, int downCacheSize) throws Exception
   {
      //
      // TODO - THIS IS A TEMPORARY IMPLEMENTATION; WILL BE REPLACED WITH INTEGRATION-CONSISTENT ONE
      // TODO - if I find a way not using UnifiedClassLoader3 directly, then get rid of
      //        <path refid="jboss.jmx.classpath"/> from jms/build.xml dependentmodule.classpath
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

      return createDestinationInternal(destinationMBeanConfig, on, jndiName, true, fullSize,
                                       pageSize, downCacheSize);
   }

   private boolean destroyDestination(boolean isQueue, String name) throws Exception
   {
      String destType = isQueue ? "Queue" : "Topic";
      String ons ="jboss.messaging.destination:service="+ destType + ",name=" + name;
      ObjectName on = new ObjectName(ons);

      MBeanServer mbeanServer = getServer();

      // we can only destroy destinations that exist AND that have been created programatically
      if (!mbeanServer.isRegistered(on))
      {
         return false;
      }
      Boolean b = (Boolean)mbeanServer.getAttribute(on, "CreatedProgrammatically");
      if (!b.booleanValue())
      {
         log.warn("Cannot destroy a destination that has not been created programatically");
         return false;
      }
      mbeanServer.invoke(on, "stop", new Object[0], new String[0]);
      mbeanServer.invoke(on, "destroy", new Object[0], new String[0]);
      mbeanServer.unregisterMBean(on);
      return true;
   }

   // Inner classes -------------------------------------------------
   
   private class FailoverListener implements ReplicationListener
   {
      public void onReplicationChange(Serializable key, Map updatedReplicantMap,
                                      boolean added, int originatingNodeId)
      {
         if (key.equals(DefaultClusteredPostOffice.FAILED_OVER_FOR_KEY))
         {
            // We have a failover status change - notify anyone waiting

            log.debug(ServerPeer.this + ".FailoverListener got failover event, notifying those waiting on lock");

            synchronized (failoverStatusLock)
            {
               failoverStatusLock.notifyAll();
            }
         }
      }      
   }
}
