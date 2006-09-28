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
import java.net.URL;
import java.util.Map;
import java.util.Set;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.aop.AspectXmlLoader;
import org.jboss.jms.server.connectionfactory.ConnectionFactoryJNDIMapper;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.jms.server.connectormanager.SimpleConnectorManager;
import org.jboss.jms.server.endpoint.ServerConsumerEndpoint;
import org.jboss.jms.server.plugin.contract.JMSUserManager;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.server.security.SecurityMetadataStore;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.memory.SimpleMemoryManager;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.contract.ShutdownLogger;
import org.jboss.messaging.core.plugin.postoffice.DefaultPostOffice;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.util.Util;
import org.jboss.mx.loading.UnifiedClassLoader3;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.marshal.MarshalFactory;
import org.jboss.remoting.serialization.SerializationStreamFactory;
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

   //public static final String RECOVERABLE_CTX_NAME = "jms-recoverables";

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
   
   private boolean crashed;

   // wired components

   private DestinationJNDIMapper destinationJNDIMapper;
   private SecurityMetadataStore securityStore;
   private ConnectionFactoryJNDIMapper connFactoryJNDIMapper;
   private TransactionRepository txRepository;
   private ConnectionManager connectionManager;
   private ConnectorManager connectorManager;
   private IdManager messageIdManager;
   private IdManager channelIdManager;
   private IdManager transactionIdManager;
   private MemoryManager memoryManager;
   private QueuedExecutorPool queuedExecutorPool;
   private MessageStore messageStore;   

   // plugins

   protected ObjectName persistenceManagerObjectName;
   protected PersistenceManager persistenceManager;
   
   protected ObjectName queuePostOfficeObjectName;
   protected DefaultPostOffice queuePostOffice;
   
   protected ObjectName topicPostOfficeObjectName;
   protected DefaultPostOffice topicPostOffice;
     
   protected ObjectName jmsUserManagerObjectName;
   protected JMSUserManager jmsUserManager;
   
   protected ObjectName shutdownLoggerObjectName;
   protected ShutdownLogger shutdownLogger;

   //Other stuff
   
   private JMSServerInvocationHandler handler;

   // We keep a map of consumers to prevent us to recurse through the attached session in order to
   // find the ServerConsumerDelegate so we can acknowledge the message. Originally, this map was
   // maintained per-connection, but with the http://jira.jboss.org/jira/browse/JBMESSAGING-211 bug
   // we shared it among connections, so a transaction submitted on the "wrong" connection can
   // still succeed. For more details, see the JIRA issue.

   private Map consumers;

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
      
      consumers = new ConcurrentReaderHashMap();

      version = Version.instance();

      started = false;
   }

   // ServiceMBeanSupport overrides ---------------------------------

   public synchronized void startService() throws Exception
   {
      try
      {
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
         
         shutdownLogger = (ShutdownLogger)mbeanServer.
            getAttribute(shutdownLoggerObjectName, "Instance");
         
         //We get references to some plugins lazily to avoid problems with circular
         //MBean dependencies
            
         // Create the wired components
         messageIdManager = new IdManager("MESSAGE_ID", 4096, persistenceManager);
         channelIdManager = new IdManager("CHANNEL_ID", 10, persistenceManager);
         transactionIdManager = new IdManager("TRANSACTION_ID", 1024, persistenceManager);
         destinationJNDIMapper = new DestinationJNDIMapper(this);
         connFactoryJNDIMapper = new ConnectionFactoryJNDIMapper(this);
         connectionManager = new SimpleConnectionManager();
         connectorManager = new SimpleConnectorManager();
         memoryManager = new SimpleMemoryManager();
         messageStore = new SimpleMessageStore();         
         txRepository = new TransactionRepository(persistenceManager, transactionIdManager);
 
         // Start the wired components
   
         messageIdManager.start();
         channelIdManager.start();
         transactionIdManager.start();
         destinationJNDIMapper.start();
         connFactoryJNDIMapper.start();
         connectionManager.start();
         connectorManager.start();         
         memoryManager.start();
         messageStore.start();
         securityStore.start();
         txRepository.start();
         
         //Did the server crash last time?
         
         //TODO do we need this?
         crashed = shutdownLogger.startup(serverPeerID);                            

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
   
         shutdownLogger.shutdown(serverPeerID);         
                  
         // Stop the wired components         
         
         messageIdManager.start();
         messageIdManager = null;
         channelIdManager.start();
         channelIdManager = null;
         transactionIdManager.start();
         transactionIdManager = null;
         destinationJNDIMapper.start();
         destinationJNDIMapper = null;
         connFactoryJNDIMapper.start();
         connFactoryJNDIMapper = null;
         connectionManager.start();
         connectionManager = null;
         connectorManager.start(); 
         connectorManager = null;
         memoryManager.start();
         memoryManager = null;
         messageStore.start();
         messageStore = null;
         securityStore.start();
         securityStore = null;
         txRepository.start();
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

   public ObjectName getPersistenceManager()
   {
      return persistenceManagerObjectName;
   }

   public void setPersistenceManager(ObjectName on)
   {
      persistenceManagerObjectName = on;
   }
   
   public ObjectName getQueuePostOffice()
   {
      return queuePostOfficeObjectName;
   }

   public void setQueuePostOffice(ObjectName on)
   {
      queuePostOfficeObjectName = on;
   }
   
   public ObjectName getTopicPostOffice()
   {
      return topicPostOfficeObjectName;
   }

   public void setTopicPostOffice(ObjectName on)
   {
      topicPostOfficeObjectName = on;
   }
   
   public ObjectName getJmsUserManager()
   {
      return jmsUserManagerObjectName;
   }

   public void setJMSUserManager(ObjectName on)
   {
      jmsUserManagerObjectName = on;
   }
   
   public ObjectName getShutdownLogger()
   {
      return shutdownLoggerObjectName;
   }

   public void setShutdownLogger(ObjectName on)
   {
      shutdownLoggerObjectName = on;
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

   public IdManager getMessageIdManager()
   {
      return messageIdManager;
   }
   
   public IdManager getChannelIdManager()
   {
      return channelIdManager;
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
   
   public boolean crashedLastTime()
   {
      return crashed;
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
   
   public PostOffice getQueuePostOfficeInstance() throws Exception
   {
      // We get the reference lazily to avoid problems with MBean circular dependencies
      if (queuePostOffice == null)
      {
         queuePostOffice = (DefaultPostOffice)getServer().
            getAttribute(queuePostOfficeObjectName, "Instance");
      }
      return queuePostOffice;
   }
      
   public PostOffice getTopicPostOfficeInstance() throws Exception
   {
      // We get the reference lazily to avoid problems with MBean circular dependencies
      if (topicPostOffice == null)
      {
         topicPostOffice = (DefaultPostOffice)getServer().
            getAttribute(topicPostOfficeObjectName, "Instance");
      }
      return topicPostOffice;        
   }
      
   public ShutdownLogger getShutdownLoggerInstance()
   {
      return shutdownLogger;
   }
      
   public synchronized int getNextObjectID()
   {
      return objectIDSequence++;
   }

   public ServerConsumerEndpoint putConsumerEndpoint(int consumerID, ServerConsumerEndpoint c)
   {
      log.debug(this + " caching consumer " + consumerID);
      return (ServerConsumerEndpoint)consumers.put(new Integer(consumerID), c);
   }

   public ServerConsumerEndpoint getConsumerEndpoint(int consumerID)
   {
      return (ServerConsumerEndpoint)consumers.get(new Integer(consumerID));
   }

   public ServerConsumerEndpoint removeConsumerEndpoint(Integer consumerID)
   {
      log.debug(this + " removing consumer " + consumerID + " from the cache");
      return (ServerConsumerEndpoint)consumers.remove(consumerID);
   }
   
   public QueuedExecutorPool getQueuedExecutorPool()
   {
      return queuedExecutorPool;
   }

   public String toString()
   {
      return "ServerPeer [" + getServerPeerID() + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * Place a Recoverable instance in the JNDI tree. This can be used by a transaction manager in
    * order to obtain an XAResource so it can perform XA recovery.
    */

//   //Commented out until XA Recovery is complete
//
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
      // We explicitly associate the datatype "jms" with the java SerializationManager
      // This is vital for performance reasons.
//      SerializationStreamFactory.setManagerClassName(
//         "jms", "org.jboss.remoting.serialization.impl.jboss.JBossSerializationManager");
      SerializationStreamFactory.setManagerClassName(
             "jms", "org.jboss.jms.server.remoting.MessagingSerializationManager");
      

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

      //FIXME - Yes this is super-ugly - there must be an easier way of doing it
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

}
