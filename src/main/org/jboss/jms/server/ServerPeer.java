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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.ConnectionFactory;
import javax.jms.XAConnectionFactory;
import javax.jms.JMSException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.Attribute;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.aop.AspectXmlLoader;
import org.jboss.aop.Dispatcher;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryEndpoint;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.jms.server.security.SecurityManager;
import org.jboss.jms.server.plugin.contract.ThreadPoolDelegate;
import org.jboss.jms.server.plugin.contract.DurableSubscriptionStoreDelegate;
import org.jboss.jms.server.plugin.contract.MessageStoreDelegate;
import org.jboss.jms.server.plugin.JDBCDurableSubscriptionStore;
import org.jboss.jms.server.plugin.PersistentMessageStore;
import org.jboss.jms.tx.JMSRecoverable;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.TransactionLogDelegate;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.util.Util;
import org.jboss.remoting.Client;
import org.jboss.remoting.ConnectionListener;
import org.jboss.remoting.InvokerLocator;
import org.jboss.system.ServiceCreator;
import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;

/**
 * A JMS server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerPeer extends ServiceMBeanSupport implements DestinationManager
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerPeer.class);

   private static final String CONNECTION_FACTORY_JNDI_NAME = "ConnectionFactory";
   
   private static final String XACONNECTION_FACTORY_JNDI_NAME = "XAConnectionFactory";
   
   public static final String RECOVERABLE_CTX_NAME = "jms-recoverables";

   public static final String DEFAULT_QUEUE_CONTEXT = "/queue";
   public static final String DEFAULT_TOPIC_CONTEXT = "/topic";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String serverPeerID;
   protected ObjectName connectorName;

   //protected Connector connector;
   
   protected InvokerLocator locator;
   protected byte[] clientAOPConfig;
   private Version version;

   protected boolean started;

   protected int connFactoryIDSequence;
   protected Map connFactoryDelegates;

   // wired components

   protected DestinationJNDIMapper destinationJNDIMapper;
   protected SecurityManager sm;
   protected ClientManager clientManager;
   protected TransactionRepository txRepository;
   protected JMSServerInvocationHandler handler;
   protected ConnectionListener connectionListener;

   // plugins

   protected ObjectName threadPoolObjectName;
   protected ThreadPoolDelegate threadPoolDelegate;
   protected ObjectName transactionLogObjectName;
   protected TransactionLogDelegate transactionLogDelegate;
   protected ObjectName messageStoreObjectName;
   protected MessageStoreDelegate messageStoreDelegate;
   protected ObjectName durableSubscriptionStoreObjectName;
   protected DurableSubscriptionStoreDelegate durableSubscriptionStoreDelegate;

   // Constructors --------------------------------------------------

   public ServerPeer(String serverPeerID) throws Exception
   {
      this.serverPeerID = serverPeerID;
      
      this.connFactoryDelegates = new HashMap();

      // the default value to use, unless the JMX attribute is modified
      connectorName = new ObjectName("jboss.remoting:service=Connector,transport=socket");
      
      sm = new SecurityManager();

      version = new Version("VERSION");

      started = false;
   }

   // Public --------------------------------------------------------

   // TODO
   // The future Server interface -----------------------------------

   public Object getInstance()
   {
      return this;
   }

   // DestinationManager interface ----------------------------------

   public String registerDestination(boolean isQueue, String name, String jndiName,
                                     Element securityConfiguration) throws JMSException
   {
      jndiName = destinationJNDIMapper.registerDestination(isQueue, name, jndiName);

      if (securityConfiguration == null)
      {
         // relying on the server's default
         securityConfiguration = getDefaultSecurityConfig();
      }

      sm.setSecurityConfig(name, securityConfiguration);
      return jndiName;
   }
   
   public void unregisterDestination(boolean isQueue, String name) throws JMSException
   {
      destinationJNDIMapper.unregisterDestination(isQueue, name);
      sm.clearSecurityConfig(name);
   }

   public CoreDestination getCoreDestination(boolean isQueue, String name) throws JMSException
   {
      return destinationJNDIMapper.getCoreDestination(isQueue, name);
   }

   public CoreDestination getCoreDestination(javax.jms.Destination d) throws JMSException
   {
      boolean isQueue = d instanceof javax.jms.Queue;
      String name =
         isQueue ? ((javax.jms.Queue)d).getQueueName() : ((javax.jms.Topic)d).getTopicName();

      return getCoreDestination(isQueue, name);
   }

   public void createTemporaryDestination(javax.jms.Destination d) throws JMSException
   {
      destinationJNDIMapper.createTemporaryDestination(d);
   }

   public void destroyTemporaryDestination(javax.jms.Destination d) throws JMSException
   {
      destinationJNDIMapper.destroyTemporaryDestination(d);
   }

   public Element getDefaultSecurityConfiguration()
   {
      return sm.getDefaultSecurityConfig();
   }

   public void setSecurityConfiguration(boolean isQueue, String name, Element securityConfig)
      throws JMSException
   {
      sm.setSecurityConfig(name, securityConfig);
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

         loadClientAOPConfig();

         loadServerAOPConfig();

         MBeanServer mbeanServer = getServer();

         // Acquire references to plugins. Each plug-in will be accessed directly via a reference
         // circumventing the MBeanServer. However, they are installed as services to take advantage
         // of their automatically-creating management interface.

         threadPoolDelegate =
            (ThreadPoolDelegate)mbeanServer.getAttribute(threadPoolObjectName, "Instance");

         transactionLogDelegate =
            (TransactionLogDelegate)mbeanServer.getAttribute(transactionLogObjectName, "Instance");

         // TODO: is should be possible to share this with other peers
         messageStoreDelegate =
            (MessageStoreDelegate)mbeanServer.getAttribute(messageStoreObjectName, "Instance");

         // TODO this assignments should go away
         ((PersistentMessageStore)messageStoreDelegate).setStoreID(serverPeerID);
         ((PersistentMessageStore)messageStoreDelegate).setTransactionLog(transactionLogDelegate);

         durableSubscriptionStoreDelegate = (DurableSubscriptionStoreDelegate)mbeanServer.
            getAttribute(durableSubscriptionStoreObjectName, "Instance");

         // TODO this assignments should go away
         ((JDBCDurableSubscriptionStore)durableSubscriptionStoreDelegate).setServerPeer(this);
         ((JDBCDurableSubscriptionStore)durableSubscriptionStoreDelegate).setTransactionLog(transactionLogDelegate);
         ((JDBCDurableSubscriptionStore)durableSubscriptionStoreDelegate).setMessageStore(messageStoreDelegate);

         // initialize the rest of the internal components

         txRepository = new TransactionRepository(transactionLogDelegate);
         txRepository.loadPreparedTransactions();

         clientManager = new ClientManagerImpl(this);

         destinationJNDIMapper = new DestinationJNDIMapper(this);

         sm.init();

         initializeRemoting(mbeanServer);

         setupConnectionFactories();

         createRecoverable();

         started = true;

         log.info("JMS " + this + " started");
      }
      catch (Exception e)
      {
         log.error("Failed to start", e);
      }
   }

   public synchronized void stopService() throws Exception
   {
      if (!started)
      {
         return;
      }

      log.debug(this + " stopping");

      unloadServerAOPConfig();

      tearDownConnectionFactories();

      destinationJNDIMapper.destroyAllDestinations();

      txRepository = null;
      clientManager = null;
      destinationJNDIMapper = null;

      //remove the connection listener
//      mbeanServer.invoke(connectorName, "removeConnectionListener",
//            new Object[] {connectionListener},
//            new String[] {"org.jboss.remoting.ConnectionListener"});

      // remove the JMS subsystem invocation handler
      getServer().invoke(connectorName, "removeInvocationHandler",
                         new Object[] {"JMS"},
                         new String[] {"java.lang.String"});

//      connector.removeInvocationHandler("JMS");
//      connector.stop();
//      connector.destroy();

      removeRecoverable();

      started = false;

      log.info("JMS " + this + " stopped");
   }

   //
   // JMX operations
   //

   public String createQueue(String name, String jndiName) throws Exception
   {
      return createDestination(true, name, jndiName);
   }

   public boolean destroyQueue(String name) throws Exception
   {
      return destroyDestination(true, name);
   }

   public String createTopic(String name, String jndiName) throws Exception
   {
      return createDestination(false, name, jndiName);
   }

   public boolean destroyTopic(String name) throws Exception
   {
      return destroyDestination(false, name);
   }

   public Set getDestinations() throws Exception
   {
      return destinationJNDIMapper.getDestinations();
   }

   //
   // end of JMX operations
   //

   //
   // JMX attributes
   //

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

   public ObjectName getThreadPool()
   {
      return threadPoolObjectName;
   }

   public void setThreadPool(ObjectName on)
   {
      threadPoolObjectName = on;
   }

   public ObjectName getTransactionLog()
   {
      return transactionLogObjectName;
   }

   public void setTransactionLog(ObjectName on)
   {
      transactionLogObjectName = on;
   }

   public ObjectName getMessageStore()
   {
      return messageStoreObjectName;
   }

   public void setMessageStore(ObjectName on)
   {
      messageStoreObjectName = on;
   }

   public ObjectName getDurableSubscriptionStore()
   {
      return durableSubscriptionStoreObjectName;
   }

   public void setDurableSubscriptionStore(ObjectName on)
   {
      durableSubscriptionStoreObjectName = on;
   }

   // TODO review these below

   public String getServerPeerID()
   {
      return serverPeerID;
   }

   public String getLocatorURI()
   {
      if (locator == null)
      {
         return null;
      }
      return locator.getLocatorURI();
   }

   public ObjectName getConnector()
   {
      return connectorName;
   }

   public void setConnector(ObjectName on)
   {
      connectorName = on;
   }
      
   public void setSecurityDomain(String securityDomain)
   {
      sm.setSecurityDomain(securityDomain);
   }
   
   public String getSecurityDomain()
   {
      return sm.getSecurityDomain();
   }

   public void setDefaultSecurityConfig(Element conf) throws Exception
   {
      sm.setDefaultSecurityConfig(conf);
   }
   
   public Element getDefaultSecurityConfig()
   {
      return sm.getDefaultSecurityConfig();
   }
   
   //
   // end of JMX attributes
   //

   public boolean isDeployed(boolean isQueue, String name)
   {
      return destinationJNDIMapper.isDeployed(isQueue, name);
   }

   public DurableSubscriptionStoreDelegate getDurableSubscriptionStoreDelegate()
   {
      return durableSubscriptionStoreDelegate;
   }

   public TransactionRepository getTxRepository()
   {
      return txRepository;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   public ClientManager getClientManager()
   {
      return clientManager;
   }
   
   public DestinationJNDIMapper getDestinationManager()
   {
      return destinationJNDIMapper;
   }

   public SecurityManager getSecurityManager()
   {
      return sm;
   }

   public ConnectionFactoryDelegate getConnectionFactoryDelegate(String connectionFactoryID)
   {
      return (ConnectionFactoryDelegate)connFactoryDelegates.get(connectionFactoryID);
   }

   public byte[] getClientAOPConfig()
   {
      return clientAOPConfig;
   }

   public Version getVersion()
   {
      return version;
   }

   // access to plugin references

   public ThreadPoolDelegate getThreadPoolDelegate()
   {
      return threadPoolDelegate;
   }

   public TransactionLogDelegate getTransactionLogDelegate()
   {
      return transactionLogDelegate;
   }

   public MessageStoreDelegate getMessageStoreDelegate()
   {
      return messageStoreDelegate;
   }

   public String toString()
   {
      return "ServerPeer [" + getServerPeerID() + "]";
   }
   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   /*
    * Place a Recoverable instance in the JNDI tree.
    * This can be used by a transaction manager in order to obtain
    * an XAResource so it can perform XA recovery
    */
   private void createRecoverable() throws Exception
   {
      InitialContext ic = new InitialContext();
      
      JMSRecoverable recoverable =
         new JMSRecoverable(this.serverPeerID, (XAConnectionFactory)setupConnectionFactory(null));
      
      Context recCtx = null;
      try
      {
         recCtx = (Context)ic.lookup(RECOVERABLE_CTX_NAME);
      }
      catch (NamingException e)
      {
         //Ignore
      }
      
      if (recCtx == null)
      {
         recCtx = ic.createSubcontext(RECOVERABLE_CTX_NAME);
      }
      
      recCtx.rebind(this.serverPeerID, recoverable);
   }
   
   private void removeRecoverable() throws Exception
   {
      InitialContext ic = new InitialContext();
      
      Context recCtx = null;
      try
      {
         recCtx = (Context)ic.lookup(RECOVERABLE_CTX_NAME);
         recCtx.unbind(serverPeerID);
      }
      catch (NamingException e)
      {
         //Ignore
      }   
   }

//   /**
//    * @return - may return null if it doesn't find a "jboss" MBeanServer.
//    */
//   private MBeanServer findMBeanServer()
//   {
//      System.setProperty("jmx.invoke.getters", "true");
//
//      MBeanServer result = null;
//      ArrayList l = MBeanServerFactory.findMBeanServer(null);
//      for(Iterator i = l.iterator(); i.hasNext(); )
//      {
//         MBeanServer s = (MBeanServer)l.iterator().next();
//         if ("jboss".equals(s.getDefaultDomain()))
//         {
//            result = s;
//            break;
//         }
//      }
//      return result;
//   }

   private void initializeRemoting(MBeanServer mbeanServer) throws Exception
   {
      String s = (String)mbeanServer.invoke(connectorName, "getInvokerLocator",
                                            new Object[0], new String[0]);

      locator = new InvokerLocator(s);
      
      //Note - There seems to be a bug (feature?) in JBoss Remoting which if you specify
      //a socket timeout on the invoker locator URI then it always makes a remote call
      //even when in the same JVM

      log.debug("LocatorURI: " + getLocatorURI());

      // add the JMS subsystem invocation handler
      
      handler = new JMSServerInvocationHandler();

      mbeanServer.invoke(connectorName, "addInvocationHandler",
                         new Object[] {"JMS", handler},
                         new String[] {"java.lang.String",
                                       "org.jboss.remoting.ServerInvocationHandler"});  
      
      //install the connection listener that listens for failed connections
//      connectionListener = new ServerConnectionListener();
//      
//      mbeanServer.invoke(connectorName, "addConnectionListener",
//            new Object[] {connectionListener},
//            new String[] {"org.jboss.remoting.ConnectionListener"}); 
      
//      connector = new Connector();
//      locator = new InvokerLocator("multiplex://0.0.0.0:9099");
//      connector.setInvokerLocator(locator.getLocatorURI());
//      connector.create();
//      handler = new JMSServerInvocationHandler();
//      connector.addInvocationHandler("JMS", handler);
//      connector.start(); 
      
   }


   private void setupConnectionFactories() throws Exception
   {
      ConnectionFactory cf = setupConnectionFactory(null);
      InitialContext ic = new InitialContext();
      
      //Bind in global JNDI namespace      
      ic.rebind(CONNECTION_FACTORY_JNDI_NAME, cf);
      ic.rebind(XACONNECTION_FACTORY_JNDI_NAME, cf);

      ic.rebind("java:/" + CONNECTION_FACTORY_JNDI_NAME, cf);
      ic.rebind("java:/" + XACONNECTION_FACTORY_JNDI_NAME, cf);

      //And now the connection factories and links as required by the TCK
      //See section 4.4.15 of the TCK user guide.
      //FIXME - these should be removed when connection factories are deployable via mbeans

      Context jmsContext = null;
      try
      {
         jmsContext = (Context)ic.lookup("jms");
      }
      catch (Exception ignore)
      {         
      }
      if (jmsContext == null)
      {
         jmsContext = ic.createSubcontext("jms");
      }
      
      jmsContext.rebind("QueueConnectionFactory", cf);
      jmsContext.rebind("TopicConnectionFactory", cf);

      jmsContext.rebind("DURABLE_SUB_CONNECTION_FACTORY", setupConnectionFactory("cts"));
      jmsContext.rebind("MDBTACCESSTEST_FACTORY", setupConnectionFactory("cts1"));
      jmsContext.rebind("DURABLE_BMT_CONNECTION_FACTORY", setupConnectionFactory("cts2"));
      jmsContext.rebind("DURABLE_CMT_CONNECTION_FACTORY", setupConnectionFactory("cts3"));
      jmsContext.rebind("DURABLE_BMT_XCONNECTION_FACTORY", setupConnectionFactory("cts4"));
      jmsContext.rebind("DURABLE_CMT_XCONNECTION_FACTORY", setupConnectionFactory("cts5"));
      jmsContext.rebind("DURABLE_CMT_TXNS_XCONNECTION_FACTORY", setupConnectionFactory("cts6"));
      
      ic.close();
   }

   private ConnectionFactory setupConnectionFactory(String clientID) throws Exception
   {
      String id = genConnFactoryID();

      ServerConnectionFactoryEndpoint endpoint =
         new ServerConnectionFactoryEndpoint(id, this, clientID);

      Dispatcher.singleton.registerTarget(id, endpoint);
      connFactoryDelegates.put(id, endpoint);

      ClientConnectionFactoryDelegate delegate;
      try
      {
         delegate = new ClientConnectionFactoryDelegate(id, getLocatorURI());
      }
      catch (Exception e)
      {
         throw new JBossJMSException("Failed to create connection factory delegate", e);
      }

      JBossConnectionFactory connFactory = new JBossConnectionFactory(delegate);
      return connFactory;
   }

   private void tearDownConnectionFactories()
      throws Exception
   {
      InitialContext ic = new InitialContext();

      //TODO
      //FIXME - this is a hack. It should be removed once a better way to manage
      //connection factories is implemented
      ic.unbind("jms/DURABLE_SUB_CONNECTION_FACTORY");
      ic.unbind("jms/MDBTACCESSTEST_FACTORY");
      ic.unbind("jms/DURABLE_BMT_CONNECTION_FACTORY");
      ic.unbind("jms/DURABLE_CMT_CONNECTION_FACTORY");
      ic.unbind("jms/DURABLE_BMT_XCONNECTION_FACTORY");
      ic.unbind("jms/DURABLE_CMT_XCONNECTION_FACTORY");
      ic.unbind("jms/DURABLE_CMT_TXNS_XCONNECTION_FACTORY");

      ic.unbind(CONNECTION_FACTORY_JNDI_NAME);
      ic.unbind(XACONNECTION_FACTORY_JNDI_NAME);

      ic.unbind("java:/" + CONNECTION_FACTORY_JNDI_NAME);
      ic.unbind("java:/" + XACONNECTION_FACTORY_JNDI_NAME);

      ic.close();
   }


   private synchronized String genConnFactoryID()
   {
      return "CONNFACTORY" + connFactoryIDSequence++;
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

   private String createDestination(boolean isQueue, String name, String jndiName) throws Exception
   {
      //
      // TODO - THIS IS A TEMPORARY IMPLEMENTATION; WILL BE REPLACED WITH INTEGRATION-CONSISTENT ONE
      //

      String destType = isQueue ? "Queue" : "Topic";
      String className = "org.jboss.jms.server.destination." + destType;
      String ons ="jboss.messaging.destination:service="+ destType + ",name=" + name;
      ObjectName on = new ObjectName(ons);

      MBeanServer mbeanServer = getServer();

      String destinationMBeanConfig =
         "<mbean code=\"" + className + "\" " +
         "       name=\"" + ons + "\" " +
         "       xmbean-dd=\"xmdesc/" + destType + "-xmbean.xml\">\n" +
         "    <constructor>" +
         "        <arg type=\"boolean\" value=\"true\"/>" +
         "    </constructor>" +
         "</mbean>";

      Element element = Util.stringToElement(destinationMBeanConfig);

      ServiceCreator sc = new ServiceCreator(mbeanServer);
      sc.install(on, null, element);

      // inject dependencies
      mbeanServer.setAttribute(on, new Attribute("ServerPeer", getServiceName()));
      mbeanServer.setAttribute(on, new Attribute("JNDIName", jndiName));
      mbeanServer.invoke(on, "create", new Object[0], new String[0]);
      mbeanServer.invoke(on, "start", new Object[0], new String[0]);

      return (String)mbeanServer.getAttribute(on, "JNDIName");
   }

   private boolean destroyDestination(boolean isQueue, String name) throws Exception
   {
      String destType = isQueue ? "Queue" : "Topic";
      String ons ="jboss.messaging.destination:service="+ destType + ",name=" + name;
      ObjectName on = new ObjectName(ons);

      MBeanServer mbeanServer = getServer();

      // we can only destroy destinations that have been created programatically
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
   
   class ServerConnectionListener implements ConnectionListener
   {

      public void handleConnectionException(Throwable t, Client client)
      {
         
      }
      
   }

}
