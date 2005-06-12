/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.remoting.InvokerLocator;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryDelegate;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.container.JMSInvocationHandler;
import org.jboss.jms.client.container.InvokerInterceptor;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.aop.ClassAdvisor;
import org.jboss.aop.DomainDefinition;
import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.util.PayloadKey;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.AcknowledgmentStore;
import org.jboss.messaging.core.util.MessageStoreImpl;
import org.jboss.messaging.core.util.InMemoryAcknowledgmentStore;
import org.jboss.messaging.core.util.transaction.TransactionManagerImpl;
import org.jboss.logging.Logger;

import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.Iterator;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;

/**
 * A JMS server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerPeer
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerPeer.class);

   // Static --------------------------------------------------------

   private final String CONNECTION_FACTORY_JNDI_NAME = "ConnectionFactory";
   private final String XACONNECTION_FACTORY_JNDI_NAME = "XAConnectionFactory";

   // Attributes ----------------------------------------------------

   protected String serverPeerID;
   protected InvokerLocator locator;
   protected ClientManager clientManager;
   protected DestinationManager destinationManager;
   protected ConnectionFactoryDelegate connFactoryDelegate;
   protected Hashtable jndiEnvironment;
   protected InitialContext initialContext;
   protected MBeanServer mbeanServer;

   protected boolean started;

   protected ClassAdvisor connFactoryAdvisor;
   protected ClassAdvisor connAdvisor;
   protected ClassAdvisor sessionAdvisor;
   protected ClassAdvisor producerAdvisor;
   protected ClassAdvisor consumerAdvisor;
	protected ClassAdvisor browserAdvisor;
   protected ClassAdvisor genericAdvisor;
   

   protected PooledExecutor threadPool;

   protected MessageStore messageStore;
   protected AcknowledgmentStore acknowledgmentStore;

   protected TransactionManager transactionManager;



   // Constructors --------------------------------------------------


   public ServerPeer(String serverPeerID)
   {
      this.serverPeerID = serverPeerID;
   }

   /**
    * @param jndiEnvironment - map containing JNDI properties. Useful for testing. Passing null
    *        means the server peer uses default JNDI properties.
    */
   public ServerPeer(String serverPeerID, Hashtable jndiEnvironment)
   {
      this(serverPeerID);
      this.jndiEnvironment = jndiEnvironment;
      started = false;
   }

   // Public --------------------------------------------------------

   //
   // JMX operations
   //

   public synchronized void create()
   {
      log.debug(this + " created");
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      log.debug(this + " starting");

      initialContext = new InitialContext(jndiEnvironment);

      mbeanServer = findMBeanServer();
      transactionManager = findTransactionManager();

      clientManager = new ClientManager(this);
      destinationManager = new DestinationManager(this);
      messageStore = new MessageStoreImpl("MessageStore");
      acknowledgmentStore = new InMemoryAcknowledgmentStore("AcknowledgmentStore");
      connFactoryDelegate = new ServerConnectionFactoryDelegate(this);
      threadPool = new PooledExecutor();

      initializeRemoting();
      initializeAdvisors();

      ConnectionFactory connectionFactory = createConnectionFactory();
      bindConnectionFactory(connectionFactory);
      started = true;

      log.debug(this + " started");
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      log.debug(this + " stopping");

      unbindConnectionFactory();
      tearDownAdvisors();
      started = false;

      log.debug(this + " stopped");

   }

   public synchronized void destroy()
   {
      log.debug(this + " destroyed");
   }


   public void createQueue(String name) throws Exception
   {
      // TODO - temporary implementation
      JBossQueue q = new JBossQueue(name);
      Context queues;
      try
      {
         queues = (Context)initialContext.lookup("queue");
      }
      catch(NamingException e)
      {
         queues = initialContext.createSubcontext("queue");
      }
      queues.rebind(name, q);
      destinationManager.addDestination(q);
   }

   public void createTopic(String name) throws Exception
   {
      // TODO - temporary implementation
      JBossTopic t = new JBossTopic(name);
      Context topics;
      try
      {
         topics = (Context)initialContext.lookup("topic");
      }
      catch(NamingException e)
      {
         topics = initialContext.createSubcontext("topic");
      }
      topics.rebind(name, t);
      destinationManager.addDestination(t);
   }


   //
   // end of JMX operations
   //

   //
   // JMX attributes
   //

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


   //
   // end of JMX attributes
   //

   public synchronized boolean isStarted()
   {
      return started;
   }

   public InvokerLocator getLocator()
   {
      return locator;
   }

   public ClientManager getClientManager()
   {
      return clientManager;
   }

   public DestinationManager getDestinationManager()
   {
      return destinationManager;
   }

   public ConnectionFactoryDelegate getConnectionFactoryDelegate()
   {
      return connFactoryDelegate;
   }



   public ClassAdvisor getConnectionFactoryAdvisor()
   {
      return connFactoryAdvisor;
   }

   public ClassAdvisor getConnectionAdvisor()
   {
      return connAdvisor;
   }

   public ClassAdvisor getSessionAdvisor()
   {
      return sessionAdvisor;
   }

   public ClassAdvisor getProducerAdvisor()
   {
      return producerAdvisor;
   }
   
   public ClassAdvisor getBrowserAdvisor()
   {
      return browserAdvisor;
   }

   public ClassAdvisor getConsumerAdvisor()
   {
      return consumerAdvisor;
   }



   public PooledExecutor getThreadPool()
   {
      return threadPool;
   }

   public MessageStore getMessageStore()
   {
      return messageStore;
   }

   public AcknowledgmentStore getAcknowledgmentStore()
   {
      return acknowledgmentStore;
   }

   public TransactionManager getTransactionManager()
   {
      return transactionManager;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer();
      sb.append("ServerPeer[id=");
      sb.append(getServerPeerID());
      sb.append("]");
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   Hashtable getJNDIEnvironment()
   {
      return jndiEnvironment;
   }

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private static String[] domainNames = { "ServerConnectionFactoryDelegate",
                                           "ServerConnectionDelegate",
                                           "ServerSessionDelegate",
                                           "ServerProducerDelegate",
														 "ServerConsumerDelegate",
                                           "ServerBrowserDelegate",
                                           "GenericTarget"};

   private void initializeAdvisors() throws Exception
   {

      ClassAdvisor[] advisors = new ClassAdvisor[7];

      for(int i = 0; i < domainNames.length; i++)
      {
         DomainDefinition domainDefinition = AspectManager.instance().getContainer(domainNames[i]);
         if (domainDefinition == null)
         {
            throw new RuntimeException("Domain " + domainNames[i] + " not found");
         }
         advisors[i] = new JMSAdvisor(domainNames[i], domainDefinition.getManager(), this);
         Class c = Class.forName("org.jboss.jms.server.endpoint." + domainNames[i]);
         advisors[i].attachClass(c);

         // register the advisor with the Dispatcher
         Dispatcher.singleton.registerTarget(advisors[i].getName(), advisors[i]);
      }
      connFactoryAdvisor = advisors[0];
      connAdvisor = advisors[1];
      sessionAdvisor = advisors[2];
      producerAdvisor = advisors[3];
		consumerAdvisor = advisors[4];		
      browserAdvisor = advisors[5];
      genericAdvisor = advisors[6];
   }

   private void tearDownAdvisors() throws Exception
   {
      for(int i = 0; i < domainNames.length; i++)
      {
         Dispatcher.singleton.unregisterTarget(domainNames[i]);
      }
      connFactoryAdvisor = null;
      connAdvisor = null;
      sessionAdvisor = null;
      producerAdvisor = null;
      browserAdvisor = null;
   }

   private ConnectionFactory createConnectionFactory() throws Exception
   {
      ConnectionFactoryDelegate proxy = (ConnectionFactoryDelegate)createProxy();
      return new JBossConnectionFactory(proxy);
   }

   private Object createProxy() throws Exception
   {
      Serializable oid = connFactoryAdvisor.getName();
      String stackName = "ConnectionFactoryStack";
      AdviceStack stack = AspectManager.instance().getAdviceStack(stackName);
      // TODO why do I need an advisor to create an interceptor stack?
      Interceptor[] interceptors = stack.createInterceptors(connFactoryAdvisor, null);
      JMSInvocationHandler h = new JMSInvocationHandler(interceptors);

      SimpleMetaData metadata = new SimpleMetaData();
      // TODO: The ConnectionFactoryDelegate and ConnectionDelegate share the same locator (TCP/IP connection?). Performance?
      metadata.addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, oid, PayloadKey.AS_IS);
      metadata.addMetaData(InvokerInterceptor.REMOTING,
                           InvokerInterceptor.INVOKER_LOCATOR,
                           locator,
                           PayloadKey.AS_IS);
      metadata.addMetaData(InvokerInterceptor.REMOTING,
                           InvokerInterceptor.SUBSYSTEM,
                           "JMS",
                           PayloadKey.AS_IS);
      h.getMetaData().mergeIn(metadata);

      // TODO 
      ClassLoader loader = getClass().getClassLoader();
      Class[] interfaces = new Class[] { ConnectionFactoryDelegate.class };
      return Proxy.newProxyInstance(loader, interfaces, h);
   }

   private void bindConnectionFactory(ConnectionFactory factory) throws Exception
   {
      initialContext.rebind(CONNECTION_FACTORY_JNDI_NAME, factory);
      initialContext.rebind(XACONNECTION_FACTORY_JNDI_NAME, factory);

   }

   private void unbindConnectionFactory() throws Exception
   {
      initialContext.unbind(CONNECTION_FACTORY_JNDI_NAME);
      initialContext.unbind(XACONNECTION_FACTORY_JNDI_NAME);

   }

   /**
    * @return - may return null if it doesn't find a "jboss" MBeanServer.
    */
   private MBeanServer findMBeanServer()
   {
      MBeanServer result = null;
      ArrayList l = MBeanServerFactory.findMBeanServer(null);
      for(Iterator i = l.iterator(); i.hasNext(); )
      {
         MBeanServer s = (MBeanServer)l.iterator().next();
         if ("jboss".equals(s.getDefaultDomain()))
         {
            result = s;
            break;
         }
      }
      return result;
   }

   private void initializeRemoting() throws Exception
   {
      ObjectName on = new ObjectName("jboss.remoting:service=Connector,transport=socket");
      String s = (String)mbeanServer.invoke(on, "getInvokerLocator", new Object[0], new String[0]);
      locator = new InvokerLocator(s);

      log.debug("LocatorURI: " + getLocatorURI());

      // add the JMS subsystem
      mbeanServer.invoke(on, "addInvocationHandler",
                         new Object[] {"JMS", new JMSServerInvocationHandler()},
                         new String[] {"java.lang.String",
                                       "org.jboss.remoting.ServerInvocationHandler"});

      // TODO what happens if there is a JMS subsystem already registered? Normally, nothing bad,
      // TODO since it delegates to a static dispatcher, but make sure

      // TODO if this is ServerPeer is stopped, the InvocationHandler will be left hanging
   }

   private TransactionManager findTransactionManager() throws Exception
   {
      TransactionManager tm = null;
      try
      {
         tm = (TransactionManager)initialContext.lookup("java:/TransactionManager");
      }
      catch(NameNotFoundException e)
      {}

      if (tm == null)
      {
         tm = TransactionManagerImpl.getInstance();
         log.warn("Cannot find a transaction manager, using an internal implementation!");
      }

      log.debug("TransactionManager: " + tm);
      return tm;
   }





   // Inner classes -------------------------------------------------
}
