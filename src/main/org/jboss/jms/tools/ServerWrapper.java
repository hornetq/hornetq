/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.tools;

import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.util.JNDIUtil;
import org.jboss.aop.AspectXmlLoader;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.transport.Connector;


import javax.naming.InitialContext;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.Binding;
import javax.transaction.TransactionManager;
import javax.management.ObjectName;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.net.URL;
import java.util.Hashtable;

/**
 * A place-holder for the micro-container. Used to bootstrap a server instance, until proper
 * integration with the micro-container. Also useful for in memory testing. Run it with
 * bin/runserver.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerWrapper
{
   // Constants -----------------------------------------------------

   protected static final Logger log = Logger.getLogger(ServerWrapper.class);

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      log.info("Starting the server ...");
      new ServerWrapper().start();
   }

   // Attributes ----------------------------------------------------

   private ServerPeer serverPeer;
   private InvokerLocator locator;
   private Connector connector;
   private String serverPeerID;
   private InitialContext initialContext;
   private Hashtable jndiEnvironment;
   private MBeanServer jbossMBeanServer;
   private TransactionManager transactionManager;

   // Constructors --------------------------------------------------

   /**
    * Uses default jndi properties (jndi.properties file)
    */
   public ServerWrapper() throws Exception
   {
      this(null, null);
   }

   /**
    * @param jndiEnvironment - Contains jndi properties. Null means using default properties
    *        (jndi.properties file)
    * @param transactionManager - transaction manager instance to be used by the server. null if no
    *        transaction manager is available.
    */
   public ServerWrapper(Hashtable jndiEnvironment, TransactionManager transactionManager)
         throws Exception
   {
      this.jndiEnvironment = jndiEnvironment;
      initialContext = new InitialContext(jndiEnvironment);
      this.transactionManager = transactionManager;
      locator = new InvokerLocator("socket://localhost:9890");
      jbossMBeanServer = MBeanServerFactory.createMBeanServer("jboss");
      serverPeerID = "ServerPeer0";
   }

   // Public --------------------------------------------------------

   public void start() throws Exception
   {
      loadAspects();
      setupJNDI();
      initializeRemoting();
      serverPeer = new ServerPeer(serverPeerID, jndiEnvironment);
      serverPeer.start();
      log.info("server started");
   }

   public void stop() throws Exception
   {
      serverPeer.stop();
      serverPeer = null;
      tearDownRemoting();
      tearDownJNDI();
      unloadAspects();
      MBeanServerFactory.releaseMBeanServer(jbossMBeanServer);
      jbossMBeanServer = null;
      log.info("server stopped");
   }

   public void deployTopic(String name) throws Exception
   {
      serverPeer.getDestinationManager().createTopic(name, null);
   }

   public void undeployTopic(String name) throws Exception
   {
      serverPeer.getDestinationManager().destroyTopic(name);
   }

   public void deployQueue(String name) throws Exception
   {
      serverPeer.getDestinationManager().createQueue(name, null);
   }

   public void undeployQueue(String name) throws Exception
   {
      serverPeer.getDestinationManager().destroyQueue(name);
   }

   public ServerPeer getServerPeer()
   {
      return serverPeer;
   }

   public Connector getConnector()
   {
      return connector;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private void loadAspects() throws Exception
   {
      URL url = this.getClass().getClassLoader().getResource("jms-aop.xml");
      AspectXmlLoader.deployXML(url);
   }

   private void unloadAspects() throws Exception
   {
      URL url = this.getClass().getClassLoader().getResource("jms-aop.xml");
      AspectXmlLoader.undeployXML(url);
   }

   private void setupJNDI() throws Exception
   {
      String[] names = {DestinationManager.DEFAULT_QUEUE_CONTEXT,
                        DestinationManager.DEFAULT_TOPIC_CONTEXT};

      for (int i = 0; i < names.length; i++)
      {
         try
         {
            initialContext.lookup(names[i]);
         }
         catch(NameNotFoundException e)
         {
            JNDIUtil.createContext(initialContext, names[i]);
            log.info("Created context /" + names[i]);
         }
      }

      if (transactionManager != null)
      {
         initialContext.bind("java:/TransactionManager", transactionManager);
      }
   }

   private void tearDownJNDI() throws Exception
   {
      Context c = (Context)initialContext.lookup("/topic");
      tearDownRecursively(c);
      c = (Context)initialContext.lookup("/queue");
      tearDownRecursively(c);

      try
      {
         initialContext.unbind("java:/TransactionManager");
      }
      catch(Exception e)
      {
         log.info("Could not unbind transaction manager");
      }

      log.info("unbound messaging");
   }

   private void tearDownRecursively(Context c) throws Exception
   {
      for(NamingEnumeration ne = c.listBindings(""); ne.hasMore(); )
      {
         Binding b = (Binding)ne.next();
         String name = b.getName();
         Object object = b.getObject();
         if (object instanceof Context)
         {
            tearDownRecursively((Context)object);
         }
         c.unbind(name);
         log.info("unbound " + name);
      }
   }

   private void initializeRemoting() throws Exception
   {
      connector = new Connector();
      connector.setInvokerLocator(locator.getLocatorURI());
      connector.start();

      // register the connector as an MBean to the MBeanServer the same way JBoss does
      ObjectName on = new ObjectName("jboss.remoting:service=Connector,transport=socket");
      jbossMBeanServer.registerMBean(new JBossRemotingConnector(connector), on);
   }

   private void tearDownRemoting() throws Exception
   {
      connector.stop();
      connector = null;
   }

   // Inner classes -------------------------------------------------

   public interface JBossRemotingConnectorMBean
   {
      public String getInvokerLocator() throws Exception;
      public ServerInvocationHandler addInvocationHandler(String s, ServerInvocationHandler h)
         throws Exception;
   }

   public class JBossRemotingConnector implements JBossRemotingConnectorMBean
   {
      private Connector connector;

      public JBossRemotingConnector(Connector connector)
      {
         this.connector = connector;
      }

      public String getInvokerLocator() throws Exception
      {
         return connector.getInvokerLocator();
      }

      public ServerInvocationHandler addInvocationHandler(String s, ServerInvocationHandler h)
         throws Exception
      {
         return connector.addInvocationHandler(s, h);
      }
   }
}
