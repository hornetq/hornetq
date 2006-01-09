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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.XAConnectionFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
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
import org.jboss.jms.tx.JMSRecoverable;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.message.PersistentMessageStore;
import org.jboss.messaging.core.persistence.JDBCPersistenceManager;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;
import org.w3c.dom.Element;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;

/**
 * A JMS server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerPeer
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerPeer.class);

   private static final String CONNECTION_FACTORY_JNDI_NAME = "ConnectionFactory";
   
   private static final String XACONNECTION_FACTORY_JNDI_NAME = "XAConnectionFactory";
   
   public static final String RECOVERABLE_CTX_NAME = "jms-recoverables";
   
   private static ObjectName DESTINATION_MANAGER_OBJECT_NAME;
   
   private static ObjectName STATE_MANAGER_OBJECT_NAME;

   static
   {
      try
      {
         DESTINATION_MANAGER_OBJECT_NAME =
         new ObjectName("jboss.messaging:service=DestinationManager");
         STATE_MANAGER_OBJECT_NAME =
            new ObjectName("jboss.messaging:service=StateManager");
      }
      catch(Exception e)
      {
         log.error(e);
      }
   }

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected MBeanServer mbeanServer;

   protected String serverPeerID;
   protected ObjectName connectorName;
   
   //protected Connector connector;
   
   protected InvokerLocator locator;
   protected byte[] clientAOPConfig;
   private Version version;

   protected boolean started;

   protected DestinationManagerImpl destinationManager;
   protected ClientManager clientManager;
   protected SecurityManager securityManager;
   protected TransactionRepository txRepository;
   protected JMSServerInvocationHandler handler;

   protected PersistenceManager pm;
   protected MessageStore ms;
   protected StateManager stateManager;
   protected PooledExecutor threadPool; // manages threads used to deliver messages to consumers

   protected int connFactoryIDSequence;
   protected Map connFactoryDelegates;

   // Constructors --------------------------------------------------


   public ServerPeer(String serverPeerID) throws Exception
   {
      this.serverPeerID = serverPeerID;
      
      this.connFactoryDelegates = new HashMap();

      // the default value to use, unless the JMX attribute is modified
      connectorName = new ObjectName("jboss.remoting:service=Connector,transport=socket");
      
      securityManager = new SecurityManager();

      version = new Version("VERSION");

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
      try
      {
         
         if (started)
         {
            return;
         }
         
         loadClientAOPConfig();
         
         loadServerAOPConfig();
         
         log.info(this + " starting");
         
         mbeanServer = findMBeanServer();
   
         JDBCPersistenceManager jpm = new JDBCPersistenceManager();
         jpm.start();
         pm = jpm;
         
         txRepository = new TransactionRepository(pm);
         txRepository.loadPreparedTransactions();
               
         // TODO: is should be possible to share this with other peers
         ms = new PersistentMessageStore(serverPeerID, pm);
   
         clientManager = new ClientManagerImpl(this);
         
         destinationManager = new DestinationManagerImpl(this);
               
         JDBCStateManager jdbcStateManager = new JDBCStateManager(this);
         jdbcStateManager.start();
         stateManager = jdbcStateManager;
         
         // This pooled executor controls how threads are allocated to deliver messages
         // to consumers.
         // The buffer(queue) of the pool must be unbounded to avoid potential distributed deadlock
         // Since the buffer is unbounded, the minimum pool size has to be the same as the maximum.
         // Otherwise, we will never have more than getMinimumPoolSize threads running.
                     
         threadPool = new PooledExecutor(new LinkedQueue(), 50);
         threadPool.setMinimumPoolSize(50); 
         
         initializeRemoting();
   
         mbeanServer.registerMBean(destinationManager, DESTINATION_MANAGER_OBJECT_NAME);
         
         mbeanServer.registerMBean(stateManager, STATE_MANAGER_OBJECT_NAME);
                                                   
         securityManager.init();
         
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



   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      
      unloadServerAOPConfig();
      
      log.debug(this + " stopping");
      
      tearDownConnectionFactories();
      
      destinationManager.destroyAllDestinations();
      pm = null;      
      txRepository = null;      
      ms = null;
      clientManager = null;      
      destinationManager = null;      
      stateManager = null;
      
      // remove the JMS subsystem
      mbeanServer.invoke(connectorName, "removeInvocationHandler",
                         new Object[] {"JMS"},
                         new String[] {"java.lang.String"});
      
//      connector.removeInvocationHandler("JMS");
//      connector.stop();
//      connector.destroy();
                        
      mbeanServer.unregisterMBean(DESTINATION_MANAGER_OBJECT_NAME);
      mbeanServer.unregisterMBean(STATE_MANAGER_OBJECT_NAME);
      
      removeRecoverable();

      started = false;

      log.info("JMS " + this + " stopped");

   }

   public synchronized void destroy()
   {
      log.debug(this + " destroyed");
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
      securityManager.setSecurityDomain(securityDomain);
   }
   
   public String getSecurityDomain()
   {
      return securityManager.getSecurityDomain();
   }

   public void setDefaultSecurityConfig(Element conf)
      throws Exception
   {
      securityManager.setDefaultSecurityConfig(conf);
   }
   
   public Element getDefaultSecurityConfig()
   {
      return securityManager.getDefaultSecurityConfig();
   }
   
   public void setSecurityConfig(String dest, Element conf)
      throws Exception
   {
      securityManager.setSecurityConfig(dest, conf);
   }
   

   //
   // end of JMX attributes
   //
   
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
   
   public StateManager getStateManager()
   {
      return stateManager;
   }

   public DestinationManagerImpl getDestinationManager()
   {
      return destinationManager;
   }
   
   public SecurityManager getSecurityManager()
   {
      return securityManager;
   }

   public ConnectionFactoryDelegate getConnectionFactoryDelegate(String connectionFactoryID)
   {
      return (ConnectionFactoryDelegate)connFactoryDelegates.get(connectionFactoryID);
   }

   public PooledExecutor getThreadPool()
   {
      return threadPool;
   }

   public MessageStore getMessageStore()
   {
      return ms;
   }

   public PersistenceManager getPersistenceManager()
   {
      return pm;
   }
   
   public byte[] getClientAOPConfig()
   {
      return clientAOPConfig;
   }

   public Version getVersion()
   {
      return version;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer();
      sb.append("ServerPeer (id=");
      sb.append(getServerPeerID());
      sb.append(")");
      return sb.toString();
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

   /**
    * @return - may return null if it doesn't find a "jboss" MBeanServer.
    */
   private MBeanServer findMBeanServer()
   {
      System.setProperty("jmx.invoke.getters", "true");

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
      String s = (String)mbeanServer.invoke(connectorName, "getInvokerLocator",
                                            new Object[0], new String[0]);

      locator = new InvokerLocator(s);
      
      //Note - There seems to be a bug (feature?) in JBoss Remoting which if you specify
      //a socket timeout on the invoker locator URI then it always makes a remote call
      //even when in the same JVM

      log.debug("LocatorURI: " + getLocatorURI());

      // add the JMS subsystem
      
      handler = new JMSServerInvocationHandler();

      mbeanServer.invoke(connectorName, "addInvocationHandler",
                         new Object[] {"JMS", handler},
                         new String[] {"java.lang.String",
                                       "org.jboss.remoting.ServerInvocationHandler"});  
      
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

   // Inner classes -------------------------------------------------

}
