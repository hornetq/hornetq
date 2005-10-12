/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.tools.jmx;

import org.jboss.resource.adapter.jdbc.local.LocalManagedConnectionFactory;
import org.jboss.resource.adapter.jdbc.remote.WrapperDataSourceService;
import org.jboss.resource.connectionmanager.TxConnectionManager;
import org.jboss.resource.connectionmanager.CachedConnectionManagerMBean;
import org.jboss.resource.connectionmanager.CachedConnectionManager;
import org.jboss.resource.connectionmanager.JBossManagedConnectionPool;
import org.jboss.system.ServiceController;
import org.jboss.system.Registry;
import org.jboss.tm.TxManager;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;
import org.jboss.jms.util.JNDIUtil;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactoryBuilder;
import org.jboss.remoting.InvokerLocator;
import org.jboss.aop.AspectXmlLoader;
import org.jboss.jms.server.DestinationManagerImpl;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import javax.transaction.TransactionManager;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.Context;
import javax.naming.spi.NamingManager;

import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;

import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.net.URL;

/**
 * An MBeanServer and a configurable set of services (TransactionManager, Remoting, etc) available
 * for testing.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServiceContainer
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServiceContainer.class);

   // Static --------------------------------------------------------

   public static ObjectName SERVICE_CONTROLLER_OBJECT_NAME;
   public static ObjectName REMOTING_OBJECT_NAME;

   static
   {
      try
      {
         SERVICE_CONTROLLER_OBJECT_NAME =
         new ObjectName("jboss.system:service=ServiceController");
         REMOTING_OBJECT_NAME =
         new ObjectName("jboss.remoting:service=Connector,transport=socket");
      }
      catch(Exception e)
      {
         e.printStackTrace();
      }
   }

   // Attributes ----------------------------------------------------

   private TransactionManager tm;

   private MBeanServer mbeanServer;
   private InitialContext initialContext;
   private String jndiNamingFactory;
   private Server hsqldbServer;

   private boolean database;
   private boolean remoting;
   private boolean aop;
   private boolean security;

   private List toUnbindAtExit;

   // Constructors --------------------------------------------------

   public ServiceContainer(String config) throws Exception
   {
      this(config, null);
   }

   /**
    * @param config - A comma separated list of services to be started. Available services:
    *        transaction, jca, database, remoting, aop.  Example: "transaction, database, remoting".
    *        "all" will start every service available. A dash in front of a service name will
    *        disable that service. Example "all,-aop".
    * @param tm - specifies a specific TransactionManager instance to bind into the mbeanServer.
    *        If null, the default JBoss TransactionManager implementation will be used.
    */
   public ServiceContainer(String config, TransactionManager tm) throws Exception
   {
      this.tm = tm;
      parseConfig(config);
      toUnbindAtExit = new ArrayList();
   }

   // Public --------------------------------------------------------

   public void start() throws Exception
   {

      try
      {
         toUnbindAtExit.clear();

         jndiNamingFactory = System.getProperty("java.naming.factory.initial");

         //TODO: need to think more about this; if I don't do it, though, bind() fails because it tries to use "java.naming.provider.url"
         try
         {
            NamingManager.setInitialContextFactoryBuilder(new InVMInitialContextFactoryBuilder());
         }
         catch(IllegalStateException e)
         {
            // OK
         }

         Hashtable t = InVMInitialContextFactory.getJNDIEnvironment();
         System.setProperty("java.naming.factory.initial",
                            (String)t.get("java.naming.factory.initial"));

         initialContext = new InitialContext();

         mbeanServer = MBeanServerFactory.createMBeanServer("jboss");

         startServiceController();

         if (database)
         {
            startInVMDatabase();
         }
         if (remoting)
         {
            startRemoting();
         }

         if (aop)
         {
            loadAspects();
         }

         if (security)
         {
            startSecurityManager();
         }

         loadJNDIContexts();

         log.debug("ServiceContainer started");
      }
      catch(Throwable e)
      {
         log.error("Failed to start ServiceContainer", e);
         throw new Exception("Failed to start ServiceContainer");
      }
   }

   public void stop() throws Exception
   {
      unloadJNDIContexts();

      if (aop)
      {
         unloadAspects();
      }

      stopService(REMOTING_OBJECT_NAME);
      if (database)
      {
         stopInVMDatabase();
      }
      stopServiceController();
      MBeanServerFactory.releaseMBeanServer(mbeanServer);
      
      if (security)
      {
         initialContext.unbind(MockJBossSecurityManager.TEST_SECURITY_DOMAIN);
      }
      
      initialContext.close();

      cleanJNDI();

      if (jndiNamingFactory != null)
      {
         System.setProperty("java.naming.factory.initial", jndiNamingFactory);
      }
      log.debug("ServiceContainer stopped");
   }

   public DataSource getDataSource()
   {
      return null;
   }

   public UserTransaction getUserTransaction()
   {
      return null;
   }

   public Object getService(ObjectName on) throws Exception
   {
      return mbeanServer.invoke(on, "getInstance", new Object[0], new String[0]);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void loadJNDIContexts() throws Exception
   {
      String[] names = {DestinationManagerImpl.DEFAULT_QUEUE_CONTEXT,
                        DestinationManagerImpl.DEFAULT_TOPIC_CONTEXT};

      for (int i = 0; i < names.length; i++)
      {
         try
         {
            initialContext.lookup(names[i]);
         }
         catch(NameNotFoundException e)
         {
            JNDIUtil.createContext(initialContext, names[i]);
            log.debug("Created context /" + names[i]);
         }
      }
   }

   private void unloadJNDIContexts() throws Exception
   {
      Context c = (Context)initialContext.lookup("/topic");
      JNDIUtil.tearDownRecursively(c);
      c = (Context)initialContext.lookup("/queue");
      JNDIUtil.tearDownRecursively(c);
   }

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

   private void startServiceController() throws Exception
   {
      // I don't really need it, because I enforce dependencies by hand, but this will keep some
      // services happy.
      ServiceController sc = new ServiceController();
      mbeanServer.registerMBean(sc, SERVICE_CONTROLLER_OBJECT_NAME);
   }

   private void stopServiceController() throws Exception
   {
      mbeanServer.unregisterMBean(SERVICE_CONTROLLER_OBJECT_NAME);
   }


   private void startInVMDatabase() throws Exception
   {
      HsqlProperties props = new HsqlProperties();
      props.setProperty("server.database.0", "mem:messaging");
      props.setProperty("server.dbname.0", "memmessaging");
      props.setProperty("server.trace", "false");
      props.setProperty("server.silent", "true");
      props.setProperty("server.no_system_exit", "true");

      hsqldbServer = new Server();
      hsqldbServer.setLogWriter(null);
      hsqldbServer.setProperties(props);
      hsqldbServer.start();

      //Temporary hack

      log.debug("started the database");
   }

   private void stopInVMDatabase() throws Exception
   {
      Class.forName("org.hsqldb.jdbcDriver" );
      Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:messaging", "sa", "");
      Statement stat = conn.createStatement();
      stat.executeUpdate("SHUTDOWN");
      conn.close();

      // faster stop
//      hsqldbServer.stop();
   }


   private void startRemoting() throws Exception
   {
      RemotingJMXWrapper mbean =
            new RemotingJMXWrapper(new InvokerLocator("socket://localhost:9890"));
      mbeanServer.registerMBean(mbean, REMOTING_OBJECT_NAME);
      mbeanServer.invoke(REMOTING_OBJECT_NAME, "start", new Object[0], new String[0]);
      log.debug("started " + REMOTING_OBJECT_NAME);
   }
   
   private void startSecurityManager() throws Exception
   {
      MockJBossSecurityManager sm = new MockJBossSecurityManager();
      this.initialContext.bind(MockJBossSecurityManager.TEST_SECURITY_DOMAIN, sm);
      
      log.debug("Started JBoss Mock Security Manager");
   }

   private void stopService(ObjectName target) throws Exception
   {
      if (mbeanServer.isRegistered(target))
      {
         mbeanServer.invoke(target, "stop", new Object[0], new String[0]);
         mbeanServer.unregisterMBean(target);
         log.debug("stopped " + target);
      }
   }

   private void cleanJNDI() throws Exception
   {
      InitialContext ic = new InitialContext();

      for(Iterator i = toUnbindAtExit.iterator(); i.hasNext(); )
      {
         String name = (String)i.next();
         ic.unbind(name);
      }
      ic.close();
   }

   private void parseConfig(String config)
   {
      config = config.toLowerCase();
      for (StringTokenizer st = new StringTokenizer(config, ", "); st.hasMoreTokens(); )
      {
         String tok = st.nextToken();
         boolean minus = false;

         if (tok.startsWith("-"))
         {
            tok = tok.substring(1);
            minus = true;
         }

         if ("all".equals(tok))
         {            
            database = true;
            remoting = true;
            aop = true;
            security = true;
         }         
         else if ("database".equals(tok))
         {
            database = true;
            if (minus)
            {
               database = false;
            }
         }         
         else if ("remoting".equals(tok))
         {
            remoting = true;
            if (minus)
            {
               remoting = false;
            }

         }
         else if ("aop".equals(tok))
         {
            aop = true;
            if (minus)
            {
               aop = false;
            }
         }
         else if ("security".equals(tok))
         {
            security = true;
            if (minus)
            {
               security = false;
            }
         }
         else
         {
            throw new IllegalArgumentException("Unknown service: " + tok);
         }
      }
   }

   // Inner classes -------------------------------------------------
}
