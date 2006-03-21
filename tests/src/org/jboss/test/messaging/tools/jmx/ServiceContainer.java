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
package org.jboss.test.messaging.tools.jmx;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Collections;

import javax.management.Attribute;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.spi.NamingManager;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.jboss.jms.util.JNDIUtil;
import org.jboss.jms.util.XMLUtil;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerLocator;
import org.jboss.resource.adapter.jdbc.local.LocalManagedConnectionFactory;
import org.jboss.resource.adapter.jdbc.remote.WrapperDataSourceService;
import org.jboss.resource.connectionmanager.CachedConnectionManager;
import org.jboss.resource.connectionmanager.CachedConnectionManagerMBean;
import org.jboss.resource.connectionmanager.JBossManagedConnectionPool;
import org.jboss.resource.connectionmanager.TxConnectionManager;
import org.jboss.system.Registry;
import org.jboss.system.ServiceController;
import org.jboss.system.ServiceCreator;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jboss.MBeanConfigurationElement;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactoryBuilder;
import org.jboss.tm.TxManager;


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
   public static ObjectName CLASS_LOADER_OBJECT_NAME;
   public static ObjectName TRANSACTION_MANAGER_OBJECT_NAME;
   public static ObjectName CACHED_CONNECTION_MANAGER_OBJECT_NAME;
   public static ObjectName CONNECTION_MANAGER_OBJECT_NAME;
   public static ObjectName MANAGED_CONNECTION_FACTORY_OBJECT_NAME;
   public static ObjectName MANAGED_CONNECTION_POOL_OBJECT_NAME;
   public static ObjectName WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME;
   public static ObjectName REMOTING_OBJECT_NAME;

   public static String DATA_SOURCE_JNDI_NAME = "java:/DefaultDS";
   public static String TRANSACTION_MANAGER_JNDI_NAME = "java:/TransactionManager";

   static
   {
      try
      {
         SERVICE_CONTROLLER_OBJECT_NAME =
         new ObjectName("jboss.system:service=ServiceController");
         CLASS_LOADER_OBJECT_NAME =
         new ObjectName("jboss.system:service=ClassLoader");
         TRANSACTION_MANAGER_OBJECT_NAME =
         new ObjectName("jboss:service=TransactionManager");
         CACHED_CONNECTION_MANAGER_OBJECT_NAME =
         new ObjectName("jboss.jca:service=CachedConnectionManager");
         CONNECTION_MANAGER_OBJECT_NAME =
         new ObjectName("jboss.jca:name=DefaultDS,service=LocalTxCM");
         MANAGED_CONNECTION_FACTORY_OBJECT_NAME =
         new ObjectName("jboss.jca:name=DefaultDS,service=ManagedConnectionFactory");
         MANAGED_CONNECTION_POOL_OBJECT_NAME =
         new ObjectName("jboss.jca:name=DefaultDS,service=ManagedConnectionPool");
         WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME =
         new ObjectName("jboss.jca:name=DefaultDS,service=DataSourceBinding");
         REMOTING_OBJECT_NAME =
         new ObjectName("jboss.messaging:service=Connector,transport=socket");
      }
      catch(Exception e)
      {
         e.printStackTrace();
      }
   }

   // Attributes ----------------------------------------------------

   private TransactionManager tm;

   private MBeanServer mbeanServer;
   private ServiceCreator serviceCreator; // the 'creator' helps in creating and registering XMBeans
   private InitialContext initialContext;
   private String jndiNamingFactory;
   private Server hsqldbServer;

   private boolean transaction;
   private boolean database;
   private boolean jca;
   private boolean remotingSocket;
   private boolean remotingMultiplex;
   private boolean security;

   private List toUnbindAtExit;
   private String ipAddressOrHostName;

   // Static --------------------------------------------------------

   public static Object type(MBeanInfo mbeanInfo, String attributeName, String valueAsString)
      throws Exception
   {
      MBeanAttributeInfo[] attrs = mbeanInfo.getAttributes();
      MBeanAttributeInfo attr = null;

      for(int i = 0; i < attrs.length; i++)
      {
         if (attrs[i].getName().equals(attributeName))
         {
            attr = attrs[i];
            break;
         }
      }

      if (attr == null)
      {
         throw new Exception("No such attribute: " + attributeName);
      }

      String type = attr.getType();

      if ("int".equals(type) || "java.lang.Integer".equals(type))
      {
         int i = Integer.parseInt(valueAsString);
         return new Integer(i);
      }
      else if ("boolean".equals(type) || "java.lang.Boolean".equals(type))
      {
         boolean b = Boolean.valueOf(valueAsString).booleanValue();
         return new Boolean(b);
      }
      else if ("java.lang.String".equals(type))
      {
         return valueAsString;
      }
      else if ("javax.management.ObjectName".equals(type))
      {
         return new ObjectName(valueAsString);
      }
      else if ("org.w3c.dom.Element".equals(type))
      {
         return XMLUtil.stringToElement(valueAsString);
      }

      throw new Exception("Don't know to handle type " + type);

   }

   // Constructors --------------------------------------------------

   public ServiceContainer(String config) throws Exception
   {
      this(config, null);
   }

   /**
    * @param config - A comma separated list of services to be started. Available services:
    *        transaction, jca, database, remoting.  Example: "transaction, database, remoting".
    *        "all" will start every service available. A dash in front of a service name will
    *        disable that service. Example "all,-database".
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
         configureAddress();

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

         boolean java5 = false;

         try
         {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            cl.loadClass("java.lang.management.ManagementFactory");
            java5 = true;
         }
         catch(ClassNotFoundException e)
         {
            // OK
         }

         if (java5)
         {
            System.setProperty("javax.management.builder.initial",
                               "org.jboss.test.messaging.tools.jmx.MBeanServerBuilder");
         }

         mbeanServer = MBeanServerFactory.createMBeanServer("jboss");

         serviceCreator = new ServiceCreator(mbeanServer);

         startServiceController();

         registerClassLoader();

         if (database)
         {
            startInVMDatabase();
         }
         if (transaction)
         {
            startTransactionManager();
         }
         if (jca)
         {
            startManagedConnectionFactory();
            startCachedConnectionManager();
            startManagedConnectionPool();
            startConnectionManager();
            startWrapperDataSourceService();
         }
         if (remotingSocket)
         {
            startRemoting(false);
         }
         if (remotingMultiplex)
         {
            startRemoting(true);
         }
         if (security)
         {
            startSecurityManager();
         }

         loadJNDIContexts();

         log.debug(this + " started");
      }
      catch(Throwable e)
      {
         log.error("Failed to start ServiceContainer", e);
         throw new Exception("Failed to start ServiceContainer", e);
      }
   }

   public void stop() throws Exception
   {
      unloadJNDIContexts();

      stopService(REMOTING_OBJECT_NAME);
      stopService(WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME);
      stopService(CONNECTION_MANAGER_OBJECT_NAME);
      stopService(MANAGED_CONNECTION_POOL_OBJECT_NAME);
      stopService(CACHED_CONNECTION_MANAGER_OBJECT_NAME);
      stopService(TRANSACTION_MANAGER_OBJECT_NAME);
      stopService(MANAGED_CONNECTION_FACTORY_OBJECT_NAME);
      if (database)
      {
         stopInVMDatabase();
      }

      unregisterClassLoader();
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
            
      log.debug(this + " stopped");
   }

   public DataSource getDataSource()
   {
      DataSource ds = null;
      try
      {
         InitialContext ic = new InitialContext();
         ds = (DataSource)ic.lookup(DATA_SOURCE_JNDI_NAME);
         ic.close();
      }
      catch(Exception e)
      {
         log.error("Failed to look up DataSource", e);
      }
      return ds;
   }

   public TransactionManager getTransactionManager()
   {
      TransactionManager tm = null;
      try
      {
         InitialContext ic = new InitialContext();
         tm = (TransactionManager)ic.lookup(TRANSACTION_MANAGER_JNDI_NAME);
         ic.close();
      }
      catch(Exception e)
      {
         log.error("Failed to look up transaction manager", e);
      }
      return tm;
   }

   public UserTransaction getUserTransaction()
   {
      return null;
   }

   public Object getService(ObjectName on) throws Exception
   {
      return mbeanServer.invoke(on, "getInstance", new Object[0], new String[0]);
   }

   /**
    * @return Set<ObjectName>
    */
   public Set query(ObjectName pattern)
   {
      if (pattern == null)
      {
         return Collections.EMPTY_SET;
      }
      return mbeanServer.queryNames(pattern, null);
   }

   /**
    * Note that this method makes no assumption on whether the service was created or started, nor
    * does it attempt to create/start the service.
    *
    * @param service - a Standard/DynamicMBean instance.
    */
   public void registerService(Object service, ObjectName on) throws Exception
   {
      mbeanServer.registerMBean(service, on);
      log.debug(service + " registered as " + on);
   }

   /**
    * Creates and registers a service based on the MBean service descriptor element. Supports
    * XMBeans. The implementing class and the ObjectName are inferred from the mbean element. If
    * there are configuration attributed specified in the deployment descriptor, they are applied
    * to the service instance.
    */
   public ObjectName registerAndConfigureService(MBeanConfigurationElement mbeanConfig)
      throws Exception
   {
      ObjectName on = mbeanConfig.getObjectName();
      serviceCreator.install(on, CLASS_LOADER_OBJECT_NAME, mbeanConfig.getDelegate());

      // inject dependencies
      for(Iterator i = mbeanConfig.dependencyOptionalAttributeNames().iterator(); i.hasNext(); )
      {
         String name = (String)i.next();
         String value = mbeanConfig.getDependencyOptionalAttributeValue(name);
         setAttribute(on, name, value);
      }

      // apply attributes
      for(Iterator i = mbeanConfig.attributeNames().iterator(); i.hasNext();)
      {
         String name = (String)i.next();
         String value = mbeanConfig.getAttributeValue(name);
         setAttribute(on, name, value);
      }

      log.debug(mbeanConfig + " registered and configured");
      return on;
   }

   /**
    * Note that this method makes no assumption on whether the service was stopped or destroyed, nor
    * does it attempt to stop/destroy the service.
    */
   public void unregisterService(ObjectName on) throws Exception
   {
      mbeanServer.unregisterMBean(on);
      log.debug(on + " unregistered");
   }

   public Object invoke(ObjectName on, String operationName, Object[] params, String[] signature)
      throws Exception
   {
      try
      {
         return mbeanServer.invoke(on, operationName, params, signature);
      }
      catch(MBeanException e)
      {
         // unwrap the exception thrown by the service
         throw (Exception)e.getCause();
      }
   }

   /**
    * Set the attribute value, performing String -> Object conversion as appropriate.
    */
   public void setAttribute(ObjectName on, String name, String valueAsString) throws Exception
   {
      MBeanInfo mbeanInfo = mbeanServer.getMBeanInfo(on);
      Object value = type(mbeanInfo, name, valueAsString);
      mbeanServer.setAttribute(on, new Attribute(name, value));
   }

   public Object getAttribute(ObjectName on, String name) throws Exception
   {
      return mbeanServer.getAttribute(on, name);
   }

   public String toString()
   {
      return "ServiceContainer[" + Integer.toHexString(hashCode()) + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void configureAddress() throws Exception
   {

      String s = System.getProperty("test.bind.address");
      if (s == null)
      {
         ipAddressOrHostName = "localhost";         
      }
      else
      {
         ipAddressOrHostName = s;
      }
      
      log.debug("all server sockets will be open on address: " + ipAddressOrHostName);
   }

   private void loadJNDIContexts() throws Exception
   {
      String[] names = {ServerManagement.DEFAULT_QUEUE_CONTEXT,
                        ServerManagement.DEFAULT_TOPIC_CONTEXT};

      for (int i = 0; i < names.length; i++)
      {
         try
         {
            initialContext.lookup(names[i]);
         }
         catch(NameNotFoundException e)
         {
            JNDIUtil.createContext(initialContext, names[i]);
            log.debug("created context /" + names[i]);
         }
      }
   }

   private void unloadJNDIContexts() throws Exception
   {
      // ServerPeer should do that upon its shutdown, this is redundant

      String[] context = { "/topic", "/queue" };
      for(int i = 0; i < context.length; i++)
      {
         try
         {
            Context c = (Context)initialContext.lookup(context[i]);
            JNDIUtil.tearDownRecursively(c);
         }
         catch(NameNotFoundException e)
         {
            // OK
            log.debug("no context " + context[i] + " to unload, cleanup already performed");
         }
      }
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


   /**
    * Register a class loader used to instantiate other services.
    */
   private void registerClassLoader() throws Exception
   {
      ClassLoader cl = getClass().getClassLoader();
      mbeanServer.registerMBean(new ClassLoaderJMXWrapper(cl), CLASS_LOADER_OBJECT_NAME);
   }

   private void unregisterClassLoader() throws Exception
   {
      mbeanServer.unregisterMBean(CLASS_LOADER_OBJECT_NAME);
   }


   private void startInVMDatabase() throws Exception
   {
      HsqlProperties props = new HsqlProperties();
      props.setProperty("server.database.0", "mem:test");
      props.setProperty("server.dbname.0", "memtest");
      props.setProperty("server.trace", "false");
      props.setProperty("server.silent", "true");
      props.setProperty("server.no_system_exit", "true");
      props.setProperty("server.port", 27862);
      props.setProperty("server.address", ipAddressOrHostName);

      hsqldbServer = new Server();
      hsqldbServer.setLogWriter(null);
      hsqldbServer.setProperties(props);
      hsqldbServer.start();


      log.debug("started the database");
   }

   private void stopInVMDatabase() throws Exception
   {
      Class.forName("org.hsqldb.jdbcDriver" );
      Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:test", "sa", "");
      Statement stat = conn.createStatement();
      stat.executeUpdate("SHUTDOWN");
      conn.close();

      // faster stop
//      hsqldbServer.stop();
   }

   private void startTransactionManager() throws Exception
   {
      if (tm == null)
      {
         // the default JBoss TransactionManager
         tm = TxManager.getInstance();
      }

      TransactionManagerJMXWrapper mbean = new TransactionManagerJMXWrapper(tm);
      mbeanServer.registerMBean(mbean, TRANSACTION_MANAGER_OBJECT_NAME);
      mbeanServer.invoke(TRANSACTION_MANAGER_OBJECT_NAME, "start", new Object[0], new String[0]);
      log.debug("started " + TRANSACTION_MANAGER_OBJECT_NAME);

      initialContext.bind(TRANSACTION_MANAGER_JNDI_NAME, tm);
      toUnbindAtExit.add(TRANSACTION_MANAGER_JNDI_NAME);

      log.debug("bound " + TRANSACTION_MANAGER_JNDI_NAME);

      // to get this to work I need to bind DTMTransactionFactory in JNDI
//      ClientUserTransaction singleton = ClientUserTransaction.getSingleton();
//      initialContext.bind("UserTransaction", singleton);
//      toUnbindAtExit.add("UserTransaction");
//      log.info("bound /UserTransaction");

   }

   private void startCachedConnectionManager() throws Exception
   {
      CachedConnectionManager ccm = new CachedConnectionManager();

      // dependencies
      ccm.setTransactionManagerServiceName(TRANSACTION_MANAGER_OBJECT_NAME);

      mbeanServer.registerMBean(ccm, CACHED_CONNECTION_MANAGER_OBJECT_NAME);
      mbeanServer.invoke(CACHED_CONNECTION_MANAGER_OBJECT_NAME, "start", new Object[0], new String[0]);
      log.debug("started " + CACHED_CONNECTION_MANAGER_OBJECT_NAME);

   }

   private void startManagedConnectionFactory() throws Exception
   {
      LocalManagedConnectionFactory mcf = new LocalManagedConnectionFactory();
      mcf.setConnectionURL("jdbc:hsqldb:mem:test");
      mcf.setDriverClass("org.hsqldb.jdbcDriver");
      mcf.setUserName("sa");

      ManagedConnectionFactoryJMXWrapper mbean = new ManagedConnectionFactoryJMXWrapper(mcf);
      mbeanServer.registerMBean(mbean, MANAGED_CONNECTION_FACTORY_OBJECT_NAME);
      mbeanServer.invoke(MANAGED_CONNECTION_FACTORY_OBJECT_NAME, "start", new Object[0], new String[0]);
      log.debug("started " + MANAGED_CONNECTION_FACTORY_OBJECT_NAME);
   }

   private void startManagedConnectionPool() throws Exception
   {
      JBossManagedConnectionPool mcp = new JBossManagedConnectionPool();
      mcp.setCriteria("ByContainer");

      // dependencies
      mcp.setManagedConnectionFactoryName(MANAGED_CONNECTION_FACTORY_OBJECT_NAME);

      mbeanServer.registerMBean(mcp, MANAGED_CONNECTION_POOL_OBJECT_NAME);
      mbeanServer.invoke(MANAGED_CONNECTION_POOL_OBJECT_NAME, "start", new Object[0], new String[0]);
      log.debug("started " + MANAGED_CONNECTION_POOL_OBJECT_NAME);
   }

   private void startConnectionManager() throws Exception
   {
      TxConnectionManager cm = new TxConnectionManager();
      cm.preRegister(mbeanServer, CONNECTION_MANAGER_OBJECT_NAME);
      cm.setTrackConnectionByTx(true);
      cm.setLocalTransactions(true);

      // dependencies
      cm.setTransactionManagerService(TRANSACTION_MANAGER_OBJECT_NAME);
      cm.setCachedConnectionManager(CachedConnectionManagerMBean.OBJECT_NAME);
      cm.setManagedConnectionPool(MANAGED_CONNECTION_POOL_OBJECT_NAME);


      mbeanServer.registerMBean(cm, CONNECTION_MANAGER_OBJECT_NAME);
      mbeanServer.invoke(CONNECTION_MANAGER_OBJECT_NAME, "start", new Object[0], new String[0]);
      log.debug("started " + CONNECTION_MANAGER_OBJECT_NAME);
   }

   private void startWrapperDataSourceService() throws Exception
   {
      WrapperDataSourceService wdss = new WrapperDataSourceService();
      wdss.setJndiName(DATA_SOURCE_JNDI_NAME);

      // dependencies
      wdss.setConnectionManager(CONNECTION_MANAGER_OBJECT_NAME);
      ObjectName irrelevant = new ObjectName(":name=irrelevant");
      wdss.setJMXInvokerName(irrelevant);
      Registry.bind(irrelevant, new NoopInvoker());

      mbeanServer.registerMBean(wdss, WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME);
      mbeanServer.invoke(WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME, "start", new Object[0], new String[0]);

      log.debug("started " + WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME);
   }

   private void startRemoting(boolean multiplex) throws Exception
   {
      RemotingJMXWrapper mbean;
      
      String params = "/?marshaller=org.jboss.jms.server.remoting.JMSWireFormat&" +
                      "unmarshaller=org.jboss.jms.server.remoting.JMSWireFormat&" +
                      "serializationtype=jboss&" +
                      "dataType=jms&" +
                      "socketTimeout=0&" +
                      "socket.check_connection=false";
      
      String locatorURI;
      if (multiplex)
      {
         locatorURI = "multiplex://" + ipAddressOrHostName + ":9111" + params;                  
      }
      else
      {
         locatorURI = "socket://" + ipAddressOrHostName + ":9111" + params;
      }
      
      log.debug("Using the following locator uri:" + locatorURI);
      
      InvokerLocator locator = new InvokerLocator(locatorURI);
      
      log.debug("Started remoting connector on uri:" + locator.getLocatorURI());
      
      mbean = new RemotingJMXWrapper(locator);
      mbeanServer.registerMBean(mbean, REMOTING_OBJECT_NAME);
      mbeanServer.invoke(REMOTING_OBJECT_NAME, "start", new Object[0], new String[0]);
      log.debug("started " + REMOTING_OBJECT_NAME);
   }
   
   
   private void startSecurityManager() throws Exception
   {
      MockJBossSecurityManager sm = new MockJBossSecurityManager();
      this.initialContext.bind(MockJBossSecurityManager.TEST_SECURITY_DOMAIN, sm);
      
      log.debug("started JBoss Mock Security Manager");
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
            transaction = true;
            database = true;
            jca = true;
            remotingSocket = true;
            security = true;
         }
         else if ("transaction".equals(tok))
         {
            transaction = true;
            if (minus)
            {
               transaction = false;
            }
         }
         else if ("database".equals(tok))
         {
            database = true;
            if (minus)
            {
               database = false;
            }
         }
         else if ("jca".equals(tok))
         {
            jca = true;
            if (minus)
            {
               jca = false;
            }
         }
         else if ("remoting".equals(tok))
         {
            remotingSocket = true;
            if (minus)
            {
               remotingSocket = false;
            }

         }
         else if ("remoting-multiplex".equals(tok))
         {
            remotingMultiplex = true;
            if (minus)
            {
               remotingMultiplex = false;
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
         else if ("none".equals(tok))
         {
            transaction = false;
            database = false;
            jca = false;
            remotingSocket = false;
            security = false;
         }
         else
         {
            throw new IllegalArgumentException("Unknown service: " + tok);
         }
      }
   }

   // Inner classes -------------------------------------------------
}
