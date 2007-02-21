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


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import javax.management.Attribute;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.NotificationListener;
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
import org.jboss.jms.jndi.JMSProviderAdapter;
import org.jboss.jms.jndi.JNDIProviderAdapter;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.jms.util.JNDIUtil;
import org.jboss.jms.util.XMLUtil;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.transport.PortUtil;
import org.jboss.resource.adapter.jdbc.local.LocalManagedConnectionFactory;
import org.jboss.resource.adapter.jdbc.remote.WrapperDataSourceService;
import org.jboss.resource.adapter.jms.JmsManagedConnectionFactory;
import org.jboss.resource.connectionmanager.CachedConnectionManager;
import org.jboss.resource.connectionmanager.CachedConnectionManagerMBean;
import org.jboss.resource.connectionmanager.ConnectionFactoryBindingService;
import org.jboss.resource.connectionmanager.JBossManagedConnectionPool;
import org.jboss.resource.connectionmanager.TxConnectionManager;
import org.jboss.system.Registry;
import org.jboss.system.ServiceController;
import org.jboss.system.ServiceCreator;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jboss.MBeanConfigurationElement;
import org.jboss.test.messaging.tools.jboss.ServiceDeploymentDescriptor;
import org.jboss.test.messaging.tools.jndi.Constants;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactory;
import org.jboss.test.messaging.tools.jndi.InVMInitialContextFactoryBuilder;
import org.jboss.tm.TransactionManagerService;
import org.jboss.tm.TxManager;
import org.jboss.tm.usertx.client.ServerVMClientUserTransaction;

import com.arjuna.ats.arjuna.recovery.RecoveryManager;


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
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServiceContainer.class);

   private static final String CONFIGURATION_FILE_NAME = "container.xml";

   public static final String DO_NOT_USE_MESSAGING_MARSHALLERS = "DO_NOT_USE_MESSAGING_MARSHALLERS";

   // Static ---------------------------------------------------------------------------------------

   public static ObjectName SERVICE_CONTROLLER_OBJECT_NAME;
   public static ObjectName CLASS_LOADER_OBJECT_NAME;
   public static ObjectName TRANSACTION_MANAGER_OBJECT_NAME;
   public static ObjectName CACHED_CONNECTION_MANAGER_OBJECT_NAME;

   public static ObjectName DEFAULTDS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME;
   public static ObjectName DEFAULTDS_MANAGED_CONNECTION_POOL_OBJECT_NAME;
   public static ObjectName DEFAULTDS_CONNECTION_MANAGER_OBJECT_NAME;
   public static ObjectName DEFAULTDS_WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME;

   public static ObjectName JMS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME;
   public static ObjectName JMS_MANAGED_CONNECTION_POOL_OBJECT_NAME;
   public static ObjectName JMS_CONNECTION_MANAGER_OBJECT_NAME;
   public static ObjectName JMS_CONNECTION_FACTORY_BINDING_SERVICE_OBJECT_NAME;

   public static ObjectName REMOTING_OBJECT_NAME;

   // Used only on testcases where Socket and HTTP are deployed at the same time
   public static ObjectName HTTP_REMOTING_OBJECT_NAME;

   public static String DATA_SOURCE_JNDI_NAME = "java:/DefaultDS";
   public static String TRANSACTION_MANAGER_JNDI_NAME = "java:/TransactionManager";
   public static String USER_TRANSACTION_JNDI_NAME = "UserTransaction";
   public static String JCA_JMS_CONNECTION_FACTORY_JNDI_NAME = "java:/JCAConnectionFactory";

   public static long HTTP_CONNECTOR_CALLBACK_POLL_PERIOD = 102;

   // List<ObjectName>
   private List connFactoryObjectNames = new ArrayList();


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

         DEFAULTDS_CONNECTION_MANAGER_OBJECT_NAME =
         new ObjectName("jboss.jca:name=DefaultDS,service=LocalTxCM");
         DEFAULTDS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME =
         new ObjectName("jboss.jca:name=DefaultDS,service=ManagedConnectionFactory");
         DEFAULTDS_MANAGED_CONNECTION_POOL_OBJECT_NAME =
         new ObjectName("jboss.jca:name=DefaultDS,service=ManagedConnectionPool");
         DEFAULTDS_WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME =
         new ObjectName("jboss.jca:name=DefaultDS,service=DataSourceBinding");

         JMS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME =
         new ObjectName("jboss.jca:service=ManagedConnectionFactory,name=JCAConnectionFactory");
         JMS_MANAGED_CONNECTION_POOL_OBJECT_NAME =
         new ObjectName("jboss.jca:service=ManagedConnectionPool,name=JCAConnectionFactory");
         JMS_CONNECTION_MANAGER_OBJECT_NAME =
         new ObjectName("jboss.jca:service=TxCM,name=JCAConnectionFactory");
         JMS_CONNECTION_FACTORY_BINDING_SERVICE_OBJECT_NAME =
         new ObjectName("jboss.jca:service=ConnectionFactoryBinding,name=JCAConnectionFactory");

         REMOTING_OBJECT_NAME =
         new ObjectName("jboss.messaging:service=Connector,transport=bisocket");

         HTTP_REMOTING_OBJECT_NAME =
         new ObjectName("jboss.messaging:service=Connector,transport=http");
      }
      catch(Exception e)
      {
         e.printStackTrace();
      }
   }

   public static String getCurrentAddress() throws Exception
   {
      String currentAddress = System.getProperty("test.bind.address");

      if (currentAddress == null)
      {
         currentAddress = "localhost";
      }
      return currentAddress;
   }

   // Attributes -----------------------------------------------------------------------------------

   private ServiceContainerConfiguration config;

   private TransactionManager tm;

   private MBeanServer mbeanServer;
   private ServiceCreator serviceCreator; // the 'creator' helps in creating and registering XMBeans
   private InitialContext initialContext;
   private String jndiNamingFactory;
   private Server hsqldbServer;
   private RecoveryManager recoveryManager;

   private boolean transaction;
   private boolean jbossjta; //To use the ex-Arjuna tx mgr
   private boolean database;
   private boolean jca;
   private boolean remoting;
   private boolean security;
   private boolean httpConnectionFactory;
   private boolean multiplexer; // the JGroups channels multiplexer

   private List toUnbindAtExit;
   private String ipAddressOrHostName;

   // There may be many service containers on the same machine, so we need to distinguish them
   // so we don't start up multiple servers with services running on the same port
   private int serverIndex;

   // Static ---------------------------------------------------------------------------------------

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
      else if ("long".equals(type) || "java.lang.Long".equals(type))
      {
         long l = Long.parseLong(valueAsString);
         return new Long(l);
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
      else if (type.startsWith("org.jboss."))
      {
         Class interfazza = ServiceContainer.class.getClassLoader().loadClass(type);
         Class implementation = ServiceContainer.class.getClassLoader().loadClass(valueAsString);
         return implementation.newInstance();
      }

      throw new Exception("Don't know to handle type " + type);

   }

   // Constructors ---------------------------------------------------------------------------------

   public ServiceContainer(String servicesToStart) throws Exception
   {
      this(servicesToStart, null);
   }

   public ServiceContainer(String sevicesToStart, int serverIndex) throws Exception
   {
      this(sevicesToStart, null, serverIndex);
   }

   /**
    * @param sevicesToStart - A comma separated list of services to be started. Available services:
    *        transaction, jca, database, remoting.  Example: "transaction, database, remoting".
    *        "all" will start every service available. A dash in front of a service name will
    *        disable that service. Example "all,-database".
    * @param tm - specifies a specific TransactionManager instance to bind into the mbeanServer.
    *        If null, the default JBoss TransactionManager implementation will be used.
    */
   public ServiceContainer(String sevicesToStart, TransactionManager tm) throws Exception
   {
      this.tm = tm;
      parseConfig(sevicesToStart);
      toUnbindAtExit = new ArrayList();
      this.serverIndex = 0;
   }

   public ServiceContainer(String sevicesToStart, TransactionManager tm, int serverIndex)
      throws Exception
   {
      this.tm = tm;
      parseConfig(sevicesToStart);
      toUnbindAtExit = new ArrayList();
      this.serverIndex = serverIndex;
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * By default, starting the container DELETES ALL DATA previously existing in the database.
    */
   public void start() throws Exception
   {
      start(true);
   }

   public void start(boolean cleanDatabase) throws Exception
   {
      start(cleanDatabase, null);
   }

   public void start(boolean cleanDatabase, ServiceAttributeOverrides attrOverrides)
      throws Exception
   {
      try
      {
         readConfigurationFile();

         ipAddressOrHostName = getCurrentAddress();
         log.debug("all server sockets will be open on address " + ipAddressOrHostName);

         toUnbindAtExit.clear();

         jndiNamingFactory = System.getProperty("java.naming.factory.initial");

         //TODO: need to think more about this; if I don't do it, though, bind() fails because it tries to use "java.naming.provider.url"
         try
         {
            NamingManager.
               setInitialContextFactoryBuilder(new InVMInitialContextFactoryBuilder());
         }
         catch(IllegalStateException e)
         {
            // OK
         }

         Hashtable t = InVMInitialContextFactory.getJNDIEnvironment(serverIndex);
         System.setProperty("java.naming.factory.initial",
                            (String)t.get("java.naming.factory.initial"));
         System.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME,
                            Integer.toString(serverIndex));

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

         if (jbossjta)
         {
            deleteObjectStore();
         }

         if (transaction || jbossjta)
         {
            startTransactionManager();
         }

         if (database)
         {
            startInVMDatabase();
         }

         if (jca)
         {
            startCachedConnectionManager(CACHED_CONNECTION_MANAGER_OBJECT_NAME);

            // DefaultDS specific
            startManagedConnectionFactory(DEFAULTDS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME);
            startManagedConnectionPool(DEFAULTDS_MANAGED_CONNECTION_POOL_OBJECT_NAME,
                                       DEFAULTDS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME,
                                       "ByContainer");
            startConnectionManager(DEFAULTDS_CONNECTION_MANAGER_OBJECT_NAME, true, true,
                                   TRANSACTION_MANAGER_OBJECT_NAME,
                                   CachedConnectionManagerMBean.OBJECT_NAME,
                                   DEFAULTDS_MANAGED_CONNECTION_POOL_OBJECT_NAME);
            startWrapperDataSourceService();
         }

         if (database && (transaction || jbossjta) && jca && cleanDatabase)
         {
            // We make sure the database is clean (only if we have all dependencies the database,
            // othewise we'll get an access error)
            dropAllTables();
         }

         if (remoting)
         {
            startRemoting(attrOverrides, config.getRemotingTransport(), REMOTING_OBJECT_NAME);
         }

         if (security)
         {
            startSecurityManager();
         }

         if (multiplexer)
         {
            startMultiplexer();
         }

         loadJNDIContexts();

         log.debug("loaded JNDI context");


         String transport = config.getRemotingTransport();

         log.info("Remoting type: .............. " + (remoting ? transport : "DISABLED"));
         log.info("Serialization type: ......... " + config.getSerializationType());
         log.info("Database: ................... " + config.getDatabaseType());
         log.info("Clustering mode: ............ " +
            (this.isClustered() ? "CLUSTERED" : "NON-CLUSTERED"));

         log.debug(this + " started");
      }
      catch(Throwable e)
      {
         log.error("Failed to start ServiceContainer", e);
         throw new Exception("Failed to start ServiceContainer", e);
      }
   }

   public void startConnectionFactories(ServiceAttributeOverrides attrOverrides) throws Exception
   {
      deployConnectionFactories("server/default/deploy/connection-factories-service.xml", attrOverrides);

      log.info("HTTP ConnectionFactory " + httpConnectionFactory);
      if (httpConnectionFactory)
      {
         log.info("Installing HTTP connection factory");
         ServiceAttributeOverrides httpOverride = new ServiceAttributeOverrides();
         startRemoting(httpOverride, "http", HTTP_REMOTING_OBJECT_NAME);
         deployConnectionFactories("server/default/deploy/connection-factory-http.xml", attrOverrides);
      }

      // bind the default JMS provider
      bindDefaultJMSProvider();
      // bind the JCA ConnectionFactory
      bindJCAJMSConnectionFactory();
   }

   public void stopConnectionFactories() throws Exception
   {
      for(Iterator i = connFactoryObjectNames.iterator(); i.hasNext(); )
      {
         try
         {
            ObjectName on = (ObjectName)i.next();
            invoke(on, "stop", new Object[0], new String[0]);
            invoke(on, "destroy", new Object[0], new String[0]);
            unregisterService(on);
         }
         catch (Exception ignore)
         {
            //If the serverpeer failed when starting up previously, then only some of the
            //services may be started. The ones that didn't start will fail when attempting to shut
            //them down.
            //Hence we must catch and ignore or we won't shut everything down
         }
      }
      connFactoryObjectNames.clear();

   }

   public void stop() throws Exception
   {

      unloadJNDIContexts();

      stopService(REMOTING_OBJECT_NAME);

      if (httpConnectionFactory)
      {
         stopService(HTTP_REMOTING_OBJECT_NAME);
      }

      if (jca)
      {
         stopWrapperDataSourceService();
         stopConnectionManager(DEFAULTDS_CONNECTION_MANAGER_OBJECT_NAME);
         stopManagedConnectionPool(DEFAULTDS_MANAGED_CONNECTION_POOL_OBJECT_NAME);
         stopManagedConnectionFactory(DEFAULTDS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME);
         stopService(CACHED_CONNECTION_MANAGER_OBJECT_NAME);
      }

      stopService(TRANSACTION_MANAGER_OBJECT_NAME);

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

   public UserTransaction getUserTransaction() throws Exception
   {
      return (UserTransaction)initialContext.lookup(USER_TRANSACTION_JNDI_NAME);
   }

   public Object getService(ObjectName on) throws Exception
   {
      return mbeanServer.invoke(on, "getInstance", new Object[0], new String[0]);
   }

   public Properties getPersistenceManagerSQLProperties() throws Exception
   {
      String databaseType = getDatabaseType();

      String persistenceConfigFile =
         "server/default/deploy/" + databaseType + "-persistence-service.xml";

      log.info("Persistence config file: .... " + persistenceConfigFile);
      
      URL persistenceConfigFileURL = getClass().getClassLoader().getResource(persistenceConfigFile);
      if (persistenceConfigFileURL == null)
      {
         throw new Exception("Cannot find " + persistenceConfigFile + " in the classpath");
      }

      ServiceDeploymentDescriptor pdd = new ServiceDeploymentDescriptor(persistenceConfigFileURL);

      MBeanConfigurationElement persistenceManagerConfig =
         (MBeanConfigurationElement)pdd.query("service", "PersistenceManager").iterator().next();

      String props = persistenceManagerConfig.getAttributeValue("SqlProperties");

      if (props != null)
      {
         ByteArrayInputStream is = new ByteArrayInputStream(props.getBytes());

         Properties sqlProperties = new Properties();

         sqlProperties.load(is);

         return sqlProperties;
      }
      else
      {
         return null;
      }
   }

   public Properties getPostOfficeSQLProperties() throws Exception
   {
      String databaseType = getDatabaseType();

      String persistenceConfigFile =
         "server/default/deploy/" + databaseType + "-persistence-service.xml";

      log.info("Peristence config file: .. " + persistenceConfigFile);

      URL persistenceConfigFileURL = getClass().getClassLoader().getResource(persistenceConfigFile);
      if (persistenceConfigFileURL == null)
      {
         throw new Exception("Cannot find " + persistenceConfigFile + " in the classpath");
      }

      ServiceDeploymentDescriptor pdd = new ServiceDeploymentDescriptor(persistenceConfigFileURL);

      MBeanConfigurationElement postOfficeConfig =
         (MBeanConfigurationElement)pdd.query("service", "PostOffice").iterator().next();

      String props = postOfficeConfig.getAttributeValue("SqlProperties");

      if (props != null)
      {
         ByteArrayInputStream is = new ByteArrayInputStream(props.getBytes());

         Properties sqlProperties = new Properties();

         sqlProperties.load(is);

         return sqlProperties;
      }
      else
      {
         return null;
      }
   }

   public Properties getClusteredPostOfficeSQLProperties() throws Exception
   {
      String databaseType = getDatabaseType();

      String persistenceConfigFile;
      if (databaseType.equals("hsqldb"))
      {
         persistenceConfigFile =
            "server/default/deploy/" + databaseType + "-persistence-service.xml";
      }
      else
      {
         persistenceConfigFile =
            "server/default/deploy/clustered-" + databaseType + "-persistence-service.xml";
      }

      log.info("Persistence config file: .... " + persistenceConfigFile);

      URL persistenceConfigFileURL = getClass().getClassLoader().getResource(persistenceConfigFile);
      if (persistenceConfigFileURL == null)
      {
         throw new Exception("Cannot find " + persistenceConfigFile + " in the classpath");
      }

      ServiceDeploymentDescriptor pdd = new ServiceDeploymentDescriptor(persistenceConfigFileURL);

      MBeanConfigurationElement postOfficeConfig =
         (MBeanConfigurationElement)pdd.query("service", "PostOffice").iterator().next();

      String props = postOfficeConfig.getAttributeValue("SqlProperties");

      if (props != null)
      {
         ByteArrayInputStream is = new ByteArrayInputStream(props.getBytes());

         Properties sqlProperties = new Properties();

         sqlProperties.load(is);

         return sqlProperties;
      }
      else
      {
         return null;
      }
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

   public void addNotificationListener(ObjectName on, NotificationListener listener)
      throws Exception
   {
      mbeanServer.addNotificationListener(on, listener, null, null);
   }

   public void removeNotificationListener(ObjectName on, NotificationListener listener)
      throws Exception
   {
      mbeanServer.removeNotificationListener(on, listener);
   }

   public MBeanServer getMBeanServer()
   {
      return mbeanServer;
   }

   public void bindDefaultJMSProvider() throws Exception
   {
      JNDIProviderAdapter pa = new JNDIProviderAdapter();
      pa.setQueueFactoryRef("/ConnectionFactory");
      pa.setTopicFactoryRef("/ConnectionFactory");
      pa.setFactoryRef("/ConnectionFactory");
      initialContext.bind("java:/DefaultJMSProvider", pa);
   }

   public void unbindDefaultJMSProvider() throws Exception
   {
      initialContext.unbind("java:/DefaultJMSProvider");
   }

   public void bindJCAJMSConnectionFactory() throws Exception
   {
      deployJBossJMSRA(JMS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME);

      startManagedConnectionPool(JMS_MANAGED_CONNECTION_POOL_OBJECT_NAME,
                                 JMS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME,
                                 "ByApplication");

      startConnectionManager(JMS_CONNECTION_MANAGER_OBJECT_NAME, true, false, // not local, but XA(!)
                             TRANSACTION_MANAGER_OBJECT_NAME,
                             CachedConnectionManagerMBean.OBJECT_NAME,
                             JMS_MANAGED_CONNECTION_POOL_OBJECT_NAME);

      ObjectName on = JMS_CONNECTION_FACTORY_BINDING_SERVICE_OBJECT_NAME;

      // create it
      ConnectionFactoryBindingService cfBindingService = new ConnectionFactoryBindingService();

      // register it
      mbeanServer.registerMBean(cfBindingService, on);

      // configure it
      mbeanServer.setAttribute(on, new Attribute("ConnectionManager", JMS_CONNECTION_MANAGER_OBJECT_NAME));
      mbeanServer.setAttribute(on, new Attribute("JndiName", JCA_JMS_CONNECTION_FACTORY_JNDI_NAME));
      mbeanServer.setAttribute(on, new Attribute("UseJavaContext", Boolean.TRUE));

      // start it
      mbeanServer.invoke(on, "start", new Object[0], new String[0]);

      log.debug("started " + on);
   }

   /**
    * This method may be called twice successively, so it is important to handle graciously this
    * situation.
    */
   public void unbindJCAJMSConnectionFactory() throws Exception
   {
      ObjectName on = JMS_CONNECTION_FACTORY_BINDING_SERVICE_OBJECT_NAME;

      if (mbeanServer.isRegistered(on))
      {
         mbeanServer.invoke(on, "stop", new Object[0], new String[0]);
         mbeanServer.invoke(on, "destroy", new Object[0], new String[0]);
         mbeanServer.unregisterMBean(on);
      }

      stopConnectionManager(JMS_CONNECTION_MANAGER_OBJECT_NAME);
      stopManagedConnectionPool(JMS_MANAGED_CONNECTION_POOL_OBJECT_NAME);
      undeployJBossJMSRA(JMS_MANAGED_CONNECTION_FACTORY_OBJECT_NAME);
   }

   public String getDatabaseType()
   {
      return config.getDatabaseType();
   }

   public String getRemotingTransport()
   {
      return config.getRemotingTransport();
   }

   public boolean isClustered()
   {
      return config.isClustered();
   }
   
   public void installJMSProviderAdaptor(String jndi, JMSProviderAdapter adaptor) throws Exception
   {
      log.info("Binding adaptor " + adaptor + " in JNDI: " + jndi);
      initialContext.bind(jndi, adaptor);

   }

   public void uninstallJMSProviderAdaptor(String jndi) throws Exception
   {
      initialContext.unbind(jndi);
   }
   
   public void startRecoveryManager()
   {
      log.info("Starting arjuna recovery manager");

      //Need to start the recovery manager manually - if deploying
      //inside JBoss this wouldn't be necessary - since you would use
      //the TransactionManagerService MBean which would start the recovery manager
      //for you
      recoveryManager = RecoveryManager.manager(RecoveryManager.INDIRECT_MANAGEMENT);

      log.info("Started recovery manager");
   }
   
   public void stopRecoveryManager()
   {
      if (recoveryManager != null)
      {
         recoveryManager.stop();
      }      
   }

   public String toString()
   {
      return "ServiceContainer[" + Integer.toHexString(hashCode()) + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   /**
    * Note that this method makes no assumption on whether the service was created or started, nor
    * does it attempt to create/start the service.
    *
    * @param service - a Standard/DynamicMBean instance.
    */
   private void registerService(Object service, ObjectName on) throws Exception
   {
      mbeanServer.registerMBean(service, on);
      log.debug(service + " registered as " + on);
   }

   private void readConfigurationFile() throws Exception
   {
      InputStream cs = getClass().getClassLoader().getResourceAsStream(CONFIGURATION_FILE_NAME);
      if (cs == null)
      {
         throw new Exception("Cannot file container's configuration file " +
                             CONFIGURATION_FILE_NAME + ". Make sure it is in the classpath.");
      }

      try
      {
         config = new ServiceContainerConfiguration(cs);
      }
      finally
      {
         cs.close();
      }
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
      if (!"hsqldb".equals(config.getDatabaseType()))
      {
         // is an out-of-process DB, and it must be stared externally
         return;
      }

      log.debug("starting " + config.getDatabaseType() + " in-VM");

      String url = config.getDatabaseConnectionURL();
      HsqlProperties props = new HsqlProperties();
      props.setProperty("server.database.0", ServiceContainerConfiguration.getHypersonicDatabase(url));
      props.setProperty("server.dbname.0", ServiceContainerConfiguration.getHypersonicDbname(url));
      props.setProperty("server.trace", "false");
      props.setProperty("server.silent", "true");
      props.setProperty("server.no_system_exit", "true");
      props.setProperty("server.port", 27862);
      props.setProperty("server.address", ipAddressOrHostName);

      hsqldbServer = new Server();
      hsqldbServer.setLogWriter(null);
      hsqldbServer.setProperties(props);
      hsqldbServer.start();

      log.debug("started " + config.getDatabaseType() + " in-VM");
   }

   private void stopInVMDatabase() throws Exception
   {
      if (!"hsqldb".equals(config.getDatabaseType()))
      {
         // is an out-of-process DB, and it must be stopped externally
         return;
      }

      log.debug("stop " + hsqldbServer);

      Class.forName(config.getDatabaseDriverClass());

      Connection conn =
         DriverManager.getConnection(config.getDatabaseConnectionURL(),
                                     config.getDatabaseUserName(),
                                     config.getDatabasePassword());

      Statement stat = conn.createStatement();
      stat.executeUpdate("SHUTDOWN");
      conn.close();

      // faster stop
      // hsqldbServer.stop();
   }

   private void startTransactionManager() throws Exception
   {
      if (tm == null)
      {
         if (jbossjta)
         {
            log.info("Starting arjuna tx mgr");
            tm = com.arjuna.ats.jta.TransactionManager.transactionManager();
         }
         else
         {
            log.info("Starting non arjuna tx mgr");
            tm = TxManager.getInstance();
         }
      }

      TransactionManagerJMXWrapper mbean = new TransactionManagerJMXWrapper(tm);
      mbeanServer.registerMBean(mbean, TRANSACTION_MANAGER_OBJECT_NAME);
      mbeanServer.invoke(TRANSACTION_MANAGER_OBJECT_NAME, "start", new Object[0], new String[0]);
      log.debug("started " + TRANSACTION_MANAGER_OBJECT_NAME);

      initialContext.bind(TRANSACTION_MANAGER_JNDI_NAME, tm);
      toUnbindAtExit.add(TRANSACTION_MANAGER_JNDI_NAME);

      log.debug("bound " + TRANSACTION_MANAGER_JNDI_NAME);

      initialContext.
         rebind(USER_TRANSACTION_JNDI_NAME, ServerVMClientUserTransaction.getSingleton());

      log.debug("bound " + USER_TRANSACTION_JNDI_NAME);
   }

   private boolean deleteDirectory(File directory)
   {
      if (directory.isDirectory())
      {
         String[] files = directory.list();

         for (int j = 0; j < files.length; j++)
         {
            if (!deleteDirectory(new File(directory, files[j])))
            {
               return false;
            }
         }
      }

      return directory.delete();
   }

   private void deleteObjectStore()
   {
      // First delete the object store - might have been left over from a previous run

      String objectStoreDir = System.getProperty("objectstore.dir");

      log.info("Deleting object store: " + objectStoreDir);

      if (objectStoreDir == null)
      {
         log.warn("Cannot find objectstore.dir parameter");
      }
      else
      {
         File f = new File(objectStoreDir);

         deleteDirectory(f);
      }
   }



   private void startCachedConnectionManager(ObjectName on) throws Exception
   {
      CachedConnectionManager ccm = new CachedConnectionManager();

      // dependencies
      ccm.setTransactionManagerServiceName(TRANSACTION_MANAGER_OBJECT_NAME);

      mbeanServer.registerMBean(ccm, on);
      mbeanServer.invoke(on, "start", new Object[0], new String[0]);
      log.debug("started " + on);

   }

   /**
    * Database specific.
    */
   private void startManagedConnectionFactory(ObjectName on) throws Exception
   {
      LocalManagedConnectionFactory mcf = new LocalManagedConnectionFactory();


log.info("connection url:" + config.getDatabaseConnectionURL());
log.info("driver:" + config.getDatabaseConnectionURL());
log.info("username:" + config.getDatabaseUserName());
log.info("password:" + config.getDatabasePassword());
      mcf.setConnectionURL(config.getDatabaseConnectionURL());
      mcf.setDriverClass(config.getDatabaseDriverClass());
      mcf.setUserName(config.getDatabaseUserName());
      mcf.setPassword(config.getDatabasePassword());
      String isolation = config.getDatabaseTransactionIsolation();
      if (isolation != null)
      {
         mcf.setTransactionIsolation(isolation);
      }

      ManagedConnectionFactoryJMXWrapper mbean = new ManagedConnectionFactoryJMXWrapper(mcf);
      mbeanServer.registerMBean(mbean, on);
      mbeanServer.invoke(on, "start", new Object[0], new String[0]);
      log.debug("started " + on);
   }

   /**
    * This method may be called twice successively, so it is important to handle graciously this
    * situation.
    */
   private void stopManagedConnectionFactory(ObjectName on) throws Exception
   {
      stopService(on);
   }

   private void startManagedConnectionPool(ObjectName on,
                                           ObjectName managedConnectionFactoryObjectName,
                                           String criteria) throws Exception
   {
      JBossManagedConnectionPool mcp = new JBossManagedConnectionPool();
      mcp.setCriteria(criteria);

      // dependencies
      mcp.setManagedConnectionFactoryName(managedConnectionFactoryObjectName);

      mbeanServer.registerMBean(mcp, on);
      mbeanServer.invoke(on, "start", new Object[0], new String[0]);
      log.debug("started " + on);
   }

   /**
    * This method may be called twice successively, so it is important to handle graciously this
    * situation.
    */
   private void stopManagedConnectionPool(ObjectName on) throws Exception
   {
      stopService(on);
   }

   private TxConnectionManager startConnectionManager(ObjectName on,
                                                      boolean trackConnectionByTx,
                                                      boolean localTransactions,
                                                      ObjectName transactionManagerObjectName,
                                                      ObjectName cachedConnectionManagerObjectName,
                                                      ObjectName managedConnectionPoolObjectName)
      throws Exception
   {
      TxConnectionManager cm = new TxConnectionManager();
      cm.preRegister(mbeanServer, on);
      cm.setTrackConnectionByTx(trackConnectionByTx);
      cm.setLocalTransactions(localTransactions);

      // dependencies
      cm.setTransactionManagerService(transactionManagerObjectName);
      cm.setCachedConnectionManager(cachedConnectionManagerObjectName);
      cm.setManagedConnectionPool(managedConnectionPoolObjectName);

      mbeanServer.registerMBean(cm, on);
      mbeanServer.invoke(on, "start", new Object[0], new String[0]);
      log.debug("started " + on);

      return cm;
   }

   /**
    * This method may be called twice successively, so it is important to handle graciously this
    * situation.
    */
   private void stopConnectionManager(ObjectName on) throws Exception
   {
      stopService(on);
   }

   private void startWrapperDataSourceService() throws Exception
   {
      WrapperDataSourceService wdss = new WrapperDataSourceService();
      wdss.setJndiName(DATA_SOURCE_JNDI_NAME);

      // dependencies
      wdss.setConnectionManager(DEFAULTDS_CONNECTION_MANAGER_OBJECT_NAME);
      ObjectName irrelevant = new ObjectName(":name=irrelevant");
      wdss.setJMXInvokerName(irrelevant);
      Registry.bind(irrelevant, new NoopInvoker());

      mbeanServer.registerMBean(wdss, DEFAULTDS_WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME);
      mbeanServer.invoke(DEFAULTDS_WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME, "start", new Object[0], new String[0]);

      log.debug("started " + DEFAULTDS_WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME);
   }

   private void stopWrapperDataSourceService() throws Exception
   {
      stopService(DEFAULTDS_WRAPPER_DATA_SOURCE_SERVICE_OBJECT_NAME);
   }

   private void deployJBossJMSRA(ObjectName managedConnFactoryObjectName) throws Exception
   {
      JmsManagedConnectionFactory mcf = new JmsManagedConnectionFactory();
//      mcf.setClientID("");
//      mcf.setUserName("");
//      mcf.setPassword("");
      mcf.setJmsProviderAdapterJNDI("java:/DefaultJMSProvider");
      mcf.setStrict(true);
      mcf.setSessionDefaultType("javax.jms.Queue");

      registerService(new ManagedConnectionFactoryJMXWrapper(mcf), managedConnFactoryObjectName);
   }

   /**
    * This method may be called twice successively, so it is important to handle graciously this
    * situation.
    */
   private void undeployJBossJMSRA(ObjectName managedConnFactoryObjectName) throws Exception
   {
      stopService(managedConnFactoryObjectName);
   }

   private void startRemoting(ServiceAttributeOverrides attrOverrides,
                              String transport,
                              ObjectName objectName) throws Exception
   {
      log.debug("Starting remoting transport=" + transport + " objectName=" + objectName);
      RemotingJMXWrapper mbean;
      String locatorURI = null;

      // some tests may want specific locator URI overrides to simulate special conditions; use
      // that with priority, if available
      Map overrideMap = null;

      if (attrOverrides != null)
      {
         overrideMap = attrOverrides.get(objectName);

         if (overrideMap != null)
         {
            locatorURI = (String)overrideMap.get("LocatorURI");
         }
      }

      if (locatorURI == null)
      {
         // TODO - use remoting-service.xml parameters, not these ...

         String serializationType = config.getSerializationType();

         //TODO - Actually serializationType is irrelevant since we pass a DataOutput/InputStream
         //       into the marshaller and don't use serialization apart from one specific case with
         //       a JMS ObjectMessage in which case Java serialization is always currently used -
         //       (we could make this configurable)

         long clientLeasePeriod = 20000;

         String marshallers =
            "marshaller=org.jboss.jms.server.remoting.JMSWireFormat&" +
            "unmarshaller=org.jboss.jms.server.remoting.JMSWireFormat&";
         String dataType = "dataType=jms&";

         // We use this from thirdparty remoting tests when we don't want to send stuff through
         // JMSWireFormat, but we want everything else in the connector's configuration to be
         // identical with what we use in Messaging
         if (overrideMap != null && overrideMap.get(DO_NOT_USE_MESSAGING_MARSHALLERS) != null)
         {
            marshallers = "";
            dataType = "";
            serializationType = "java";
         }
         
         // Note that we DO NOT want the direct thread pool on the server side - since that can lead
         // to deadlocks

         String params =
            "/?" +
            marshallers +
            "serializationtype=" + serializationType + "&" +
            dataType +
            "socket.check_connection=false&" +
            "clientLeasePeriod=" + clientLeasePeriod + "&" +
            "callbackStore=org.jboss.remoting.callback.BlockingCallbackStore&" +
            "clientSocketClass=org.jboss.jms.client.remoting.ClientSocketWrapper&" +
            "serverSocketClass=org.jboss.jms.server.remoting.ServerSocketWrapper&" +
            "NumberOfRetries=1&" +
            "NumberOfCallRetries=2&" +
            "callbackErrorsAllowed=1";
         
         
         
         // specific parameters per transport

         if ("http".equals(transport))
         {
            params += "&callbackPollPeriod=" + HTTP_CONNECTOR_CALLBACK_POLL_PERIOD;
         }
         else
         {
            params += "&timeout=0";
         }
         
         if ("sslbisocket".equals(transport) || "sslsocket".equals(transport))
         {
            System.setProperty("javax.net.ssl.keyStorePassword", "secureexample");
            String keyStoreFilePath = this.getClass().getResource("../../../../../../../etc/messaging.keystore").getFile();
            System.setProperty("javax.net.ssl.keyStore", keyStoreFilePath);
         }
         
         int freePort = PortUtil.findFreePort(ipAddressOrHostName);
         locatorURI = transport + "://" + ipAddressOrHostName + ":" + freePort + params;
         log.info("creating server for: " + locatorURI);
      }


      log.debug("Using locator uri: " + locatorURI);

      InvokerLocator locator = new InvokerLocator(locatorURI);

      log.debug("Started remoting connector on uri:" + locator.getLocatorURI());

      mbean = new RemotingJMXWrapper(locator);
      mbeanServer.registerMBean(mbean, objectName);
      mbeanServer.invoke(objectName, "start", new Object[0], new String[0]);

      ServerInvocationHandler handler = new JMSServerInvocationHandler();

      mbeanServer.invoke(objectName, "addInvocationHandler",
                         new Object[] { ServerPeer.REMOTING_JMS_SUBSYSTEM, handler},
                         new String[] { "java.lang.String",
                                        "org.jboss.remoting.ServerInvocationHandler"});

      log.debug("started " + objectName);
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
   
   private void executeStatement(TransactionManager mgr, DataSource ds, String statement) throws Exception
   {
      Connection conn = null;
      boolean exception = false;
   
      try
      {
         try
         {
            mgr.begin();            
            
            conn = ds.getConnection();
            
            log.debug("executing " + statement);
            
            PreparedStatement ps = conn.prepareStatement(statement);
      
            ps.executeUpdate();
      
            log.debug(statement + " executed");
      
            ps.close();           
         }
         catch (SQLException e)
         {
            // Ignore
            log.debug("Failed to execute statement", e);
            exception = true;
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }         
         
         if (exception)
         {
            mgr.rollback();
         }
         else
         {
            mgr.commit();
         }
      }
      
     
   }

   protected void dropAllTables() throws Exception
   {
      log.info("DROPPING ALL TABLES FROM DATABASE!");

      InitialContext ctx = new InitialContext();
      
      // We need to execute each drop in its own transaction otherwise postgresql will not execute
      // further commands after one fails

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");

      javax.transaction.Transaction txOld = mgr.suspend();
                  
      executeStatement(mgr, ds, "DROP TABLE JBM_POSTOFFICE");
      
      executeStatement(mgr, ds, "DROP TABLE JBM_MSG_REF");

      executeStatement(mgr, ds, "DROP TABLE JBM_MSG");
     
      executeStatement(mgr, ds, "DROP TABLE JBM_TX");
      
      executeStatement(mgr, ds, "DROP TABLE JBM_COUNTER");
      
      executeStatement(mgr, ds, "DROP TABLE JBM_USER");
      
      executeStatement(mgr, ds, "DROP TABLE JBM_ROLE");
      
      if (txOld != null)
      {
         mgr.resume(txOld);
      }

      log.debug("done with the database");
   }

   private void startMultiplexer() throws Exception
   {
      log.debug("Starting multiplexer");

      String multiplexerConfigFile = "server/default/deploy/multiplexer-service.xml";
      URL multiplexerCofigURL = getClass().getClassLoader().getResource(multiplexerConfigFile);

      if (multiplexerCofigURL == null)
      {
         throw new Exception("Cannot find " + multiplexerCofigURL + " in the classpath");
      }

      ServiceDeploymentDescriptor multiplexerDD =
         new ServiceDeploymentDescriptor(multiplexerCofigURL);

      List services = multiplexerDD.query("name", "Multiplexer");

      if (services.isEmpty())
      {
         log.info("Couldn't find multiplexer config");
      }
      else
      {
         log.info("Could find multiplexer config");
      }

      MBeanConfigurationElement multiplexerConfig =
         (MBeanConfigurationElement)services.iterator().next();
      ObjectName nameMultiplexer = registerAndConfigureService(multiplexerConfig);
      invoke(nameMultiplexer,"create", new Object[0], new String[0]);
      invoke(nameMultiplexer,"start", new Object[0], new String[0]);
   }

   private void overrideAttributes(ObjectName on, ServiceAttributeOverrides attrOverrides)
      throws Exception
   {
      if (attrOverrides == null)
      {
         return;
      }

      Map sao = attrOverrides.get(on);

      for(Iterator i = sao.entrySet().iterator(); i.hasNext();)
      {
         Map.Entry entry = (Map.Entry)i.next();
         String attrName = (String)entry.getKey();
         Object attrValue = entry.getValue();
         setAttribute(on, attrName, attrValue.toString());

      }
   }

   public void deployConnectionFactories(String connFactoryConfigFile,
                                         ServiceAttributeOverrides attrOverrides) throws Exception
   {

      URL connFactoryConfigFileURL =
         getClass().getClassLoader().getResource(connFactoryConfigFile);

      if (connFactoryConfigFileURL == null)
      {
         throw new Exception("Cannot find " + connFactoryConfigFile + " in the classpath");
      }

      ServiceDeploymentDescriptor cfdd =
         new ServiceDeploymentDescriptor(connFactoryConfigFileURL);
      List connFactoryElements = cfdd.query("service", "ConnectionFactory");
      if (connFactoryElements.isEmpty())
      {
         connFactoryElements = cfdd.query("service", "HTTPConnectionFactory");
      }
      connFactoryObjectNames.clear();
      for (Iterator i = connFactoryElements.iterator(); i.hasNext();)
      {
         MBeanConfigurationElement connFactoryElement = (MBeanConfigurationElement) i.next();
         ObjectName on = registerAndConfigureService(connFactoryElement);
         overrideAttributes(on, attrOverrides);
         // dependencies have been automatically injected already
         invoke(on, "create", new Object[0], new String[0]);
         invoke(on, "start", new Object[0], new String[0]);
         connFactoryObjectNames.add(on);
      }
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
            remoting = true;
            security = true;
         }
         else
         if ("all+http".equals(tok))
         {
            transaction = true;
            database = true;
            jca = true;
            remoting = true;
            security = true;
            httpConnectionFactory = true;
         }
         else if ("transaction".equals(tok))
         {
            transaction = true;
            if (minus)
            {
               transaction = false;
            }
         }
         else if ("jbossjta".equals(tok))
         {
            if (transaction)
            {
               throw new IllegalArgumentException("Cannot have the old JBoss transaction manager AND the JBoss Transactions transaction manager");
            }
            
            //Use the JBoss Transactions (ex Arjuna) JTA
            jbossjta = true;
            if (minus)
            {
               jbossjta = false;
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
            remoting = true;
            if (minus)
            {
               remoting = false;
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
         else if ("multiplexer".equals(tok))
         {
            multiplexer = true;
            if (minus)
            {
               multiplexer = false;
            }
         }
         else if ("none".equals(tok))
         {
            transaction = false;
            database = false;
            jca = false;
            remoting = false;
            security = false;
            multiplexer = false;
         }
         else
         {
            throw new IllegalArgumentException("Unknown service: " + tok);
         }
      }
   }

   // Inner classes --------------------------------------------------------------------------------
}
