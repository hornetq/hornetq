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
package org.jboss.test.messaging.tools.jmx.rmi;

import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.transaction.UserTransaction;

import org.jboss.aop.AspectXmlLoader;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.util.XMLUtil;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.aop.PoisonInterceptor;
import org.jboss.test.messaging.tools.jboss.MBeanConfigurationElement;
import org.jboss.test.messaging.tools.jboss.ServiceDeploymentDescriptor;
import org.jboss.test.messaging.tools.jmx.MockJBossSecurityManager;
import org.jboss.test.messaging.tools.jmx.RemotingJMXWrapper;
import org.jboss.test.messaging.tools.jmx.ServiceAttributeOverrides;
import org.jboss.test.messaging.tools.jmx.ServiceConfigHelper;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.messaging.tools.jndi.Constants;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * LocalTestServer.java,v 1.1 2006/02/21 08:25:32 timfox Exp
 */
public class LocalTestServer implements Server
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(LocalTestServer.class);

   // Static ---------------------------------------------------------------------------------------

   public static void setEnvironmentServerIndex(int serverIndex)
   {
      System.setProperty(Constants.SERVER_INDEX_PROPERTY_NAME, Integer.toString(serverIndex));
   }

   public static void clearEnvironmentServerIndex()
   {
      System.getProperty(Constants.SERVER_INDEX_PROPERTY_NAME, null);
   }

   // Attributes -----------------------------------------------------------------------------------

   private ServiceContainer sc;

   // service dependencies   
   private ObjectName persistenceManagerObjectName;
   private ObjectName postOfficeObjectName;
   private ObjectName jmsUserManagerObjectName;

   // the server MBean itself
   private ObjectName serverPeerObjectName;

   private int serverIndex;

   // Constructors ---------------------------------------------------------------------------------

   public LocalTestServer()
   {
      super();
   }

   public LocalTestServer(int serverIndex)
   {
      this();

      this.serverIndex = serverIndex;
   }

   // Server implementation ------------------------------------------------------------------------

   public int getServerID()
   {
      return serverIndex;
   }

   public void start(String containerConfig,
                     boolean clearDatabase) throws Exception
   {
      start(containerConfig, null, clearDatabase, true);
   }

   public synchronized void start(String containerConfig,
                                  ServiceAttributeOverrides attrOverrides,
                                  boolean clearDatabase,
                                  boolean startMessagingServer) throws Exception
   {
      if (isStarted())
      {
         return;
      }

      log.debug("starting service container");

      try
      {
         setEnvironmentServerIndex(serverIndex);

         sc = new ServiceContainer(containerConfig, null, serverIndex);
         sc.start(clearDatabase, attrOverrides);

         if (sc.getDatabaseName().equals("hsqldb") && sc.isClustered())
         {
            throw new IllegalStateException("The test server cannot be started in clustered mode with hsqldb as a database - must use a shared database");
         }

         if ("none".equals(containerConfig))
         {
            return;
         }

         if (startMessagingServer)
         {
            startServerPeer(serverIndex, null, null, attrOverrides, sc.isClustered());
         }

         log.info("Server " + serverIndex + " started");
      }
      finally
      {
         clearEnvironmentServerIndex();
      }
   }

   public synchronized boolean stop() throws Exception
   {
      if (!isStarted())
      {
         return false;
      }

      try
      {
         setEnvironmentServerIndex(serverIndex);

         stopServerPeer();

         log.debug("stopping service container");

         sc.stop();
         sc = null;

         log.info("server stopped");
      }
      finally
      {
         clearEnvironmentServerIndex();
      }

      return true;
   }


   public void ping() throws Exception
   {
      // noop... do nothing
   }

   public synchronized void kill() throws Exception
   {
      throw new IllegalStateException("Cannot KILL a local server. Consider using stop() instead.");
   }

   public ObjectName deploy(String mbeanConfiguration) throws Exception
   {
      Element mbeanElement = XMLUtil.stringToElement(mbeanConfiguration);
      MBeanConfigurationElement mbc = new MBeanConfigurationElement(mbeanElement);
      return sc.registerAndConfigureService(mbc);
   }

   public void undeploy(ObjectName on) throws Exception
   {
      sc.unregisterService(on);
   }

   public Object getAttribute(ObjectName on, String attribute) throws Exception
   {
      return sc.getAttribute(on, attribute);
   }

   public void setAttribute(ObjectName on, String name, String valueAsString) throws Exception
   {
      sc.setAttribute(on, name, valueAsString);
   }

   public Object invoke(ObjectName on, String operationName, Object[] params, String[] signature)
      throws Exception
   {
      return sc.invoke(on, operationName, params, signature);
   }

   public void addNotificationListener(ObjectName on, NotificationListener listener)
      throws Exception
   {
      sc.addNotificationListener(on, listener);
   }

   public void removeNotificationListener(ObjectName on, NotificationListener listener)
      throws Exception
   {
      sc.removeNotificationListener(on, listener);
   }

   public Set query(ObjectName pattern) throws Exception
   {
      return sc.query(pattern);
   }

   public String getRemotingTransport()
   {
      return sc.getRemotingTransport();
   }

   public void log(int level, String text)
   {
      if (ServerManagement.FATAL == level)
      {
         log.fatal(text);
      }
      else if (ServerManagement.ERROR == level)
      {
         log.error(text);
      }
      else if (ServerManagement.WARN == level)
      {
         log.warn(text);
      }
      else if (ServerManagement.INFO == level)
      {
         log.info(text);
      }
      else if (ServerManagement.DEBUG == level)
      {
         log.debug(text);
      }
      else if (ServerManagement.TRACE == level)
      {
         log.trace(text);
      }
      else
      {
         // log everything else as INFO
         log.info(text);
      }
   }

   public synchronized boolean isStarted() throws Exception
   {
      return sc != null;
   }

   public void startServerPeer(int serverPeerID,
                               String defaultQueueJNDIContext,
                               String defaultTopicJNDIContext,
                               ServiceAttributeOverrides attrOverrides,
                               boolean clustered) throws Exception
   {
      try
      {
         log.debug("creating ServerPeer instance");

         // we are using the "default" service deployment descriptors available in
         // src/etc/server/default/deploy. This will allow to test the default parameters we ship.

         String mainConfigFile = "server/default/deploy/messaging-service.xml";
         
         String persistenceConfigFile = sc.getPersistenceConfigFile(clustered);

         log.info(" Persistence config file .. " + persistenceConfigFile);

         ServiceDeploymentDescriptor mdd = ServiceConfigHelper.loadConfigFile(mainConfigFile);
         
         ServiceDeploymentDescriptor pdd = ServiceConfigHelper.loadConfigFile(persistenceConfigFile);

         MBeanConfigurationElement persistenceManagerConfig =
            ServiceConfigHelper.getServiceConfiguration(pdd, "PersistenceManager");
         persistenceManagerObjectName = sc.registerAndConfigureService(persistenceManagerConfig);
         overrideAttributes(persistenceManagerObjectName, attrOverrides);
         sc.invoke(persistenceManagerObjectName, "create", new Object[0], new String[0]);
         sc.invoke(persistenceManagerObjectName, "start", new Object[0], new String[0]);

         MBeanConfigurationElement jmsUserManagerConfig =
            ServiceConfigHelper.getServiceConfiguration(pdd, "JMSUserManager");
         jmsUserManagerObjectName = sc.registerAndConfigureService(jmsUserManagerConfig);
         overrideAttributes(jmsUserManagerObjectName, attrOverrides);
         sc.invoke(jmsUserManagerObjectName, "create", new Object[0], new String[0]);
         sc.invoke(jmsUserManagerObjectName, "start", new Object[0], new String[0]);

         // register server peer as a service, dependencies are injected automatically
         MBeanConfigurationElement serverPeerConfig =
            ServiceConfigHelper.getServiceConfiguration(mdd, "ServerPeer");

         // overwrite the file configuration, if needed
         overrideServerPeerConfiguration(serverPeerConfig, serverPeerID, defaultQueueJNDIContext,
               defaultTopicJNDIContext);

         serverPeerObjectName = sc.registerAndConfigureService(serverPeerConfig);

         overrideAttributes(serverPeerObjectName, attrOverrides);

         // overwrite the config file security domain
         sc.setAttribute(serverPeerObjectName, "SecurityDomain",
                         MockJBossSecurityManager.TEST_SECURITY_DOMAIN);

         log.debug("starting JMS server");

         sc.invoke(serverPeerObjectName, "create", new Object[0], new String[0]);
         sc.invoke(serverPeerObjectName, "start", new Object[0], new String[0]);

         MBeanConfigurationElement postOfficeConfig =
            ServiceConfigHelper.getServiceConfiguration(pdd, "PostOffice");

         postOfficeObjectName = sc.registerAndConfigureService(postOfficeConfig);
         overrideAttributes(postOfficeObjectName, attrOverrides);
         sc.invoke(postOfficeObjectName, "create", new Object[0], new String[0]);
         sc.invoke(postOfficeObjectName, "start", new Object[0], new String[0]);

         log.debug("deploying connection factories");

         sc.startConnectionFactories(attrOverrides);
      }
      catch (Exception e)
      {
         log.error("Failed to start server peer", e);
         throw e;
      }
   }

   public void stopServerPeer() throws Exception
   {
      if (!isServerPeerStarted())
      {
         log.debug("messaging server was not started, so there is no stopping");
         return;
      }

      try
      {
         // if we don't find a ServerPeer instance registered under the serverPeerObjectName
         // ObjectName, we assume that the server was already stopped and we silently exit
         if (sc.query(serverPeerObjectName).isEmpty())
         {
            log.warn("ServerPeer already stopped");
            return;
         }

         // unbind the JCA ConnectionFactory; nothing happens if no connection factory is bound
         sc.unbindJCAJMSConnectionFactory();
         sc.unbindDefaultJMSProvider();

         log.debug("stopping connection factories");

         sc.stopConnectionFactories();

         log.debug("stopping all destinations");

         Set destinations = (Set)sc.getAttribute(serverPeerObjectName, "Destinations");

         for(Iterator i = destinations.iterator(); i.hasNext(); )
         {
            String name;
            boolean isQueue = true;
            Destination d = (Destination)i.next();
            if (d instanceof Queue)
            {
               name = ((Queue)d).getQueueName();
            }
            else
            {
               isQueue = false;
               name = ((Topic)d).getTopicName();
            }

            undeployDestination(isQueue, name);
         }

         log.debug("stopping JMS server");

         try
         {
            sc.invoke(serverPeerObjectName, "stop", new Object[0], new String[0]);
            sc.invoke(serverPeerObjectName, "destroy", new Object[0], new String[0]);
            sc.unregisterService(serverPeerObjectName);
         }
         catch (Exception ignore)
         {
            //If the serverpeer failed when starting up previously, then only some of the
            //services may be started. The ones that didn't start will fail when attempting to shut
            //them down.
            //Hence we must catch and ignore or we won't shut everything down
         }

         log.debug("stopping ServerPeer's plug-in dependencies");

         try
         {
            sc.invoke(jmsUserManagerObjectName, "stop", new Object[0], new String[0]);
            sc.invoke(jmsUserManagerObjectName, "destroy", new Object[0], new String[0]);
            sc.unregisterService(jmsUserManagerObjectName);
         }
         catch (Exception ignore)
         {
            //If the serverpeer failed when starting up previously, then only some of the
            //services may be started. The ones that didn't start will fail when attempting to shut
            //them down.
            //Hence we must catch and ignore or we won't shut everything down
         }

         try
         {
            sc.invoke(postOfficeObjectName, "stop", new Object[0], new String[0]);
            sc.invoke(postOfficeObjectName, "destroy", new Object[0], new String[0]);
            sc.unregisterService(postOfficeObjectName);
         }
         catch (Exception ignore)
         {
            //If the serverpeer failed when starting up previously, then only some of the
            //services may be started. The ones that didn't start will fail when attempting to shut
            //them down.
            //Hence we must catch and ignore or we won't shut everything down
         }

         try
         {
            sc.invoke(persistenceManagerObjectName, "stop", new Object[0], new String[0]);
            sc.invoke(persistenceManagerObjectName, "destroy", new Object[0], new String[0]);
            sc.unregisterService(persistenceManagerObjectName);
         }
         catch (Exception ignore)
         {
            //If the serverpeer failed when starting up previously, then only some of the
            //services may be started. The ones that didn't start will fail when attempting to shut
            //them down.
            //Hence we must catch and ignore or we won't shut everything down
         }
      }
      catch (Exception e)
      {
         log.error("Failed to stop server peer", e);
         throw e;
      }
   }

   public boolean isServerPeerStarted() throws Exception
   {
      if (sc.query(serverPeerObjectName).isEmpty())
      {
         return false;
      }
      ServerPeer sp = (ServerPeer)sc.getAttribute(serverPeerObjectName, "Instance");
      sp.getVersion();
      return true;
   }

   public ObjectName getServerPeerObjectName()
   {
      return serverPeerObjectName;
   }

   public Set getConnectorSubsystems() throws Exception
   {
      RemotingJMXWrapper remoting =
         (RemotingJMXWrapper)sc.getService(ServiceContainer.REMOTING_OBJECT_NAME);

      return remoting.getConnectorSubsystems();
   }

   public void addServerInvocationHandler(String subsystem, ServerInvocationHandler handler)
      throws Exception
   {
      RemotingJMXWrapper remoting =
         (RemotingJMXWrapper)sc.getService(ServiceContainer.REMOTING_OBJECT_NAME);

      remoting.addInvocationHandler(subsystem, handler);
   }

   public void removeServerInvocationHandler(String subsystem) throws Exception
   {
      RemotingJMXWrapper remoting =
         (RemotingJMXWrapper)sc.getService(ServiceContainer.REMOTING_OBJECT_NAME);

      remoting.removeInvocationHandler(subsystem);
   }

   /**
    * Only for in-VM use!
    */
   public MessageStore getMessageStore() throws Exception
   {
      ServerPeer serverPeer = (ServerPeer)sc.getAttribute(serverPeerObjectName, "Instance");
      return serverPeer.getMessageStore();
   }

   public DestinationManager getDestinationManager() throws Exception
   {
      ServerPeer serverPeer = (ServerPeer)sc.getAttribute(serverPeerObjectName, "Instance");
      return serverPeer.getDestinationManager();
   }

   public PersistenceManager getPersistenceManager() throws Exception
   {
      ServerPeer serverPeer = (ServerPeer)sc.getAttribute(serverPeerObjectName, "Instance");
      return serverPeer.getPersistenceManagerInstance();
   }

   /**
    * Only for in-VM use!
    */
   public ServerPeer getServerPeer() throws Exception
   {
      return (ServerPeer)sc.
         getAttribute(serverPeerObjectName, "Instance");
   }

   public void deployTopic(String name, String jndiName, boolean clustered) throws Exception
   {
      deployDestination(false, name, jndiName, clustered);
   }

   public void deployTopic(String name, String jndiName, int fullSize, int pageSize,
                           int downCacheSize, boolean clustered) throws Exception
   {
      deployDestination(false, name, jndiName, fullSize, pageSize, downCacheSize, clustered);
   }

   public void deployTopicProgrammatically(String name, String jndiName) throws Exception
   {
      sc.invoke(serverPeerObjectName, "deployTopic",
                new Object[] { name, jndiName },
                new String[] { "java.lang.String", "java.lang.String"} );
   }

   public void deployQueue(String name, String jndiName, boolean clustered) throws Exception
   {
      deployDestination(true, name, jndiName, clustered);
   }

   public void deployQueue(String name, String jndiName, int fullSize, int pageSize,
                           int downCacheSize, boolean clustered) throws Exception
   {
      deployDestination(true, name, jndiName, fullSize, pageSize, downCacheSize, clustered);
   }

   public void deployQueueProgrammatically(String name, String jndiName) throws Exception
   {
      sc.invoke(serverPeerObjectName, "deployQueue",
                new Object[] { name, jndiName },
                new String[] { "java.lang.String", "java.lang.String"} );
   }

   public void deployDestination(boolean isQueue, String name, String jndiName, boolean clustered) throws Exception
   {
      String config =
         "<mbean code=\"org.jboss.jms.server.destination." + (isQueue ? "QueueService" : "TopicService") + "\"" +
         "       name=\"jboss.messaging.destination:service=" + (isQueue ? "Queue" : "Topic") + ",name=" + name + "\"" +
         "       xmbean-dd=\"xmdesc/" + (isQueue ? "Queue" : "Topic" ) + "-xmbean.xml\">" +
         (jndiName != null ? "    <attribute name=\"JNDIName\">" + jndiName + "</attribute>" : "") +
         "       <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "       <attribute name=\"Clustered\">" + String.valueOf(clustered) + "</attribute>" +
         "</mbean>";

      MBeanConfigurationElement mbean =
         new MBeanConfigurationElement(XMLUtil.stringToElement(config));
      ObjectName deston = sc.registerAndConfigureService(mbean);
      sc.invoke(deston, "create", new Object[0], new String[0]);
      sc.invoke(deston, "start", new Object[0], new String[0]);
   }

   public void deployDestination(boolean isQueue,
                                 String name,
                                 String jndiName,
                                 int fullSize,
                                 int pageSize,
                                 int downCacheSize,
                                 boolean clustered) throws Exception
   {
      log.info("deploying queue, fullsize:" + fullSize + ", ps:" + pageSize + " dc size:" + downCacheSize);

      String config =
         "<mbean code=\"org.jboss.jms.server.destination." + (isQueue ? "QueueService" : "TopicService") + "\"" +
         "       name=\"jboss.messaging.destination:service=" + (isQueue ? "Queue" : "Topic") + ",name=" + name + "\"" +
         "       xmbean-dd=\"xmdesc/" + (isQueue ? "Queue" : "Topic" ) + "-xmbean.xml\">" +
         (jndiName != null ? "    <attribute name=\"JNDIName\">" + jndiName + "</attribute>" : "") +
         "       <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "    <attribute name=\"FullSize\">" + fullSize + "</attribute>" +
         "    <attribute name=\"PageSize\">" + pageSize + "</attribute>" +
         "    <attribute name=\"DownCacheSize\">" + downCacheSize + "</attribute>" +
         "    <attribute name=\"Clustered\">" + String.valueOf(clustered) + "</attribute>" +
         "</mbean>";

      MBeanConfigurationElement mbean =
         new MBeanConfigurationElement(XMLUtil.stringToElement(config));
      ObjectName deston = sc.registerAndConfigureService(mbean);
      sc.invoke(deston, "create", new Object[0], new String[0]);
      sc.invoke(deston, "start", new Object[0], new String[0]);
   }

   public void undeployDestination(boolean isQueue, String name) throws Exception
   {
      ObjectName pattern =
         new ObjectName("*:service=" + (isQueue ? "Queue" : "Topic") + ",name=" + name);
      Set s = sc.query(pattern);
      int size = s.size();
      if (size == 0)
      {
         log.debug("No such " + (isQueue ? "queue" : "topic") + " to undeploy: " + name);
         return;
      }
      if (size > 1)
      {
         throw new Exception("Too many destination with the same name: " + name);
      }
      ObjectName destinationObjectName = (ObjectName)s.iterator().next();
      sc.invoke(destinationObjectName, "stop", new Object[0], new String[0]);
      sc.invoke(destinationObjectName, "destroy", new Object[0], new String[0]);
      sc.unregisterService(destinationObjectName);
   }

   public boolean undeployDestinationProgrammatically(boolean isQueue, String name) throws Exception
   {
      if (isQueue)
      {
         return  ((Boolean)sc.invoke(serverPeerObjectName, "undeployQueue",
                                    new Object[] { name },
                                    new String[] { "java.lang.String"})).booleanValue();
      }
      else
      {
         return  ((Boolean)sc.invoke(serverPeerObjectName, "undeployTopic",
                                    new Object[] { name },
                                    new String[] { "java.lang.String"})).booleanValue();
      }
   }

   public void deployConnectionFactory(String objectName,
                                       String[] jndiBindings,
                                       int prefetchSize) throws Exception
   {
      deployConnectionFactory(objectName, jndiBindings, prefetchSize, -1, -1, -1, false, false);
   }

   public void deployConnectionFactory(String objectName,
                                       String[] jndiBindings) throws Exception
   {
      deployConnectionFactory(objectName, jndiBindings, -1, -1, -1, -1, false, false);
   }
   
   public void deployConnectionFactory(String objectName,
         String[] jndiBindings,
         int prefetchSize,
         int defaultTempQueueFullSize,
         int defaultTempQueuePageSize,
         int defaultTempQueueDownCacheSize) throws Exception
   {
   	this.deployConnectionFactory(objectName, jndiBindings, prefetchSize, defaultTempQueueFullSize,
   			defaultTempQueuePageSize, defaultTempQueueDownCacheSize, false, false);   			
   }
   
   public void deployConnectionFactory(String objectName,
         String[] jndiBindings,
         boolean supportsFailover, boolean supportsLoadBalancing) throws Exception
   {
   	this.deployConnectionFactory(objectName, jndiBindings, -1, -1,
   			-1, -1, supportsFailover, supportsLoadBalancing);   			
   }

   private void deployConnectionFactory(String objectName,
                                       String[] jndiBindings,
                                       int prefetchSize,
                                       int defaultTempQueueFullSize,
                                       int defaultTempQueuePageSize,
                                       int defaultTempQueueDownCacheSize,
                                       boolean supportsFailover,
                                       boolean supportsLoadBalancing) throws Exception
   {
      log.trace("deploying connection factory with name: " + objectName);
      
      String config =
         "<mbean code=\"org.jboss.jms.server.connectionfactory.ConnectionFactory\"\n" +
                "name=\"" + objectName + "\"\n" +
                "xmbean-dd=\"xmdesc/ConnectionFactory-xmbean.xml\">\n" +
         "<depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>\n" +
         "<depends optional-attribute-name=\"Connector\">" + ServiceContainer.REMOTING_OBJECT_NAME +
         "</depends>\n";

      if (defaultTempQueueFullSize != -1)
      {
         config += "<attribute name=\"DefaultTempQueueFullSize\">" + defaultTempQueueFullSize + "</attribute>\n";
      }

      if (defaultTempQueuePageSize != -1)
      {
         config += "<attribute name=\"DefaultTempQueuePageSize\">" + defaultTempQueuePageSize + "</attribute>\n";
      }

      if (defaultTempQueueDownCacheSize != -1)
      {
         config += "<attribute name=\"DefaultTempQueueDownCacheSize\">" + defaultTempQueueDownCacheSize + "</attribute>\n";
      }

      if (prefetchSize != -1)
      {
         config += "<attribute name=\"PrefetchSize\">" + prefetchSize + "</attribute>";
      }
      
      config += "<attribute name=\"SupportsFailover\">" + supportsFailover + "</attribute>";
      config += "<attribute name=\"SupportsLoadBalancing\">" + supportsLoadBalancing + "</attribute>";

      config += "<attribute name=\"JNDIBindings\"><bindings>";

      for(int i = 0; i < jndiBindings.length; i++)
      {
         config += "<binding>" + jndiBindings[i] + "</binding>\n";
      }
      config += "</bindings></attribute></mbean>";

      MBeanConfigurationElement mc = new MBeanConfigurationElement(XMLUtil.stringToElement(config));
      ObjectName on = sc.registerAndConfigureService(mc);
      
      log.trace("Object name is now: " + on);
      
      sc.invoke(on, "create", new Object[0], new String[0]);
      sc.invoke(on, "start", new Object[0], new String[0]);
   }

   public void undeployConnectionFactory(ObjectName objectName) throws Exception
   {
      sc.invoke(objectName, "stop", new Object[0], new String[0]);
      sc.invoke(objectName, "destroy", new Object[0], new String[0]);
      sc.unregisterService(objectName);
   }

   public void configureSecurityForDestination(String destName, String config) throws Exception
   {
      Set s = sc.query(new ObjectName("*:service=Queue,name=" + destName));
      for(Iterator i = s.iterator(); i.hasNext();)
      {
         ObjectName on = (ObjectName)i.next();
         sc.setAttribute(on, "SecurityConfig", config);
      }

      s = sc.query(new ObjectName("*:service=Topic,name=" + destName));
      for(Iterator i = s.iterator(); i.hasNext();)
      {
         ObjectName on = (ObjectName)i.next();
         sc.setAttribute(on, "SecurityConfig", config);
      }
   }

   public void setDefaultSecurityConfig(String config) throws Exception
   {
      sc.setAttribute(serverPeerObjectName, "DefaultSecurityConfig", config);
   }

   public String getDefaultSecurityConfig() throws Exception
   {
      Element element = (Element)sc.getAttribute(serverPeerObjectName, "DefaultSecurityConfig");
      return XMLUtil.elementToString(element);
   }

   public Object executeCommand(Command command) throws Exception
   {
      return command.execute();
   }

   public UserTransaction getUserTransaction() throws Exception
   {
      return sc.getUserTransaction();
   }

   public Set getNodeIDView() throws Exception
   {
      return (Set)sc.getAttribute(postOfficeObjectName, "NodeIDView");
   }

   public List pollNotificationListener(long listenerID) throws Exception
   {
      throw new IllegalStateException("Poll doesn't make sense on a local server. " +
                                      "Register listeners directly instead.");
   }

   public void poisonTheServer(int type) throws Exception
   {
      URL url = this.getClass().getClassLoader().getResource("poison.xml");
      AspectXmlLoader.deployXML(url);

      log.debug(url + " deployed");

//      URL url = this.getClass().getClassLoader().getResource("poison.xml");
//      AspectXmlLoader.undeployXML(url);
      
      PoisonInterceptor.setType(type);
   }   

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected ServiceContainer getServiceContainer()
   {
      return sc;
   }

   protected void overrideServerPeerConfiguration(MBeanConfigurationElement config,
         int serverPeerID, String defaultQueueJNDIContext, String defaultTopicJNDIContext)
      throws Exception
   {
      // overwrite the file configuration, if needed
      config.setConstructorArgumentValue(0, 0, String.valueOf(serverPeerID));

      if (defaultQueueJNDIContext != null)
      {
         config.setConstructorArgumentValue(0, 1, defaultQueueJNDIContext);
      }
      
      if (defaultTopicJNDIContext != null)
      {
         config.setConstructorArgumentValue(0, 2, defaultTopicJNDIContext);
      }
   }

   // Private --------------------------------------------------------------------------------------

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
         sc.setAttribute(on, attrName, attrValue.toString());

      }
   }

   // Inner classes --------------------------------------------------------------------------------

}
