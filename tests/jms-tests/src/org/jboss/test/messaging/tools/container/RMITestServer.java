/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.test.messaging.tools.container;

import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;
import org.jboss.messaging.jms.server.management.SubscriptionInfo;
import org.jboss.messaging.jms.server.management.TopicControlMBean;

/**
 * An RMI wrapper to access the ServiceContainer from a different address space.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * RMITestServer.java,v 1.1 2006/02/21 08:25:33 timfox Exp
 */
public class RMITestServer extends UnicastRemoteObject implements Server
{
   // Constants -----------------------------------------------------

   public static final String RMI_SERVER_PREFIX = "messaging_rmi_server_";
   public static final String NAMING_SERVER_PREFIX = "naming_rmi_server_";

   public static final int DEFAULT_REGISTRY_PORT = 33777;
   public static final int DEFAULT_SERVER_INDEX = 0;
   public static final String DEFAULT_SERVER_HOST = "localhost";

   private static final long serialVersionUID = -368445344011004778L;
   private static final Logger log = Logger.getLogger(RMITestServer.class);

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      log.debug("initializing RMI runtime");

      String host = System.getProperty("test.bind.address");

      if (host == null)
      {
         host = DEFAULT_SERVER_HOST;
      }

      int serverIndex = DEFAULT_SERVER_INDEX;

      String s = System.getProperty("test.server.index");

      if (s != null)
      {
         serverIndex = Integer.parseInt(s);
      }

      log.info("RMI server " + serverIndex + ", bind address " + host);

      RMITestServer testServer = new RMITestServer(serverIndex);
      log.debug("RMI server " + serverIndex + " created");

      // let RMI know the bind address
      System.setProperty("java.rmi.server.hostname", host);

      Registry registry;

      // try to bind first
      try
      {
         registry = LocateRegistry.getRegistry(DEFAULT_REGISTRY_PORT);
         registry.bind(RMI_SERVER_PREFIX + serverIndex, testServer);
         registry.bind(NAMING_SERVER_PREFIX + serverIndex, testServer.getNamingDelegate());
      }
      catch(Exception e)
      {
         log.info("Failure using an existing registry, trying creating it");

         // try to create it
         registry = LocateRegistry.createRegistry(DEFAULT_REGISTRY_PORT);

         registry.bind(RMI_SERVER_PREFIX + serverIndex, testServer);
         registry.bind(NAMING_SERVER_PREFIX + serverIndex, testServer.getNamingDelegate());
      }

      log.info("RMI server " + serverIndex + " bound");     
   }

   // Attributes ----------------------------------------------------

   protected RemoteTestServer server;
   private RMINamingDelegate namingDelegate;

   // Constructors --------------------------------------------------

   public RMITestServer(int index) throws Exception
   {
      namingDelegate = new RMINamingDelegate(index);
      server = new RemoteTestServer(index);
   }

   // Server implementation -----------------------------------------

   public int getServerID()
   {
      return server.getServerID();
   }


   public void start(String[] containerConfig, HashMap<String, Object> configuration, boolean clearDatabase) throws Exception
   {
      server.start(containerConfig, configuration, clearDatabase);
   }



   public boolean stop() throws Exception
   {
      boolean result = server.stop();
      namingDelegate.reset();
      return result;
   }

   public synchronized void kill() throws Exception
   {
   	log.info("kill() invoked - first deregistering from the rmi registry");

      // unregister myself from the RMI registry

      Registry registry = LocateRegistry.getRegistry(DEFAULT_REGISTRY_PORT);

      String name = RMI_SERVER_PREFIX + server.getServerID();
      registry.unbind(name);
      log.info("unregistered " + name + " from registry");

      // unregister myself from the RMI registry

      name = NAMING_SERVER_PREFIX + server.getServerID();
      registry.unbind(name);
      log.info("unregistered " + name + " from registry");

      log.info("Killing VM!!!!");
      
      Runtime.getRuntime().halt(1);
   }

   public void ping() throws Exception
   {
      //noop - nothing to be done
   }

   public void log(int level, String text) throws Exception
   {
      server.log(level, text);
   }

   public void startServerPeer(int serverPeerID) throws Exception
   {
      server.startServerPeer(serverPeerID);
   }

   public void stopServerPeer() throws Exception
   {
      server.stopServerPeer();
   }

   public boolean isStarted() throws Exception
   {
      return server.isStarted();
   }

   public MessagingServer getServerPeer() throws Exception
   {
      return server.getServerPeer();
   }

   public void destroyQueue(String name, String jndiName) throws Exception
   {
      server.destroyQueue(name, jndiName);
   }
   
   public void destroyTopic(String name, String jndiName) throws Exception
   {
      server.destroyTopic(name, jndiName);
   }
   
   public void createQueue(String name, String jndiName) throws Exception
   {
      server.createQueue(name, jndiName);
   }
   
   public void createTopic(String name, String jndiName) throws Exception
   {
      server.createTopic(name, jndiName);
   }

   public void deployConnectionFactory(String objectName, List<String> jndiBindings)
      throws Exception
   {
      server.deployConnectionFactory(objectName, jndiBindings);
   }

   public void deployConnectionFactory(String clientID, String objectName, List<String> jndiBindings) throws Exception
   {
      server.deployConnectionFactory(clientID, objectName, jndiBindings);
   }

   public void deployConnectionFactory(String objectName, List<String> jndiBindings, int prefetchSize)
      throws Exception
   {
      server.deployConnectionFactory(objectName, jndiBindings, prefetchSize);
   }

   public void deployConnectionFactory(String clientId,
                                        String objectName,
                                        List<String> jndiBindings,
                                        int prefetchSize,
                                        int defaultTempQueueFullSize,
                                        int defaultTempQueuePageSize,
                                        int defaultTempQueueDownCacheSize,
                                        boolean supportsFailover,
                                        boolean supportsLoadBalancing,
                                        int dupsOkBatchSize,
                                        boolean blockOnAcknowledge) throws Exception
   {
      server.deployConnectionFactory(clientId, objectName, jndiBindings, prefetchSize, defaultTempQueueFullSize, defaultTempQueuePageSize, defaultTempQueueDownCacheSize,
              supportsFailover, supportsLoadBalancing, dupsOkBatchSize, blockOnAcknowledge);  
   }
    public void deployConnectionFactory(String objectName,
                                       List<String> jndiBindings,
                                       int prefetchSize,
                                       int defaultTempQueueFullSize,
                                       int defaultTempQueuePageSize,
                                       int defaultTempQueueDownCacheSize) throws Exception
   {
      server.deployConnectionFactory(objectName, jndiBindings, prefetchSize,
                                     defaultTempQueueFullSize, defaultTempQueuePageSize, defaultTempQueueDownCacheSize);
   }
   
   public void deployConnectionFactory(String objectName,
         List<String> jndiBindings, boolean supportsFailover, boolean supportsLoadBalancing) throws Exception
   {
   	server.deployConnectionFactory(objectName, jndiBindings, supportsFailover, supportsLoadBalancing);
   }

   public void undeployConnectionFactory(String objectName) throws Exception
   {
      server.undeployConnectionFactory(objectName);
   }

   public void configureSecurityForDestination(String destName, boolean isQueue, Set<Role> roles) throws Exception
   {
      server.configureSecurityForDestination(destName, isQueue, roles);
   }
 
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private RMINamingDelegate getNamingDelegate()
   {
      return namingDelegate;
   }

   public MessagingServer getMessagingServer() throws Exception
   {
      return server.getMessagingServer();
   }

   public JMSServerManager getJMSServerManager()
   {
      return server.getJMSServerManager();
   }

   public void removeAllMessages(String destination, boolean isQueue) throws Exception
   {
      server.removeAllMessages(destination, isQueue);
   }

   public Integer getMessageCountForQueue(String queueName) throws Exception
   {
      ObjectName objectName = ObjectNames.getJMSQueueObjectName(queueName);
      JMSQueueControlMBean queue = (JMSQueueControlMBean) MBeanServerInvocationHandler.newProxyInstance(
            ManagementFactory.getPlatformMBeanServer(), objectName, JMSQueueControlMBean.class, false);
      return queue.getMessageCount();
   }


   public List<String> listAllSubscribersForTopic(String s) throws Exception
   {
      ObjectName objectName = ObjectNames.getJMSTopicObjectName(s);
      TopicControlMBean topic = (TopicControlMBean) MBeanServerInvocationHandler.newProxyInstance(
            ManagementFactory.getPlatformMBeanServer(), objectName, TopicControlMBean.class, false);
      SubscriptionInfo[] subInfos = topic.listAllSubscriptionInfos();
      List<String> subs = new ArrayList<String>();
      for (SubscriptionInfo info : subInfos)
      {
         subs.add(info.getName());
      }
      return subs;
   }


   public Set<Role> getSecurityConfig() throws Exception
   {
      return server.getSecurityConfig();
   }

   public void setSecurityConfig(Set<Role> defConfig) throws Exception
   {
      server.setSecurityConfig(defConfig);
   }

   public InitialContext getInitialContext() throws Exception
   {
      Hashtable env = new Hashtable();
      env.put("java.naming.factory.initial",
              "org.jboss.test.messaging.tools.container.RemoteInitialContextFactory");
      env.put("java.naming.provider.url", "");
      env.put("java.naming.factory.url.pkgs", "");
      env.put(Constants.SERVER_INDEX_PROPERTY_NAME, ""+getServerID());

      return new InitialContext(env);
   }
   

   // Inner classes -------------------------------------------------
}
