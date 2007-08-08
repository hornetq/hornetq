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
package org.jboss.test.messaging.tools.container;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.transaction.UserTransaction;

import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.remoting.ServerInvocationHandler;

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
   private Map proxyListeners;

   // Constructors --------------------------------------------------

   public RMITestServer(int index) throws Exception
   {
      namingDelegate = new RMINamingDelegate(index);
      server = new RemoteTestServer(index);
      proxyListeners = new HashMap();
   }

   // Server implementation -----------------------------------------

   public int getServerID()
   {
      return server.getServerID();
   }

   public void start(String containerConfig, boolean clearDatabase) throws Exception
   {
      start(containerConfig, null, clearDatabase, true);
   }

   public void start(String containerConfig, ServiceAttributeOverrides attrOverrides,
                     boolean clearDatabase, boolean startMessagingServer) throws Exception
   {
      server.start(containerConfig, attrOverrides, clearDatabase, startMessagingServer);
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

   public ObjectName deploy(String mbeanConfiguration) throws Exception
   {
      return server.deploy(mbeanConfiguration);
   }

   public void undeploy(ObjectName on) throws Exception
   {
      server.undeploy(on);
   }

   public Object getAttribute(ObjectName on, String attribute) throws Exception
   {
      return server.getAttribute(on, attribute);
   }

   public void setAttribute(ObjectName on, String name, String valueAsString) throws Exception
   {
      server.setAttribute(on, name, valueAsString);
   }

   public Object invoke(ObjectName on, String operationName, Object[] params, String[] signature)
      throws Exception
   {
      return server.invoke(on, operationName, params, signature);
   }

   public void addNotificationListener(ObjectName on, NotificationListener listener)
      throws Exception
   {
      if (!(listener instanceof NotificationListenerID))
      {
         throw new IllegalArgumentException("A RMITestServer can only handle NotificationListenerIDs!");
      }

      long id = ((NotificationListenerID)listener).getID();

      ProxyNotificationListener pl = new ProxyNotificationListener();

      synchronized(proxyListeners)
      {
         proxyListeners.put(new Long(id), pl);
      }

      server.addNotificationListener(on, pl);
   }

   public void removeNotificationListener(ObjectName on, NotificationListener listener)
      throws Exception
   {

      if (!(listener instanceof NotificationListenerID))
      {
         throw new IllegalArgumentException("A RMITestServer can only handle NotificationListenerIDs!");
      }

      long id = ((NotificationListenerID)listener).getID();

      ProxyNotificationListener pl = null;

      synchronized(proxyListeners)
      {
         pl = (ProxyNotificationListener)proxyListeners.remove(new Long(id));
      }

      server.removeNotificationListener(on, pl);
   }

   public Set query(ObjectName pattern) throws Exception
   {
      return server.query(pattern);
   }

   public String getRemotingTransport()
   {
      return server.getRemotingTransport();
   }

   public void log(int level, String text) throws Exception
   {
      server.log(level, text);
   }

   public void startServerPeer(int serverPeerID, String defaultQueueJNDIContext,
                               String defaultTopicJNDIContext, boolean clustered) throws Exception
   {
      startServerPeer(serverPeerID, defaultQueueJNDIContext,
                      defaultTopicJNDIContext, null, clustered);
   }


   public void startServerPeer(int serverPeerID, String defaultQueueJNDIContext,
                               String defaultTopicJNDIContext,
                               ServiceAttributeOverrides attrOverrides, boolean clustered)
      throws Exception
   {
      server.startServerPeer(serverPeerID, defaultQueueJNDIContext,
                             defaultTopicJNDIContext, attrOverrides, clustered);
   }

   public void stopServerPeer() throws Exception
   {
      server.stopServerPeer();
   }

   public boolean isServerPeerStarted() throws Exception
   {
      return server.isServerPeerStarted();
   }

   public ObjectName getServerPeerObjectName() throws Exception
   {
      return server.getServerPeerObjectName();
   }

   public boolean isStarted() throws Exception
   {
      return server.isStarted();
   }

   public Set getConnectorSubsystems() throws Exception
   {
      return server.getConnectorSubsystems();
   }

   public void addServerInvocationHandler(String subsystem, ServerInvocationHandler handler)
      throws Exception
   {
      server.addServerInvocationHandler(subsystem, handler);
   }

   public void removeServerInvocationHandler(String subsystem) throws Exception
   {
      server.removeServerInvocationHandler(subsystem);
   }

   public MessageStore getMessageStore() throws Exception
   {
      return server.getMessageStore();
   }

   public DestinationManager getDestinationManager() throws Exception
   {
      return server.getDestinationManager();
   }

   public PersistenceManager getPersistenceManager() throws Exception
   {
      return server.getPersistenceManager();
   }

   public ServerPeer getServerPeer() throws Exception
   {
      return server.getServerPeer();
   }

   public void deployTopic(String name, String jndiName, boolean clustered) throws Exception
   {
      server.deployTopic(name, jndiName, clustered);
   }

   public void deployTopic(String name,
                           String jndiName,
                           int fullSize,
                           int pageSize,
                           int downCacheSize,
                           boolean clustered) throws Exception
   {
      server.deployTopic(name, jndiName, fullSize, pageSize, downCacheSize, clustered);
   }

   public void deployTopicProgrammatically(String name, String jndiName) throws Exception
   {
      server.deployTopicProgrammatically(name, jndiName);
   }

   public void deployQueue(String name, String jndiName, boolean clustered) throws Exception
   {
      server.deployQueue(name, jndiName, clustered);
   }

   public void deployQueue(String name,
                           String jndiName,
                           int fullSize,
                           int pageSize,
                           int downCacheSize,
                           boolean clustered) throws Exception
   {
      server.deployQueue(name, jndiName, fullSize, pageSize, downCacheSize, clustered);
   }

   public void deployQueueProgrammatically(String name, String jndiName) throws Exception
   {
      server.deployQueueProgrammatically(name, jndiName);
   }

   public void undeployDestination(boolean isQueue, String name) throws Exception
   {
      server.undeployDestination(isQueue, name);
   }

   public boolean undeployDestinationProgrammatically(boolean isQueue, String name) throws Exception
   {
      return server.undeployDestinationProgrammatically(isQueue, name);
   }

   public void deployConnectionFactory(String objectName, String[] jndiBindings)
      throws Exception
   {
      server.deployConnectionFactory(objectName, jndiBindings);
   }

   public void deployConnectionFactory(String objectName, String[] jndiBindings, int prefetchSize)
      throws Exception
   {
      server.deployConnectionFactory(objectName, jndiBindings, prefetchSize);
   }

   public void deployConnectionFactory(String objectName,
                                       String[] jndiBindings,
                                       int prefetchSize,
                                       int defaultTempQueueFullSize,
                                       int defaultTempQueuePageSize,
                                       int defaultTempQueueDownCacheSize) throws Exception
   {
      server.deployConnectionFactory(objectName, jndiBindings, prefetchSize,
                                     defaultTempQueueFullSize, defaultTempQueuePageSize, defaultTempQueueDownCacheSize);
   }
   
   public void deployConnectionFactory(String objectName,
         String[] jndiBindings, boolean supportsFailover, boolean supportsLoadBalancing) throws Exception
   {
   	server.deployConnectionFactory(objectName, jndiBindings, supportsFailover, supportsLoadBalancing);
   }

   public void undeployConnectionFactory(ObjectName objectName) throws Exception
   {
      server.undeployConnectionFactory(objectName);
   }

   public void configureSecurityForDestination(String destName, String config) throws Exception
   {
      server.configureSecurityForDestination(destName, config);
   }

   public void setDefaultSecurityConfig(String config) throws Exception
   {
      server.setDefaultSecurityConfig(config);
   }

   public String getDefaultSecurityConfig() throws Exception
   {
      return server.getDefaultSecurityConfig();
   }

   public Object executeCommand(Command command) throws Exception
   {
      return server.executeCommand(command);
   }

   public UserTransaction getUserTransaction() throws Exception
   {
      return server.getUserTransaction();
   }

   public Set getNodeIDView() throws Exception
   {
      return server.getNodeIDView();
   }
   
   public Map getFailoverMap() throws Exception
   {
   	return server.getFailoverMap();
   }
   
   public Map getRecoveryArea(String queueName) throws Exception
   {
   	return server.getRecoveryArea(queueName);
   }
   
   public int getRecoveryMapSize(String queueName) throws Exception
   {
   	return server.getRecoveryMapSize(queueName);
   }
   
   public List pollNotificationListener(long listenerID) throws Exception
   {
      ProxyNotificationListener pl = null;

      synchronized(proxyListeners)
      {
         pl = (ProxyNotificationListener)proxyListeners.get(new Long(listenerID));
      }

      if (pl == null)
      {
         return Collections.EMPTY_LIST;
      }

      return pl.drain();
   }

   public void poisonTheServer(int type) throws Exception
   {
      server.poisonTheServer(type);
   }
   
   public void flushManagedConnectionPool()
   {
   	server.flushManagedConnectionPool();
   }
   
   public void resetAllSuckers() throws Exception
   {
   	server.resetAllSuckers();
   }   
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private RMINamingDelegate getNamingDelegate()
   {
      return namingDelegate;
   }

   // Inner classes -------------------------------------------------
}
