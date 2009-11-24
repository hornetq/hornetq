/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.management.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.BridgeConfiguration;
import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.config.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.AcceptorControl;
import org.hornetq.core.management.BridgeControl;
import org.hornetq.core.management.BroadcastGroupControl;
import org.hornetq.core.management.ClusterConnectionControl;
import org.hornetq.core.management.DiscoveryGroupControl;
import org.hornetq.core.management.DivertControl;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.management.Notification;
import org.hornetq.core.management.NotificationListener;
import org.hornetq.core.management.ObjectNameBuilder;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.messagecounter.MessageCounter;
import org.hornetq.core.messagecounter.MessageCounterManager;
import org.hornetq.core.messagecounter.impl.MessageCounterManagerImpl;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.remoting.spi.Acceptor;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.Divert;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:fox@redhat.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ManagementServiceImpl implements ManagementService
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ManagementServiceImpl.class);

   private final MBeanServer mbeanServer;

   private final boolean jmxManagementEnabled;

   private final Map<String, Object> registry;

   private final NotificationBroadcasterSupport broadcaster;

   private PostOffice postOffice;

   private PagingManager pagingManager;

   private StorageManager storageManager;

   private HornetQServer messagingServer;

   private HierarchicalRepository<Set<Role>> securityRepository;

   private HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private HornetQServerControlImpl messagingServerControl;

   private MessageCounterManager messageCounterManager;

   private final SimpleString managementNotificationAddress;

   private final SimpleString managementAddress;

   private final String managementClusterUser;

   private final String managementClusterPassword;

   private final long managementRequestTimeout;

   private boolean started = false;

   private boolean messageCounterEnabled;

   private boolean notificationsEnabled;

   private final Set<NotificationListener> listeners = new org.hornetq.utils.ConcurrentHashSet<NotificationListener>();

   private final ObjectNameBuilder objectNameBuilder;

   // Static --------------------------------------------------------

   private static void checkDefaultManagementClusterCredentials(String user, String password)
   {
      if (ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_USER.equals(user) && ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_PASSWORD.equals(password))
      {
         log.warn("It has been detected that the cluster admin user and password which are used to " + "replicate management operation from one node to the other have not been changed from the installation default. "
                  + "Please see the HornetQ user guide for instructions on how to do this.");
      }
   }

   // Constructor ----------------------------------------------------

   public ManagementServiceImpl(final MBeanServer mbeanServer,
                                final Configuration configuration)
   {
      this.mbeanServer = mbeanServer;
      this.jmxManagementEnabled = configuration.isJMXManagementEnabled();
      this.messageCounterEnabled = configuration.isMessageCounterEnabled();
      this.managementAddress = configuration.getManagementAddress();
      this.managementNotificationAddress = configuration.getManagementNotificationAddress();
      this.managementClusterUser = configuration.getManagementClusterUser();
      this.managementClusterPassword = configuration.getManagementClusterPassword();
      this.managementRequestTimeout = configuration.getManagementRequestTimeout();

      checkDefaultManagementClusterCredentials(managementClusterUser, managementClusterPassword);

      registry = new HashMap<String, Object>();
      broadcaster = new NotificationBroadcasterSupport();
      notificationsEnabled = true;
      objectNameBuilder = ObjectNameBuilder.create(configuration.getJMXDomain());
   }

   // Public --------------------------------------------------------

   // ManagementService implementation -------------------------

   public ObjectNameBuilder getObjectNameBuilder()
   {
      return objectNameBuilder;
   }
   
   public MessageCounterManager getMessageCounterManager()
   {
      return messageCounterManager;
   }
   
   public void setStorageManager(StorageManager storageManager)
   {
      this.storageManager = storageManager;
   }

   public HornetQServerControlImpl registerServer(final PostOffice postOffice,
                                                  final StorageManager storageManager,
                                                  final Configuration configuration,
                                                  final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                                  final HierarchicalRepository<Set<Role>> securityRepository,
                                                  final ResourceManager resourceManager,
                                                  final RemotingService remotingService,
                                                  final HornetQServer messagingServer,
                                                  final QueueFactory queueFactory,
                                                  final ScheduledExecutorService scheduledThreadPool,
                                                  final PagingManager pagingManager, 
                                                  final boolean backup) throws Exception
   {
      this.postOffice = postOffice;
      this.addressSettingsRepository = addressSettingsRepository;
      this.securityRepository = securityRepository;
      this.storageManager = storageManager;
      this.messagingServer = messagingServer;
      this.pagingManager = pagingManager;

      this.messageCounterManager = new MessageCounterManagerImpl(scheduledThreadPool);
      messageCounterManager.setMaxDayCount(configuration.getMessageCounterMaxDayHistory());
      messageCounterManager.reschedule(configuration.getMessageCounterSamplePeriod());

      messagingServerControl = new HornetQServerControlImpl(postOffice,
                                                            configuration,
                                                            resourceManager,
                                                            remotingService,
                                                            messagingServer,
                                                            messageCounterManager,
                                                            broadcaster);
      ObjectName objectName = objectNameBuilder.getHornetQServerObjectName();
      registerInJMX(objectName, messagingServerControl);
      registerInRegistry(ResourceNames.CORE_SERVER, messagingServerControl);

      return messagingServerControl;
   }

   public synchronized void unregisterServer() throws Exception
   {
      ObjectName objectName = objectNameBuilder.getHornetQServerObjectName();
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_SERVER);
   }

   public synchronized void registerAddress(final SimpleString address) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getAddressObjectName(address);
      AddressControlImpl addressControl = new AddressControlImpl(address, postOffice, pagingManager, securityRepository);

      registerInJMX(objectName, addressControl);

      registerInRegistry(ResourceNames.CORE_ADDRESS + address, addressControl);

      if (log.isDebugEnabled())
      {
         log.debug("registered address " + objectName);
      }
   }

   public synchronized void unregisterAddress(final SimpleString address) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getAddressObjectName(address);

      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_ADDRESS + address);
   }

   public synchronized void registerQueue(final Queue queue,
                                          final SimpleString address,
                                          final StorageManager storageManager) throws Exception
   {
      QueueControlImpl queueControl = new QueueControlImpl(queue,
                                                           address.toString(),
                                                           postOffice,
                                                           addressSettingsRepository);
      if (messageCounterManager != null)
      {
         MessageCounter counter = new MessageCounter(queue.getName().toString(),
                                                     null,
                                                     queueControl,
                                                     false,
                                                     queue.isDurable(),
                                                     messageCounterManager.getMaxDayCount());
         queueControl.setMessageCounter(counter);
         messageCounterManager.registerMessageCounter(queue.getName().toString(), counter);
      }
      ObjectName objectName = objectNameBuilder.getQueueObjectName(address, queue.getName());
      registerInJMX(objectName, queueControl);
      registerInRegistry(ResourceNames.CORE_QUEUE + queue.getName(), queueControl);

      if (log.isDebugEnabled())
      {
         log.debug("registered queue " + objectName);
      }
   }

   public synchronized void unregisterQueue(final SimpleString name, final SimpleString address) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getQueueObjectName(address, name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_QUEUE + name);
      messageCounterManager.unregisterMessageCounter(name.toString());
   }

   public synchronized void registerDivert(Divert divert, DivertConfiguration config) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getDivertObjectName(divert.getUniqueName());
      DivertControl divertControl = new DivertControlImpl(divert, config);
      registerInJMX(objectName, new StandardMBean(divertControl, DivertControl.class));
      registerInRegistry(ResourceNames.CORE_DIVERT + config.getName(), divertControl);

      if (log.isDebugEnabled())
      {
         log.debug("registered divert " + objectName);
      }
   }

   public synchronized void unregisterDivert(final SimpleString name) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getDivertObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_DIVERT + name);
   }

   public synchronized void registerAcceptor(final Acceptor acceptor, final TransportConfiguration configuration) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getAcceptorObjectName(configuration.getName());
      AcceptorControl control = new AcceptorControlImpl(acceptor, configuration);
      registerInJMX(objectName, new StandardMBean(control, AcceptorControl.class));
      registerInRegistry(ResourceNames.CORE_ACCEPTOR + configuration.getName(), control);
   }

   public void unregisterAcceptors()
   {
      List<String> acceptors = new ArrayList<String>();
      for (String resourceName : registry.keySet())
      {
         if (resourceName.startsWith(ResourceNames.CORE_ACCEPTOR))
         {
            acceptors.add(resourceName);
         }
      }

      for (String acceptor : acceptors)
      {
         String name = acceptor.substring(ResourceNames.CORE_ACCEPTOR.length());
         try
         {
            unregisterAcceptor(name);
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }

   public synchronized void unregisterAcceptor(final String name) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getAcceptorObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_ACCEPTOR + name);
   }

   public synchronized void registerBroadcastGroup(BroadcastGroup broadcastGroup,
                                                   BroadcastGroupConfiguration configuration) throws Exception
   {
      broadcastGroup.setNotificationService(this);
      ObjectName objectName = objectNameBuilder.getBroadcastGroupObjectName(configuration.getName());
      BroadcastGroupControl control = new BroadcastGroupControlImpl(broadcastGroup, configuration);
      registerInJMX(objectName, new StandardMBean(control, BroadcastGroupControl.class));
      registerInRegistry(ResourceNames.CORE_BROADCAST_GROUP + configuration.getName(), control);
   }

   public synchronized void unregisterBroadcastGroup(String name) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getBroadcastGroupObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_BROADCAST_GROUP + name);
   }

   public synchronized void registerDiscoveryGroup(DiscoveryGroup discoveryGroup,
                                                   DiscoveryGroupConfiguration configuration) throws Exception
   {
      discoveryGroup.setNotificationService(this);
      ObjectName objectName = objectNameBuilder.getDiscoveryGroupObjectName(configuration.getName());
      DiscoveryGroupControl control = new DiscoveryGroupControlImpl(discoveryGroup, configuration);
      registerInJMX(objectName, new StandardMBean(control, DiscoveryGroupControl.class));
      registerInRegistry(ResourceNames.CORE_DISCOVERY_GROUP + configuration.getName(), control);
   }

   public synchronized void unregisterDiscoveryGroup(String name) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getDiscoveryGroupObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_DISCOVERY_GROUP + name);
   }

   public synchronized void registerBridge(Bridge bridge, BridgeConfiguration configuration) throws Exception
   {
      bridge.setNotificationService(this);
      ObjectName objectName = objectNameBuilder.getBridgeObjectName(configuration.getName());
      BridgeControl control = new BridgeControlImpl(bridge, configuration);
      registerInJMX(objectName, new StandardMBean(control, BridgeControl.class));
      registerInRegistry(ResourceNames.CORE_BRIDGE + configuration.getName(), control);
   }

   public synchronized void unregisterBridge(String name) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getBridgeObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_BRIDGE + name);
   }

   public synchronized void registerCluster(final ClusterConnection cluster,
                                            final ClusterConnectionConfiguration configuration) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getClusterConnectionObjectName(configuration.getName());
      ClusterConnectionControl control = new ClusterConnectionControlImpl(cluster, configuration);
      registerInJMX(objectName, new StandardMBean(control, ClusterConnectionControl.class));
      registerInRegistry(ResourceNames.CORE_CLUSTER_CONNECTION + configuration.getName(), control);
   }

   public synchronized void unregisterCluster(final String name) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getClusterConnectionObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_CLUSTER_CONNECTION + name);
   }

   public ServerMessage handleMessage(final ServerMessage message) throws Exception
   {
      // a reply message is sent with the result stored in the message body.
      ServerMessageImpl reply = new ServerMessageImpl(storageManager.generateUniqueID());
      reply.setBody(ChannelBuffers.dynamicBuffer(1024));

      String resourceName = message.getStringProperty(ManagementHelper.HDR_RESOURCE_NAME);
      if (log.isDebugEnabled())
      {
         log.debug("handling management message for " + resourceName);
      }

      String operation = message.getStringProperty(ManagementHelper.HDR_OPERATION_NAME);

      if (operation != null)
      {
         Object[] params = ManagementHelper.retrieveOperationParameters(message);

         if (params == null)
         {
            params = new Object[0];
         }

         try
         {
            Object result = invokeOperation(resourceName, operation, params);

            ManagementHelper.storeResult(reply, result);

            reply.putBooleanProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED, true);
         }
         catch (Exception e)
         {
            log.warn("exception while invoking " + operation + " on " + resourceName, e);
            reply.putBooleanProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED, false);
            String exceptionMessage = e.getMessage();
            if (e instanceof InvocationTargetException)
            {
               exceptionMessage = ((InvocationTargetException)e).getTargetException().getMessage();
            }
            if (e != null)
            {
               ManagementHelper.storeResult(reply, exceptionMessage);
            }
         }
      }
      else
      {
         String attribute = message.getStringProperty(ManagementHelper.HDR_ATTRIBUTE);

         if (attribute != null)
         {
            try
            {
               Object result = getAttribute(resourceName, attribute);

               ManagementHelper.storeResult(reply, result);
            }
            catch (Exception e)
            {
               log.warn("exception while retrieving attribute " + attribute + " on " + resourceName, e);
               reply.putBooleanProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED, false);
               String exceptionMessage = e.getMessage();
               if (e instanceof InvocationTargetException)
               {
                  exceptionMessage = ((InvocationTargetException)e).getTargetException().getMessage();
               }
               if (e != null)
               {
                  ManagementHelper.storeResult(reply, exceptionMessage);
               }
            }
         }
      }

      return reply;
   }

   public Object getResource(final String resourceName)
   {
      return registry.get(resourceName);
   }
   
   public Object[] getResources(Class<?> resourceType)
   {
      List<Object> resources = new ArrayList<Object>();
      for (Object entry : registry.values())
      {
         if (resourceType.isAssignableFrom(entry.getClass()))
         {
            resources.add(entry);
         }
      }
      return (Object[])resources.toArray(new Object[resources.size()]);
   }

   private Set<ObjectName> registeredNames = new HashSet<ObjectName>();

   public void registerInJMX(final ObjectName objectName, final Object managedResource) throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }

      synchronized (mbeanServer)
      {
         unregisterFromJMX(objectName);

         mbeanServer.registerMBean(managedResource, objectName);

         registeredNames.add(objectName);
      }
   }

   public synchronized void registerInRegistry(final String resourceName, final Object managedResource)
   {
      unregisterFromRegistry(resourceName);

      registry.put(resourceName, managedResource);
   }

   public void unregisterFromRegistry(final String resourceName)
   {
      registry.remove(resourceName);
   }

   // the JMX unregistration is synchronized to avoid race conditions if 2 clients tries to
   // unregister the same resource (e.g. a queue) at the same time since unregisterMBean()
   // will throw an exception if the MBean has already been unregistered
   public void unregisterFromJMX(final ObjectName objectName) throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }

      synchronized (mbeanServer)
      {
         if (mbeanServer.isRegistered(objectName))
         {
            mbeanServer.unregisterMBean(objectName);

            registeredNames.remove(objectName);
         }
      }
   }

   public void addNotificationListener(final NotificationListener listener)
   {
      listeners.add(listener);
   }

   public void removeNotificationListener(final NotificationListener listener)
   {
      listeners.remove(listener);
   }

   public SimpleString getManagementAddress()
   {
      return managementAddress;
   }

   public SimpleString getManagementNotificationAddress()
   {
      return managementNotificationAddress;
   }

   public String getClusterUser()
   {
      return managementClusterUser;
   }

   public String getClusterPassword()
   {
      return managementClusterPassword;
   }

   // HornetQComponent implementation -----------------------------

   public void start() throws Exception
   {
      if (messageCounterEnabled)
      {
         messageCounterManager.start();
      }

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      Set<String> resourceNames = new HashSet<String>(registry.keySet());

      for (String resourceName : resourceNames)
      {
         unregisterFromRegistry(resourceName);
      }

      if (jmxManagementEnabled)
      {
         if (!registeredNames.isEmpty())
         {
            List<String> unexpectedResourceNames = new ArrayList<String>();
            for (String name : resourceNames)
            {
               // only addresses and queues should still be registered
               if (!(name.startsWith(ResourceNames.CORE_ADDRESS) || name.startsWith(ResourceNames.CORE_QUEUE)))
               {
                  unexpectedResourceNames.add(name);
               }
            }
            if (!unexpectedResourceNames.isEmpty())
            {
               log.warn("On ManagementService stop, there are " + unexpectedResourceNames.size() +
                        " unexpected registered MBeans: " + unexpectedResourceNames);
            }

            for (ObjectName on : this.registeredNames)
            {
               try
               {
                  mbeanServer.unregisterMBean(on);
               }
               catch (Exception ignore)
               {
               }
            }
         }
      }

      if (messageCounterManager != null)
      {
         messageCounterManager.stop();

         messageCounterManager.resetAllCounters();

         messageCounterManager.resetAllCounterHistories();

         messageCounterManager.clear();
      }

      registeredNames.clear();

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   public void sendNotification(final Notification notification) throws Exception
   {
      if (messagingServerControl != null && notificationsEnabled)
      {
         // This needs to be synchronized since we need to ensure notifications are processed in strict sequence
         synchronized (this)
         {
            // We also need to synchronize on the post office notification lock
            // otherwise we can get notifications arriving in wrong order / missing
            // if a notification occurs at same time as sendQueueInfoToQueue is processed
            synchronized (postOffice.getNotificationLock())
            {

               // First send to any local listeners
               for (NotificationListener listener : listeners)
               {
                  try
                  {
                     listener.onNotification(notification);
                  }
                  catch (Exception e)
                  {
                     // Exception thrown from one listener should not stop execution of others
                     log.error("Failed to call listener", e);
                  }
               }

               // start sending notification *messages* only when the *remoting service* if started
               if (messagingServer == null || 
                   !messagingServer.getRemotingService().isStarted())
               {
                  return;
               }

               ServerMessage notificationMessage = new ServerMessageImpl(storageManager.generateUniqueID());

               notificationMessage.setBody(ChannelBuffers.EMPTY_BUFFER);
               // Notification messages are always durable so the user can choose whether to add a durable queue to
               // consume
               // them in
               notificationMessage.setDurable(true);
               notificationMessage.setDestination(managementNotificationAddress);

               TypedProperties notifProps;
               if (notification.getProperties() != null)
               {
                  notifProps = new TypedProperties(notification.getProperties());
               }
               else
               {
                  notifProps = new TypedProperties();
               }

               notifProps.putSimpleStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE,
                                            new SimpleString(notification.getType().toString()));

               notifProps.putLongProperty(ManagementHelper.HDR_NOTIFICATION_TIMESTAMP, System.currentTimeMillis());

               if (notification.getUID() != null)
               {
                  notifProps.putSimpleStringProperty(new SimpleString("foobar"), new SimpleString(notification.getUID()));
               }

               notificationMessage.putTypedProperties(notifProps);

               postOffice.route(notificationMessage);
            }
         }
      }
      
      if (storageManager != null)
      {
         storageManager.waitOnOperations(managementRequestTimeout);
         storageManager.clearContext();
      }
   }

   public void enableNotifications(boolean enabled)
   {
      notificationsEnabled = enabled;
   }

   public Object getAttribute(final String resourceName, final String attribute)
   {
      try
      {
         Object resource = registry.get(resourceName);

         if (resource == null)
         {
            throw new IllegalArgumentException("Cannot find resource with name " + resourceName);
         }

         Method method = null;

         String upperCaseAttribute = attribute.substring(0, 1).toUpperCase() + attribute.substring(1);
         try
         {
            method = resource.getClass().getMethod("get" + upperCaseAttribute, new Class[0]);
         }
         catch (NoSuchMethodException nsme)
         {
            try
            {
               method = resource.getClass().getMethod("is" + upperCaseAttribute, new Class[0]);
            }
            catch (NoSuchMethodException nsme2)
            {
               throw new IllegalArgumentException("no getter method for " + attribute);
            }
         }
         return method.invoke(resource, new Object[0]);
      }
      catch (Throwable t)
      {
         throw new IllegalStateException("Problem while retrieving attribute " + attribute, t);
      }
   }

   private Object invokeOperation(final String resourceName, final String operation, final Object[] params) throws Exception
   {
      Object resource = registry.get(resourceName);

      if (resource == null)
      {
         throw new IllegalArgumentException("Cannot find resource with name " + resourceName);
      }

      Method method = null;

      Method[] methods = resource.getClass().getMethods();
      for (Method m : methods)
      {
         if (m.getName().equals(operation) && m.getParameterTypes().length == params.length)
         {
            boolean match = true;

            Class<?>[] paramTypes = m.getParameterTypes();

            for (int i = 0; i < paramTypes.length; i++)
            {
               if (params[i] == null)
               {
                  continue;
               }
               if (paramTypes[i].isAssignableFrom(params[i].getClass()) || (paramTypes[i] == Long.TYPE && params[i].getClass() == Integer.class) ||
                   (paramTypes[i] == Double.TYPE && params[i].getClass() == Integer.class) ||
                   (paramTypes[i] == Long.TYPE && params[i].getClass() == Long.class) ||
                   (paramTypes[i] == Double.TYPE && params[i].getClass() == Double.class) ||
                   (paramTypes[i] == Integer.TYPE && params[i].getClass() == Integer.class) ||
                   (paramTypes[i] == Boolean.TYPE && params[i].getClass() == Boolean.class))
               {
                  // parameter match
               }
               else
               {
                  match = false;
                  break; // parameter check loop
               }
            }

            if (match)
            {
               method = m;
               break; // method match loop
            }
         }
      }

      if (method == null)
      {
         throw new IllegalArgumentException("no operation " + operation + "/" + params.length);
      }

      Object result = method.invoke(resource, params);

      return result;
   }

   // Inner classes -------------------------------------------------
}
