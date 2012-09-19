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

package org.hornetq.core.server.management.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import javax.management.MBeanServer;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.AcceptorControl;
import org.hornetq.api.core.management.BridgeControl;
import org.hornetq.api.core.management.BroadcastGroupControl;
import org.hornetq.api.core.management.ClusterConnectionControl;
import org.hornetq.api.core.management.DivertControl;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.management.impl.AcceptorControlImpl;
import org.hornetq.core.management.impl.AddressControlImpl;
import org.hornetq.core.management.impl.BridgeControlImpl;
import org.hornetq.core.management.impl.BroadcastGroupControlImpl;
import org.hornetq.core.management.impl.ClusterConnectionControlImpl;
import org.hornetq.core.management.impl.DivertControlImpl;
import org.hornetq.core.management.impl.HornetQServerControlImpl;
import org.hornetq.core.management.impl.QueueControlImpl;
import org.hornetq.core.messagecounter.MessageCounter;
import org.hornetq.core.messagecounter.MessageCounterManager;
import org.hornetq.core.messagecounter.impl.MessageCounterManagerImpl;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.*;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationListener;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.spi.core.remoting.Acceptor;
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

   private static final boolean isTrace = HornetQLogger.LOGGER.isTraceEnabled();

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

   private boolean started = false;

   private final boolean messageCounterEnabled;

   private boolean notificationsEnabled;

   private final Set<NotificationListener> listeners = new org.hornetq.utils.ConcurrentHashSet<NotificationListener>();

   private final ObjectNameBuilder objectNameBuilder;

   // Static --------------------------------------------------------

   // Constructor ----------------------------------------------------

   public ManagementServiceImpl(final MBeanServer mbeanServer, final Configuration configuration)
   {
      this.mbeanServer = mbeanServer;
      jmxManagementEnabled = configuration.isJMXManagementEnabled();
      messageCounterEnabled = configuration.isMessageCounterEnabled();
      managementAddress = configuration.getManagementAddress();
      managementNotificationAddress = configuration.getManagementNotificationAddress();

      registry = new ConcurrentHashMap<String, Object>();
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

   public void setStorageManager(final StorageManager storageManager)
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

      messageCounterManager = new MessageCounterManagerImpl(scheduledThreadPool);
      messageCounterManager.setMaxDayCount(configuration.getMessageCounterMaxDayHistory());
      messageCounterManager.reschedule(configuration.getMessageCounterSamplePeriod());

      messagingServerControl = new HornetQServerControlImpl(postOffice,
                                                            configuration,
                                                            resourceManager,
                                                            remotingService,
                                                            messagingServer,
                                                            messageCounterManager,
                                                            storageManager,
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
      AddressControlImpl addressControl = new AddressControlImpl(address,
                                                                 postOffice,
                                                                 pagingManager,
                                                                 storageManager,
                                                                 securityRepository);

      registerInJMX(objectName, addressControl);

      registerInRegistry(ResourceNames.CORE_ADDRESS + address, addressControl);

      if (HornetQLogger.LOGGER.isDebugEnabled())
      {
         HornetQLogger.LOGGER.debug("registered address " + objectName);
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
                                                           storageManager,
                                                           addressSettingsRepository);
      if (messageCounterManager != null)
      {
         MessageCounter counter = new MessageCounter(queue.getName().toString(),
                                                     null,
                                                     queue,
                                                     false,
                                                     queue.isDurable(),
                                                     messageCounterManager.getMaxDayCount());
         queueControl.setMessageCounter(counter);
         messageCounterManager.registerMessageCounter(queue.getName().toString(), counter);
      }
      ObjectName objectName = objectNameBuilder.getQueueObjectName(address, queue.getName());
      registerInJMX(objectName, queueControl);
      registerInRegistry(ResourceNames.CORE_QUEUE + queue.getName(), queueControl);

      if (HornetQLogger.LOGGER.isDebugEnabled())
      {
         HornetQLogger.LOGGER.debug("registered queue " + objectName);
      }
   }

   public synchronized void unregisterQueue(final SimpleString name, final SimpleString address) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getQueueObjectName(address, name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_QUEUE + name);
      messageCounterManager.unregisterMessageCounter(name.toString());
   }

   public synchronized void registerDivert(final Divert divert, final DivertConfiguration config) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getDivertObjectName(divert.getUniqueName().toString());
      DivertControl divertControl = new DivertControlImpl(divert, storageManager, config);
      registerInJMX(objectName, new StandardMBean(divertControl, DivertControl.class));
      registerInRegistry(ResourceNames.CORE_DIVERT + config.getName(), divertControl);

      if (HornetQLogger.LOGGER.isDebugEnabled())
      {
         HornetQLogger.LOGGER.debug("registered divert " + objectName);
      }
   }

   public synchronized void unregisterDivert(final SimpleString name) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getDivertObjectName(name.toString());
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_DIVERT + name);
   }

   public synchronized void registerAcceptor(final Acceptor acceptor, final TransportConfiguration configuration) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getAcceptorObjectName(configuration.getName());
      AcceptorControl control = new AcceptorControlImpl(acceptor, storageManager, configuration);
      registerInJMX(objectName, new StandardMBean(control, AcceptorControl.class));
      registerInRegistry(ResourceNames.CORE_ACCEPTOR + configuration.getName(), control);
   }

   public void unregisterAcceptors()
   {
      List<String> acceptors = new ArrayList<String>();
      synchronized (this)
      {
         for (String resourceName : registry.keySet())
         {
            if (resourceName.startsWith(ResourceNames.CORE_ACCEPTOR))
            {
               acceptors.add(resourceName);
            }
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

   public synchronized void registerBroadcastGroup(final BroadcastGroup broadcastGroup,
                                                   final BroadcastGroupConfiguration configuration) throws Exception
   {
      broadcastGroup.setNotificationService(this);
      ObjectName objectName = objectNameBuilder.getBroadcastGroupObjectName(configuration.getName());
      BroadcastGroupControl control = new BroadcastGroupControlImpl(broadcastGroup, storageManager, configuration);
      registerInJMX(objectName, new StandardMBean(control, BroadcastGroupControl.class));
      registerInRegistry(ResourceNames.CORE_BROADCAST_GROUP + configuration.getName(), control);
   }

   public synchronized void unregisterBroadcastGroup(final String name) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getBroadcastGroupObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_BROADCAST_GROUP + name);
   }

   public synchronized void registerBridge(final Bridge bridge, final BridgeConfiguration configuration) throws Exception
   {
      bridge.setNotificationService(this);
      ObjectName objectName = objectNameBuilder.getBridgeObjectName(configuration.getName());
      BridgeControl control = new BridgeControlImpl(bridge, storageManager, configuration);
      registerInJMX(objectName, new StandardMBean(control, BridgeControl.class));
      registerInRegistry(ResourceNames.CORE_BRIDGE + configuration.getName(), control);
   }

   public synchronized void unregisterBridge(final String name) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getBridgeObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_BRIDGE + name);
   }

   public synchronized void registerCluster(final ClusterConnection cluster,
                                            final ClusterConnectionConfiguration configuration) throws Exception
   {
      ObjectName objectName = objectNameBuilder.getClusterConnectionObjectName(configuration.getName());
      ClusterConnectionControl control = new ClusterConnectionControlImpl(cluster, storageManager, configuration);
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
      ServerMessage reply = new ServerMessageImpl(storageManager.generateUniqueID(), 512);

      String resourceName = message.getStringProperty(ManagementHelper.HDR_RESOURCE_NAME);
      if (HornetQLogger.LOGGER.isDebugEnabled())
      {
         HornetQLogger.LOGGER.debug("handling management message for " + resourceName);
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
            HornetQLogger.LOGGER.managementOperationError(e, operation, resourceName);
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
               
               reply.putBooleanProperty(ManagementHelper.HDR_OPERATION_SUCCEEDED, true);
            }
            catch (Exception e)
            {
               HornetQLogger.LOGGER.managementAttributeError(e, attribute, resourceName);
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

   public synchronized Object getResource(final String resourceName)
   {
      return registry.get(resourceName);
   }

   public synchronized Object[] getResources(final Class<?> resourceType)
   {
      List<Object> resources = new ArrayList<Object>();
      Collection<Object> clone = new ArrayList<Object>(registry.values());
      for (Object entry : clone)
      {
         if (resourceType.isAssignableFrom(entry.getClass()))
         {
            resources.add(entry);
         }
      }
      return resources.toArray(new Object[resources.size()]);
   }

   private final Set<ObjectName> registeredNames = new HashSet<ObjectName>();

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

   public synchronized void unregisterFromRegistry(final String resourceName)
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
               HornetQLogger.LOGGER.managementStopError(unexpectedResourceNames.size(), unexpectedResourceNames);
            }

            for (ObjectName on : registeredNames)
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
      
      listeners.clear();
      
      registry.clear();

      messagingServer = null;

      securityRepository = null;

      addressSettingsRepository = null;

      messagingServerControl = null;

      messageCounterManager = null;

      postOffice = null;
      
      pagingManager = null;
      
      storageManager = null;
      
      messagingServer = null;

      registeredNames.clear();

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   public void sendNotification(final Notification notification) throws Exception
   {
      if (isTrace)
      {
         HornetQLogger.LOGGER.trace("Sending Notification = "  + notification +
                   ", notificationEnabled=" + notificationsEnabled + 
                   " messagingServerControl=" + messagingServerControl);
      }
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
                     HornetQLogger.LOGGER.errorCallingNotifListener(e);
                  }
               }

               // start sending notification *messages* only when server has initialised
               // Note at backup initialisation we don't want to send notifications either
               // https://jira.jboss.org/jira/browse/HORNETQ-317
               if (messagingServer == null || !messagingServer.isActive())
               {
            	  if (HornetQLogger.LOGGER.isDebugEnabled())
            	  {
            	     HornetQLogger.LOGGER.debug("ignoring message " + notification + " as the server is not initialized");
            	  }
                  return;
               }

               long messageID = storageManager.generateUniqueID();

               ServerMessage notificationMessage = new ServerMessageImpl(messageID, 512);

               // Notification messages are always durable so the user can choose whether to add a durable queue to
               // consume them in
               notificationMessage.setDurable(true);
               notificationMessage.setAddress(managementNotificationAddress);

               if (notification.getProperties() != null)
               {
                  TypedProperties props = notification.getProperties();
                  for (SimpleString name : notification.getProperties().getPropertyNames())
                  {
                     notificationMessage.putObjectProperty(name, props.getProperty(name));
                  }
               }

               notificationMessage.putStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE,
                                                  new SimpleString(notification.getType().toString()));

               notificationMessage.putLongProperty(ManagementHelper.HDR_NOTIFICATION_TIMESTAMP, System.currentTimeMillis());

               if (notification.getUID() != null)
               {
                  notificationMessage.putStringProperty(new SimpleString("foobar"),
                                                     new SimpleString(notification.getUID()));
               }

               postOffice.route(notificationMessage, false);
            }
         }
      }
   }

   public void enableNotifications(final boolean enabled)
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
            throw HornetQMessageBundle.BUNDLE.cannotFindResource(resourceName);
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
               throw HornetQMessageBundle.BUNDLE.noGetterMethod(attribute);
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
         throw HornetQMessageBundle.BUNDLE.cannotFindResource(resourceName);
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
               if (paramTypes[i].isAssignableFrom(params[i].getClass()) || paramTypes[i] == Long.TYPE &&
                   params[i].getClass() == Integer.class ||
                   paramTypes[i] == Double.TYPE &&
                   params[i].getClass() == Integer.class ||
                   paramTypes[i] == Long.TYPE &&
                   params[i].getClass() == Long.class ||
                   paramTypes[i] == Double.TYPE &&
                   params[i].getClass() == Double.class ||
                   paramTypes[i] == Integer.TYPE &&
                   params[i].getClass() == Integer.class ||
                   paramTypes[i] == Boolean.TYPE &&
                   params[i].getClass() == Boolean.class)
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
         throw HornetQMessageBundle.BUNDLE.noOperation(operation, params.length);
      }

      Object result = method.invoke(resource, params);

      return result;
   }

   // Inner classes -------------------------------------------------
}
