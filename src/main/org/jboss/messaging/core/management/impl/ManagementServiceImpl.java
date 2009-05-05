/*
 * JBoss, Home of Professional Open Source.
 * 
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors by the
 * 
 * @authors tag. See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.management.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.cluster.ClusterConnectionConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.cluster.DivertConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.AcceptorControlMBean;
import org.jboss.messaging.core.management.BridgeControlMBean;
import org.jboss.messaging.core.management.BroadcastGroupControlMBean;
import org.jboss.messaging.core.management.ClusterConnectionControlMBean;
import org.jboss.messaging.core.management.DiscoveryGroupControlMBean;
import org.jboss.messaging.core.management.DivertControlMBean;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.Notification;
import org.jboss.messaging.core.management.NotificationListener;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.management.ReplicationOperationInvoker;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.management.jmx.impl.ReplicationAwareAddressControlWrapper;
import org.jboss.messaging.core.management.jmx.impl.ReplicationAwareMessagingServerControlWrapper;
import org.jboss.messaging.core.management.jmx.impl.ReplicationAwareQueueControlWrapper;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.MessageCounterManager;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterManagerImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.server.RemotingService;
import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.Divert;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.cluster.Bridge;
import org.jboss.messaging.core.server.cluster.BroadcastGroup;
import org.jboss.messaging.core.server.cluster.ClusterConnection;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.TypedProperties;

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

   private StorageManager storageManager;

   private HierarchicalRepository<Set<Role>> securityRepository;

   private HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private MessagingServerControl messagingServerControl;

   private final MessageCounterManager messageCounterManager;

   private final SimpleString managementNotificationAddress;

   private final SimpleString managementAddress;

   private final String managementClusterPassword;

   private final long managementRequestTimeout;

   private boolean started = false;

   private boolean messageCounterEnabled;

   private boolean notificationsEnabled;

   private final Set<NotificationListener> listeners = new org.jboss.messaging.utils.ConcurrentHashSet<NotificationListener>();

   private ReplicationOperationInvoker replicationInvoker;

   // Constructor ----------------------------------------------------

   public ManagementServiceImpl(final MBeanServer mbeanServer, final Configuration configuration)
   {
      this.mbeanServer = mbeanServer;
      this.jmxManagementEnabled = configuration.isJMXManagementEnabled();
      this.messageCounterEnabled = configuration.isMessageCounterEnabled();
      this.managementAddress = configuration.getManagementAddress();
      this.managementNotificationAddress = configuration.getManagementNotificationAddress();
      this.managementClusterPassword = configuration.getManagementClusterPassword();
      this.managementRequestTimeout = configuration.getManagementRequestTimeout();

      registry = new HashMap<String, Object>();
      broadcaster = new NotificationBroadcasterSupport();
      notificationsEnabled = true;
      messageCounterManager = new MessageCounterManagerImpl();
      messageCounterManager.setMaxDayCount(configuration.getMessageCounterMaxDayHistory());
      messageCounterManager.reschedule(configuration.getMessageCounterSamplePeriod());

      replicationInvoker = new ReplicationOperationInvokerImpl(managementClusterPassword,
                                                               managementAddress,
                                                               managementRequestTimeout);
   }

   // Public --------------------------------------------------------

   // ManagementService implementation -------------------------

   public MessageCounterManager getMessageCounterManager()
   {
      return messageCounterManager;
   }

   public MessagingServerControl registerServer(final PostOffice postOffice,
                                                final StorageManager storageManager,
                                                final Configuration configuration,
                                                final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                                                final HierarchicalRepository<Set<Role>> securityRepository,
                                                final ResourceManager resourceManager,
                                                final RemotingService remotingService,
                                                final MessagingServer messagingServer,
                                                final QueueFactory queueFactory,
                                                final boolean backup) throws Exception
   {
      this.postOffice = postOffice;
      this.addressSettingsRepository = addressSettingsRepository;
      this.securityRepository = securityRepository;
      this.storageManager = storageManager;

      messagingServerControl = new MessagingServerControl(postOffice,
                                                          configuration,
                                                          resourceManager,
                                                          remotingService,
                                                          messagingServer,
                                                          messageCounterManager,
                                                          broadcaster);
      ObjectName objectName = ObjectNames.getMessagingServerObjectName();
      registerInJMX(objectName, new ReplicationAwareMessagingServerControlWrapper(messagingServerControl,
                                                                                  replicationInvoker));
      registerInRegistry(ResourceNames.CORE_SERVER, messagingServerControl);

      return messagingServerControl;
   }

   public synchronized void unregisterServer() throws Exception
   {
      ObjectName objectName = ObjectNames.getMessagingServerObjectName();
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_SERVER);
   }

   public synchronized void registerAddress(final SimpleString address) throws Exception
   {
      ObjectName objectName = ObjectNames.getAddressObjectName(address);
      AddressControl addressControl = new AddressControl(address, postOffice, securityRepository);

      registerInJMX(objectName, new ReplicationAwareAddressControlWrapper(addressControl, replicationInvoker));

      registerInRegistry(ResourceNames.CORE_ADDRESS + address, addressControl);

      if (log.isDebugEnabled())
      {
         log.debug("registered address " + objectName);
      }
   }

   public synchronized void unregisterAddress(final SimpleString address) throws Exception
   {
      ObjectName objectName = ObjectNames.getAddressObjectName(address);

      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_ADDRESS + address);
   }

   public synchronized void registerQueue(final Queue queue,
                                          final SimpleString address,
                                          final StorageManager storageManager) throws Exception
   {
      QueueControl queueControl = new QueueControl(queue, address.toString(), postOffice, addressSettingsRepository);
      MessageCounter counter = new MessageCounter(queue.getName().toString(),
                                                  null,
                                                  queueControl,
                                                  false,
                                                  queue.isDurable(),
                                                  messageCounterManager.getMaxDayCount());
      queueControl.setMessageCounter(counter);
      messageCounterManager.registerMessageCounter(queue.getName().toString(), counter);
      ObjectName objectName = ObjectNames.getQueueObjectName(address, queue.getName());
      registerInJMX(objectName, new ReplicationAwareQueueControlWrapper(queueControl, replicationInvoker));
      registerInRegistry(ResourceNames.CORE_QUEUE + queue.getName(), queueControl);

      if (log.isDebugEnabled())
      {
         log.debug("registered queue " + objectName);
      }
   }

   public synchronized void unregisterQueue(final SimpleString name, final SimpleString address) throws Exception
   {
      ObjectName objectName = ObjectNames.getQueueObjectName(address, name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_QUEUE + name);
      messageCounterManager.unregisterMessageCounter(name.toString());
   }

   public synchronized void registerDivert(Divert divert, DivertConfiguration config) throws Exception
   {
      ObjectName objectName = ObjectNames.getDivertObjectName(divert.getUniqueName());
      DivertControlMBean divertControl = new DivertControl(divert, config);
      registerInJMX(objectName, new StandardMBean(divertControl, DivertControlMBean.class));
      registerInRegistry(ResourceNames.CORE_DIVERT + config.getName(), divertControl);

      if (log.isDebugEnabled())
      {
         log.debug("registered divert " + objectName);
      }
   }

   public synchronized void unregisterDivert(final SimpleString name) throws Exception
   {
      ObjectName objectName = ObjectNames.getDivertObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_DIVERT + name);
   }

   public synchronized void registerAcceptor(final Acceptor acceptor, final TransportConfiguration configuration) throws Exception
   {
      ObjectName objectName = ObjectNames.getAcceptorObjectName(configuration.getName());
      AcceptorControlMBean control = new AcceptorControl(acceptor, configuration);
      registerInJMX(objectName, new StandardMBean(control, AcceptorControlMBean.class));
      registerInRegistry(ResourceNames.CORE_ACCEPTOR + configuration.getName(), control);
   }

   public synchronized void unregisterAcceptor(final String name) throws Exception
   {
      ObjectName objectName = ObjectNames.getAcceptorObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_ACCEPTOR + name);
   }

   public synchronized void registerBroadcastGroup(BroadcastGroup broadcastGroup,
                                                   BroadcastGroupConfiguration configuration) throws Exception
   {
      ObjectName objectName = ObjectNames.getBroadcastGroupObjectName(configuration.getName());
      BroadcastGroupControlMBean control = new BroadcastGroupControl(broadcastGroup, configuration);
      registerInJMX(objectName, new StandardMBean(control, BroadcastGroupControlMBean.class));
      registerInRegistry(ResourceNames.CORE_BROADCAST_GROUP + configuration.getName(), control);
   }

   public synchronized void unregisterBroadcastGroup(String name) throws Exception
   {
      ObjectName objectName = ObjectNames.getBroadcastGroupObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_BROADCAST_GROUP + name);
   }

   public synchronized void registerDiscoveryGroup(DiscoveryGroup discoveryGroup,
                                                   DiscoveryGroupConfiguration configuration) throws Exception
   {
      ObjectName objectName = ObjectNames.getDiscoveryGroupObjectName(configuration.getName());
      DiscoveryGroupControlMBean control = new DiscoveryGroupControl(discoveryGroup, configuration);
      registerInJMX(objectName, new StandardMBean(control, DiscoveryGroupControlMBean.class));
      registerInRegistry(ResourceNames.CORE_DISCOVERY_GROUP + configuration.getName(), control);
   }

   public synchronized void unregisterDiscoveryGroup(String name) throws Exception
   {
      ObjectName objectName = ObjectNames.getDiscoveryGroupObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_DISCOVERY_GROUP + name);
   }

   public synchronized void registerBridge(Bridge bridge, BridgeConfiguration configuration) throws Exception
   {
      ObjectName objectName = ObjectNames.getBridgeObjectName(configuration.getName());
      BridgeControlMBean control = new BridgeControl(bridge, configuration);
      registerInJMX(objectName, new StandardMBean(control, BridgeControlMBean.class));
      registerInRegistry(ResourceNames.CORE_BRIDGE + configuration.getName(), control);
   }

   public synchronized void unregisterBridge(String name) throws Exception
   {
      ObjectName objectName = ObjectNames.getBridgeObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_BRIDGE + name);
   }

   public synchronized void registerCluster(final ClusterConnection cluster,
                                            final ClusterConnectionConfiguration configuration) throws Exception
   {
      ObjectName objectName = ObjectNames.getClusterConnectionObjectName(configuration.getName());
      ClusterConnectionControlMBean control = new ClusterConnectionControl(cluster, configuration);
      registerInJMX(objectName, new StandardMBean(control, ClusterConnectionControlMBean.class));
      registerInRegistry(ResourceNames.CORE_CLUSTER_CONNECTION + configuration.getName(), control);
   }

   public synchronized void unregisterCluster(final String name) throws Exception
   {
      ObjectName objectName = ObjectNames.getClusterConnectionObjectName(name);
      unregisterFromJMX(objectName);
      unregisterFromRegistry(ResourceNames.CORE_CLUSTER_CONNECTION + name);
   }

   public ServerMessage handleMessage(final ServerMessage message) throws Exception
   {
      // a reply message is sent with the result stored in the message body.
      // we set its type to MessageImpl.OBJECT_TYPE so that I can be received
      // as an ObjectMessage when using JMS to send management message
      ServerMessageImpl reply = new ServerMessageImpl(storageManager.generateUniqueID());
      reply.setBody(ChannelBuffers.dynamicBuffer(1024));

      SimpleString resourceName = (SimpleString)message.getProperty(ManagementHelper.HDR_RESOURCE_NAME);
      if (log.isDebugEnabled())
      {
         log.debug("handling management message for " + resourceName);
      }

      SimpleString operation = (SimpleString)message.getProperty(ManagementHelper.HDR_OPERATION_NAME);

      if (operation != null)
      {
         Object[] params = ManagementHelper.retrieveOperationParameters(message);

         if (params == null)
         {
            params = new Object[0];
         }

         try
         {
            Object result = invokeOperation(resourceName.toString(), operation.toString(), params);

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
               ManagementHelper.storeResult(message, exceptionMessage);
            }
         }
      }
      else
      {
         SimpleString attribute = (SimpleString)message.getProperty(ManagementHelper.HDR_ATTRIBUTE);

         if (attribute != null)
         {
            try
            {
               Object result = getAttribute(resourceName.toString(), attribute.toString());

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
                  ManagementHelper.storeResult(message, exceptionMessage);
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

   public String getClusterPassword()
   {
      return managementClusterPassword;
   }

   public long getManagementRequestTimeout()
   {
      return managementRequestTimeout;
   }

   public ReplicationOperationInvoker getReplicationOperationInvoker()
   {
      return replicationInvoker;
   }

   // MessagingComponent implementation -----------------------------

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
         Set<ObjectName> names = mbeanServer.queryNames(ObjectName.getInstance(ObjectNames.DOMAIN + ":*"), null);
         for (ObjectName name : names)
         {
            mbeanServer.unregisterMBean(name);
         }
      }

      messageCounterManager.stop();

      messageCounterManager.resetAllCounters();

      messageCounterManager.resetAllCounterHistories();

      messageCounterManager.clear();

      // replicationInvoker.stop();

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

               // Now send message

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

               notifProps.putStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE,
                                            new SimpleString(notification.getType().toString()));

               notifProps.putLongProperty(ManagementHelper.HDR_NOTIFICATION_TIMESTAMP, System.currentTimeMillis());

               if (notification.getUID() != null)
               {
                  notifProps.putStringProperty(new SimpleString("foobar"), new SimpleString(notification.getUID()));
               }

               notificationMessage.putTypedProperties(notifProps);

               postOffice.route(notificationMessage, null);
            }
         }
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

         try
         {
            method = resource.getClass().getMethod("get" + attribute, new Class[0]);
         }
         catch (NoSuchMethodException nsme)
         {
            try
            {
               method = resource.getClass().getMethod("is" + attribute, new Class[0]);
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
            Class<?>[] paramTypes = m.getParameterTypes();

            for (int i = 0; i < paramTypes.length; i++)
            {
               if (params[i].getClass() != paramTypes[i])
               {
                  continue;
               }
            }

            method = m;

            break;
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
