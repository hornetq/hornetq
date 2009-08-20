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

package org.hornetq.core.server.cluster.impl;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.SendAcknowledgementHandler;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionImpl;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.Notification;
import org.hornetq.core.management.NotificationService;
import org.hornetq.core.management.NotificationType;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.message.Message;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.replication.ReplicateAcknowledgeMessage;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.MessageFlowRecord;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.utils.Future;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;

/**
 * A Core BridgeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 12 Nov 2008 11:37:35
 *
 *
 */
public class BridgeImpl implements Bridge, FailureListener, SendAcknowledgementHandler
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(BridgeImpl.class);

   // Attributes ----------------------------------------------------

   private final UUID nodeUUID;

   private final SimpleString name;

   private Queue queue;

   private final Executor executor;

   private final Filter filter;

   private final SimpleString forwardingAddress;

   private final java.util.Queue<MessageReference> refs = new LinkedList<MessageReference>();

   private final Transformer transformer;

   private volatile ClientSessionFactory csf;

   private volatile ClientSessionInternal session;

   private volatile ClientProducer producer;

   private volatile boolean started;

   private final boolean useDuplicateDetection;

   private volatile boolean active;

   private final Pair<TransportConfiguration, TransportConfiguration> connectorPair;

   private final String discoveryAddress;

   private final int discoveryPort;

   private final long retryInterval;

   private final double retryIntervalMultiplier;

   private final int reconnectAttempts;
   
   private final boolean failoverOnServerShutdown;

   private final SimpleString idsHeaderName;

   private MessageFlowRecord flowRecord;

   private final SimpleString managementAddress;

   private final SimpleString managementNotificationAddress;

   private final String clusterUser;

   private final String clusterPassword;

   private Channel replicatingChannel;

   private boolean activated;

   private NotificationService notificationService;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * discoveryAddress (+ discoveryPort) and connectorPair are mutually exclusive.
    * If discoveryAddress is != null, it will be used to create the bridge's client session factory.
    * Otherwise, the connectorPair will be used
    */
   public BridgeImpl(final UUID nodeUUID,
                     final SimpleString name,
                     final Queue queue,
                     final String discoveryAddress,
                     final int discoveryPort,
                     final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                     final Executor executor,
                     final SimpleString filterString,
                     final SimpleString forwardingAddress,
                     final ScheduledExecutorService scheduledExecutor,
                     final Transformer transformer,
                     final long retryInterval,
                     final double retryIntervalMultiplier,
                     final int reconnectAttempts,
                     final boolean failoverOnServerShutdown,
                     final boolean useDuplicateDetection,
                     final SimpleString managementAddress,
                     final SimpleString managementNotificationAddress,
                     final String clusterUser,
                     final String clusterPassword,
                     final MessageFlowRecord flowRecord,
                     final Channel replicatingChannel,
                     final boolean activated,
                     final StorageManager storageManager) throws Exception
   {
      this.nodeUUID = nodeUUID;

      this.name = name;

      this.queue = queue;

      this.executor = executor;

      this.filter = FilterImpl.createFilter(filterString);

      this.forwardingAddress = forwardingAddress;

      this.transformer = transformer;

      this.useDuplicateDetection = useDuplicateDetection;

      this.discoveryAddress = discoveryAddress;
      
      this.discoveryPort = discoveryPort;
      
      this.connectorPair = connectorPair;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.reconnectAttempts = reconnectAttempts;

      this.failoverOnServerShutdown = failoverOnServerShutdown;

      this.idsHeaderName = MessageImpl.HDR_ROUTE_TO_IDS.concat(name);

      this.managementAddress = managementAddress;

      this.managementNotificationAddress = managementNotificationAddress;

      this.clusterUser = clusterUser;

      this.clusterPassword = clusterPassword;

      this.flowRecord = flowRecord;

      this.replicatingChannel = replicatingChannel;

      this.activated = activated;  
   }

   public void setNotificationService(final NotificationService notificationService)
   {
      this.notificationService = notificationService;
   }
   
   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }
      
      started = true;

      if (activated)
      {
         executor.execute(new CreateObjectsRunnable());
      }
      
      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), NotificationType.BRIDGE_STARTED, props);
         notificationService.sendNotification(notification );
      }
   }

   private void cancelRefs() throws Exception
   {
      MessageReference ref;

      LinkedList<MessageReference> list = new LinkedList<MessageReference>();

      while ((ref = refs.poll()) != null)
      {
         list.addFirst(ref);
      }

      for (MessageReference ref2 : list)
      {
         ref2.getQueue().cancel(ref2);
      }
   }

   public void stop() throws Exception
   {
      log.info("Stopping bridge " + name);
      
      if (started)
      {
         // We need to stop the csf here otherwise the stop runnable never runs since the createobjectsrunnable is
         // trying to connect to the target
         // server which isn't up in an infinite loop
         if (csf != null)
         {
            csf.close();
         }
      }
      
      executor.execute(new StopRunnable());
           
      waitForRunnablesToComplete();
      
      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(), NotificationType.BRIDGE_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification );
         }
         catch (Exception e)
         {
            log.warn("unable to send notification when broadcast group is stopped", e);
         }
      }
      log.info("Stopped bridge " + name);
      
   }

   public boolean isStarted()
   {
      return started;
   }

   public synchronized void activate()
   {
      replicatingChannel = null;

      activated = true;

      executor.execute(new CreateObjectsRunnable());
   }

   public SimpleString getName()
   {
      return name;
   }

   public Queue getQueue()
   {
      return queue;
   }
   
   public void setQueue(final Queue queue)
   {
      this.queue = queue;
   }

   public Filter getFilter()
   {
      return filter;
   }

   public SimpleString getForwardingAddress()
   {
      return forwardingAddress;
   }

   public Transformer getTransformer()
   {
      return transformer;
   }

   public boolean isUseDuplicateDetection()
   {
      return useDuplicateDetection;
   }

   // For testing only
   public RemotingConnection getForwardingConnection()
   {
      if (session == null)
      {
         return null;
      }
      else
      {
         return ((ClientSessionInternal)session).getConnection();
      }
   }

   // SendAcknowledgementHandler implementation ---------------------

   public void sendAcknowledged(final Message message)
   {
      try
      {
         final MessageReference ref = refs.poll();

         if (ref != null)
         {
            if (replicatingChannel == null)
            {
               // Acknowledge when we know send has been processed on the server
               ref.getQueue().acknowledge(ref);
            }
            else
            {
               Packet packet = new ReplicateAcknowledgeMessage(name, ref.getMessage().getMessageID());

               replicatingChannel.replicatePacket(packet, 1, new Runnable()
               {
                  public void run()
                  {
                     try
                     {
                        ref.getQueue().acknowledge(ref);
                     }
                     catch (Exception e)
                     {
                        log.error("Failed to ack", e);
                     }
                  }
               });
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to ack", e);
      }
   }

   // Consumer implementation ---------------------------------------

   public HandleStatus handle(final MessageReference ref) throws Exception
   {     
      if (filter != null && !filter.match(ref.getMessage()))
      {
         return HandleStatus.NO_MATCH;
      }
      
      if (!active)
      {
         return HandleStatus.BUSY;
      }

      synchronized (this)
      {
         ref.getQueue().referenceHandled();

         ServerMessage message = ref.getMessage();

         refs.add(ref);

         if (flowRecord != null)
         {
            // We make a shallow copy of the message, then we strip out the unwanted routing id headers and leave
            // only
            // the one pertinent for the destination node - this is important since different queues on different
            // nodes could have same queue ids
            // Note we must copy since same message may get routed to other nodes which require different headers
            message = message.copy();

            // TODO - we can optimise this

            Set<SimpleString> propNames = new HashSet<SimpleString>(message.getPropertyNames());

            byte[] queueIds = (byte[])message.getProperty(idsHeaderName);

            for (SimpleString propName : propNames)
            {
               if (propName.startsWith(MessageImpl.HDR_ROUTE_TO_IDS))
               {
                  message.removeProperty(propName);
               }
            }

            message.putBytesProperty(MessageImpl.HDR_ROUTE_TO_IDS, queueIds);

            message.putBooleanProperty(MessageImpl.HDR_FROM_CLUSTER, Boolean.TRUE);
         }

         if (useDuplicateDetection && !message.containsProperty(MessageImpl.HDR_DUPLICATE_DETECTION_ID))
         {
            // If we are using duplicate detection and there's not already a duplicate detection header, then
            // we add a header composed of the persistent node id and the message id, which makes it globally unique
            // between restarts.
            // If you use a cluster connection then a guid based duplicate id will be used since it is added *before*
            // the
            // message goes into the store and forward queue.
            // But with this technique it also works when the messages don't already have such a header in them in the
            // queue.
            byte[] bytes = new byte[24];

            ByteBuffer bb = ByteBuffer.wrap(bytes);

            bb.put(nodeUUID.asBytes());

            bb.putLong(message.getMessageID());

            message.putBytesProperty(MessageImpl.HDR_DUPLICATE_DETECTION_ID, bytes);
         }

         if (transformer != null)
         {
            message = transformer.transform(message);
         }

         SimpleString dest;

         if (forwardingAddress != null)
         {
            dest = forwardingAddress;
         }
         else
         {
            // Preserve the original address
            dest = message.getDestination();
         }
         
         producer.send(dest, message);

         return HandleStatus.HANDLED;
      }
   }

   // FailureListener implementation --------------------------------

   public void connectionFailed(final MessagingException me)
   {
      log.info("bridge " + name + " failed " + me);
      
      fail();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void waitForRunnablesToComplete()
   {
      // Wait for any create objects runnable to complete
      Future future = new Future();

      executor.execute(future);

      boolean ok = future.await(10000);

      if (!ok)
      {
         log.warn("Timed out waiting to stop");
      }
   }

   private void fail()
   {      
      if (started)
      {
         executor.execute(new FailRunnable());
      }
   }

   private synchronized boolean createObjects()
   {
      if (!started)
      {
         return false;
      }
      
      try
      {
         queue.addConsumer(BridgeImpl.this);
  
         csf = null;
         if (discoveryAddress != null)
         {
            csf = new ClientSessionFactoryImpl(discoveryAddress, discoveryPort);
         }
         else
         {
            csf = new ClientSessionFactoryImpl(connectorPair.a,
                                         connectorPair.b);
         }
         
         csf.setFailoverOnServerShutdown(failoverOnServerShutdown);
         csf.setRetryInterval(retryInterval);
         csf.setRetryIntervalMultiplier(retryIntervalMultiplier);
         csf.setReconnectAttempts(reconnectAttempts);

         //Session is pre-acknowledge
         session = (ClientSessionInternal)csf.createSession(clusterUser,
                                                            clusterPassword,
                                                            false,
                                                            true,
                                                            true,
                                                            true,
                                                            1);
         
         if (session == null)
         {
            // This can happen if the bridge is shutdown
            return false;
         }

         producer = session.createProducer();

         session.addFailureListener(BridgeImpl.this);

         session.setSendAcknowledgementHandler(BridgeImpl.this);

         // TODO - we should move this code to the ClusterConnectorImpl - and just execute it when the bridge
         // connection is opened and closed - we can use
         // a callback to tell us that
         if (flowRecord != null)
         {
            // Get the queue data

            // Create a queue to catch the notifications - the name must be deterministic on live and backup, but
            // different each time this is called
            // Otherwise it may already exist if server is restarted before it has been deleted on backup

            String qName = "notif." + nodeUUID.toString() + "." + name.toString();
            
            SimpleString notifQueueName = new SimpleString(qName);

            SimpleString filter = new SimpleString(ManagementHelper.HDR_BINDING_TYPE + "<>" +
                                                   BindingType.DIVERT.toInt() +
                                                   " AND " +
                                                   ManagementHelper.HDR_NOTIFICATION_TYPE +
                                                   " IN ('" +
                                                   NotificationType.BINDING_ADDED +
                                                   "','" +
                                                   NotificationType.BINDING_REMOVED +
                                                   "','" +
                                                   NotificationType.CONSUMER_CREATED +
                                                   "','" +
                                                   NotificationType.CONSUMER_CLOSED +
                                                   "') AND " +
                                                   ManagementHelper.HDR_DISTANCE +
                                                   "<" +
                                                   flowRecord.getMaxHops() +
                                                   " AND (" +
                                                   ManagementHelper.HDR_ADDRESS +
                                                   " LIKE '" +
                                                   flowRecord.getAddress() +
                                                   "%')");

            //The queue can't be temporary, since if the node with the bridge crashes then is restarted quickly
            //it might get deleted on the target when it does connection cleanup
            
            //When the backup activates the queue might already exist, so we catch this and ignore
            try
            {
               session.createQueue(managementNotificationAddress, notifQueueName, filter, false);
            }
            catch (MessagingException me)
            {
               if (me.getCode() == MessagingException.QUEUE_EXISTS)
               {
                  //Ok
               }
               else
               {
                  throw me;
               }
            }

            ClientConsumer notifConsumer = session.createConsumer(notifQueueName);

            notifConsumer.setMessageHandler(flowRecord);

            session.start();

            ClientMessage message = session.createClientMessage(false);

            ManagementHelper.putOperationInvocation(message,
                                                    ResourceNames.CORE_SERVER,
                                                    "sendQueueInfoToQueue",
                                                    notifQueueName.toString(),
                                                    flowRecord.getAddress());

            ClientProducer prod = session.createProducer(managementAddress);

            prod.send(message);
         }

         active = true;

         queue.deliverAsync(executor);
         
         return true;
      }
      catch (Exception e)
      {
         log.warn("Unable to connect. Bridge is now disabled.", e);

         return false;
      }
   }

   // Inner classes -------------------------------------------------

   private class StopRunnable implements Runnable
   {
      public void run()
      {
         try
         {
            synchronized (BridgeImpl.this)
            {
               if (!started)
               {
                  return;
               }

               if (session != null)
               {              
                  session.close();               
               }

               started = false;

               active = false;

            }

            queue.removeConsumer(BridgeImpl.this);

            cancelRefs();
         }
         catch (Exception e)
         {
            log.error("Failed to stop bridge", e);
         }
      }
   }

   private class FailRunnable implements Runnable
   {
      public void run()
      {
         synchronized (BridgeImpl.this)
         {
            if (!started)
            {
               return;
            }

            if (flowRecord != null)
            {
               try
               {
                 // flowRecord.reset();
               }
               catch (Exception e)
               {
                  log.error("Failed to reset", e);
               }
            }

            active = false;
         }

         try
         {
            queue.removeConsumer(BridgeImpl.this);

            session.cleanUp();

            cancelRefs();

            csf.close();
         }
         catch (Exception e)
         {
            log.error("Failed to stop", e);
         }
         
         log.info("Bridge " + name + " closed objects");

         if (!createObjects())
         {
            started = false;
         }
      }
   }

   private class CreateObjectsRunnable implements Runnable
   {
      public synchronized void run()
      {
         if (!createObjects())
         {
            active = false;

            started = false;
         }
      }
   }

}
