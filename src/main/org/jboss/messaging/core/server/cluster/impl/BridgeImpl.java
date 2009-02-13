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

package org.jboss.messaging.core.server.cluster.impl;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PING_PERIOD;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.SendAcknowledgementHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.NotificationType;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.postoffice.BindingType;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.security.impl.SecurityStoreImpl;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.cluster.Bridge;
import org.jboss.messaging.core.server.cluster.MessageFlowRecord;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.util.Future;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.UUIDGenerator;

/**
 * A BridgeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   private final SimpleString name;

   private final Queue queue;

   private final Executor executor;

   private final SimpleString filterString;

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

   private final long retryInterval;

   private final double retryIntervalMultiplier;

   private final int maxRetriesBeforeFailover;

   private final int maxRetriesAfterFailover;

   private final SimpleString idsHeaderName;

   private MessageFlowRecord flowRecord;

   private final SimpleString managementAddress;

   private final SimpleString managementNotificationAddres;

   private final String clusterPassword;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public BridgeImpl(final SimpleString name,
                     final Queue queue,
                     final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                     final Executor executor,
                     final SimpleString filterString,
                     final SimpleString forwardingAddress,
                     final ScheduledExecutorService scheduledExecutor,
                     final Transformer transformer,
                     final long retryInterval,
                     final double retryIntervalMultiplier,
                     final int maxRetriesBeforeFailover,
                     final int maxRetriesAfterFailover,
                     final boolean useDuplicateDetection, 
                     final SimpleString managementAddress, 
                     final SimpleString managementNotificationAddress,
                     final String clusterPassword) throws Exception
   {
      this(name,
           queue,
           connectorPair,
           executor,
           filterString,
           forwardingAddress,
           scheduledExecutor,
           transformer,
           retryInterval,
           retryIntervalMultiplier,
           maxRetriesBeforeFailover,
           maxRetriesAfterFailover,
           useDuplicateDetection,
           managementAddress,
           managementNotificationAddress,
           clusterPassword,
           null);
   }

   public BridgeImpl(final SimpleString name,
                     final Queue queue,
                     final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                     final Executor executor,
                     final SimpleString filterString,
                     final SimpleString forwardingAddress,
                     final ScheduledExecutorService scheduledExecutor,
                     final Transformer transformer,
                     final long retryInterval,
                     final double retryIntervalMultiplier,
                     final int maxRetriesBeforeFailover,
                     final int maxRetriesAfterFailover,
                     final boolean useDuplicateDetection,
                     final SimpleString managementAddress, 
                     final SimpleString managementNotificationAddress,
                     final String clusterPassword,
                     final MessageFlowRecord flowRecord) throws Exception
   {
      this.name = name;

      this.queue = queue;

      this.executor = executor;

      this.filterString = filterString;

      if (this.filterString != null)
      {
         this.filter = new FilterImpl(filterString);
      }
      else
      {
         this.filter = null;
      }

      this.forwardingAddress = forwardingAddress;

      this.transformer = transformer;

      this.useDuplicateDetection = useDuplicateDetection;

      this.connectorPair = connectorPair;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;

      this.maxRetriesAfterFailover = maxRetriesAfterFailover;

      this.idsHeaderName = MessageImpl.HDR_ROUTE_TO_IDS.concat(name);

      this.managementAddress = managementAddress;
      
      this.managementNotificationAddres = managementNotificationAddress;
      
      this.clusterPassword = clusterPassword;
      
      this.flowRecord = flowRecord;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      started = true;

      executor.execute(new CreateObjectsRunnable());
   }

   private void cancelRefs() throws Exception
   {
      MessageReference ref;

      LinkedList<MessageReference> list = new LinkedList<MessageReference>();

      while ((ref = refs.poll()) != null)
      {
         // ref.getQueue().cancel(ref);
         list.addFirst(ref);
      }

      for (MessageReference ref2 : list)
      {
         ref2.getQueue().cancel(ref2);
      }
   }

   public void stop() throws Exception
   {
      executor.execute(new StopRunnable());
      
      this.waitForRunnablesToComplete();
   }

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

               // We close the session factory here - this will cause any connection retries to stop

               csf.close();

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
                  flowRecord.reset();
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

         if (!createObjects())
         {
            started = false;
         }         
      }
   }

   private void waitForRunnablesToComplete()
   {
      // Wait for any create objects runnable to complete
      Future future = new Future();

      executor.execute(future);

      boolean ok = future.await(10000);

      if (!ok)
      {
         log.warn("Timed out waiting for batch to be sent");
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

         csf = new ClientSessionFactoryImpl(connectorPair.a,
                                            connectorPair.b,
                                            DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                            DEFAULT_PING_PERIOD,
                                            DEFAULT_CONNECTION_TTL,
                                            2000,
                                            DEFAULT_CONSUMER_WINDOW_SIZE,
                                            DEFAULT_CONSUMER_MAX_RATE,
                                            DEFAULT_SEND_WINDOW_SIZE,
                                            DEFAULT_PRODUCER_MAX_RATE,
                                            DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                            DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                            DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                            DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                            DEFAULT_AUTO_GROUP,
                                            DEFAULT_MAX_CONNECTIONS,
                                            DEFAULT_PRE_ACKNOWLEDGE,
                                            DEFAULT_ACK_BATCH_SIZE,
                                            retryInterval,
                                            retryIntervalMultiplier,
                                            maxRetriesBeforeFailover,
                                            maxRetriesAfterFailover);

         session = (ClientSessionInternal)csf.createSession(SecurityStoreImpl.CLUSTER_ADMIN_USER, clusterPassword, false, true, true, false, 1);

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

            SimpleString notifQueueName = new SimpleString("notif-").concat(UUIDGenerator.getInstance()
                                                                                         .generateSimpleStringUUID());

            // TODO - simplify this
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

            session.createQueue(managementNotificationAddres, notifQueueName, filter, false, true);

            ClientConsumer notifConsumer = session.createConsumer(notifQueueName);

            notifConsumer.setMessageHandler(flowRecord);

            session.start();

            ClientMessage message = session.createClientMessage(false);

            ManagementHelper.putOperationInvocation(message,
                                                    ObjectNames.getMessagingServerObjectName(),
                                                    "sendQueueInfoToQueue",
                                                    notifQueueName.toString(),
                                                    flowRecord.getAddress());

            ClientProducer prod = session.createProducer(managementAddress);

            prod.send(message);
         }

         active = true;

         queue.deliverAsync(executor);

         log.info("Bridge " + name + " connected successfully");

         return true;
      }
      catch (Exception e)
      {
         log.warn("Unable to connect. Bridge is now disabled.", e);

         return false;
      }
   }

   public boolean isStarted()
   {
      return started;
   }

   public SimpleString getName()
   {
      return name;
   }

   public Queue getQueue()
   {
      return queue;
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
         return ((ClientSessionImpl)session).getConnection();
      }
   }

   // SendAcknowledgementHandler implementation ---------------------

   public void sendAcknowledged(final Message message)
   {
      try
      {
         MessageReference ref = refs.poll();

         if (ref != null)
         {
            // Acknowledge when we know send has been processed on the server
            ref.getQueue().acknowledge(ref);
         }
      }
      catch (Exception e)
      {
         log.info("Failed to ack", e);
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
            byte[] bytes = new byte[8];

            ByteBuffer bb = ByteBuffer.wrap(bytes);

            // TODO NEEDS to incluse server id

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

   public boolean connectionFailed(final MessagingException me)
   {
      fail();

      return true;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

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
