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

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.NotificationType;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.cluster.Bridge;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
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
public class BridgeImpl implements Bridge, FailureListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(BridgeImpl.class);

   // Attributes ----------------------------------------------------

   private final SimpleString name;

   private final Queue queue;

   private final Executor executor;

   private volatile boolean busy;

   private final int maxBatchSize;

   private final long maxBatchTime;

   private final SimpleString filterString;

   private final Filter filter;

   private final SimpleString forwardingAddress;

   private int count;

   private final java.util.Queue<MessageReference> refs = new LinkedList<MessageReference>();

   private Transaction tx;

   private long lastReceivedTime = -1;

   private final StorageManager storageManager;

   private final Transformer transformer;

   private volatile ClientSessionFactory csf;

   private volatile ClientSession session;

   private volatile ClientProducer producer;

   private volatile boolean started;

   private final ScheduledFuture<?> future;

   private final boolean useDuplicateDetection;

   private volatile boolean active;

   private final Pair<TransportConfiguration, TransportConfiguration> connectorPair;

   private final long retryInterval;

   private final double retryIntervalMultiplier;

   private final int maxRetriesBeforeFailover;

   private final int maxRetriesAfterFailover;

   private final MessageHandler queueInfoMessageHandler;

   private final String queueDataAddress;

   private final SimpleString idsHeaderName;

   private final boolean forClusterConnector;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public BridgeImpl(final SimpleString name,
                     final Queue queue,
                     final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                     final Executor executor,
                     final int maxBatchSize,
                     final long maxBatchTime,
                     final SimpleString filterString,
                     final SimpleString forwardingAddress,
                     final StorageManager storageManager,
                     final ScheduledExecutorService scheduledExecutor,
                     final Transformer transformer,
                     final long retryInterval,
                     final double retryIntervalMultiplier,
                     final int maxRetriesBeforeFailover,
                     final int maxRetriesAfterFailover,
                     final boolean useDuplicateDetection,
                     final MessageHandler queueInfoMessageHandler,
                     final String queueDataAddress,
                     final boolean forClusterConnector) throws Exception
   {      
      this.name = name;

      this.queue = queue;

      this.executor = executor;

      this.maxBatchSize = maxBatchSize;

      this.maxBatchTime = maxBatchTime;

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

      this.storageManager = storageManager;

      this.transformer = transformer;

      this.useDuplicateDetection = useDuplicateDetection;

      this.connectorPair = connectorPair;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;

      this.maxRetriesAfterFailover = maxRetriesAfterFailover;

      this.queueInfoMessageHandler = queueInfoMessageHandler;

      this.queueDataAddress = queueDataAddress;

      this.forClusterConnector = forClusterConnector;

      this.idsHeaderName = MessageImpl.HDR_ROUTE_TO_IDS.concat(name);

      if (maxBatchTime != -1)
      {
         future = scheduledExecutor.scheduleAtFixedRate(new BatchTimeout(),
                                                        maxBatchTime,
                                                        maxBatchTime,
                                                        TimeUnit.MILLISECONDS);
      }
      else
      {
         future = null;
      }
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      executor.execute(new CreateObjectsRunnable());

      started = true;
   }

   private class CreateObjectsRunnable implements Runnable
   {
      public synchronized void run()
      {
         try
         {
            createTx();

            queue.addConsumer(BridgeImpl.this);

            csf = new ClientSessionFactoryImpl(connectorPair.a,
                                               connectorPair.b,
                                               retryInterval,
                                               retryIntervalMultiplier,
                                               maxRetriesBeforeFailover,
                                               maxRetriesAfterFailover);

            session = csf.createSession(false, false, false);

            producer = session.createProducer();

            session.addFailureListener(BridgeImpl.this);

            // TODO - we should move this code to the ClusterConnectorImpl - and just execute it when the bridge
            // connection is opened and closed - we can use
            // a callback to tell us that
            if (queueInfoMessageHandler != null)
            {
               // Get the queue data

               SimpleString notifQueueName = new SimpleString("notif-").concat(UUIDGenerator.getInstance()
                                                                                            .generateSimpleStringUUID());

               SimpleString filter = new SimpleString(ManagementHelper.HDR_NOTIFICATION_TYPE + " IN (" +
                                                      "'" +
                                                      NotificationType.BINDING_ADDED +
                                                      "'," +
                                                      "'" +
                                                      NotificationType.BINDING_REMOVED +
                                                      "'," +
                                                      "'" +
                                                      NotificationType.CONSUMER_CREATED +
                                                      "'," +
                                                      "'" +
                                                      NotificationType.CONSUMER_CLOSED +
                                                      "') AND " +
                                                      "("+ ManagementHelper.HDR_ADDRESS + " IS NULL OR " +                                                      
                                                      ManagementHelper.HDR_ADDRESS +
                                                      " LIKE '" +
                                                      queueDataAddress +
                                                      "%')");

               session.createQueue(DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS, notifQueueName, filter, false, true);

               ClientConsumer notifConsumer = session.createConsumer(notifQueueName);

               notifConsumer.setMessageHandler(queueInfoMessageHandler);

               session.start();

               ClientMessage message = session.createClientMessage(false);

               ManagementHelper.putOperationInvocation(message,
                                                       ManagementServiceImpl.getMessagingServerObjectName(),
                                                       "sendQueueInfoToQueue",
                                                       notifQueueName.toString(),
                                                       queueDataAddress);

               ClientProducer prod = session.createProducer(ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS);

               prod.send(message);
            }

            active = true;

            queue.deliverAsync(executor);
         }
         catch (Exception e)
         {
            log.warn("Unable to connect. Bridge is now disabled.", e);

            active = false;

            started = false;
         }
      }
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      started = false;

      active = false;

      queue.removeConsumer(this);

      if (future != null)
      {
         future.cancel(false);
      }

      // Wait until all batches are complete

      Future future = new Future();

      executor.execute(future);

      boolean ok = future.await(10000);

      if (!ok)
      {
         log.warn("Timed out waiting for batch to be sent");
      }
      
      session.close();

      csf.close();
   }

   public boolean isStarted()
   {
      return started;
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

   // Consumer implementation ---------------------------------------

   public HandleStatus handle(final MessageReference reference) throws Exception
   {
      if (busy)
      {
         return HandleStatus.BUSY;
      }

      if (filter != null && !filter.match(reference.getMessage()))
      {
         return HandleStatus.NO_MATCH;
      }

      synchronized (this)
      {
         if (!active)
         {
            return HandleStatus.BUSY;
         }

         reference.getQueue().referenceHandled();

         refs.add(reference);

         if (maxBatchTime != -1)
         {
            lastReceivedTime = System.currentTimeMillis();
         }

         count++;

         if (count == maxBatchSize)
         {
            busy = true;

            executor.execute(new BatchSender());
         }

         return HandleStatus.HANDLED;
      }
   }

   // FailureListener implementation --------------------------------

   public synchronized boolean connectionFailed(final MessagingException me)
   {
      fail();

      return true;
   }

   private void fail()
   {
      if (!started)
      {
         return;
      }

      log.warn("Bridge connection to target failed. Will try to reconnect");

      try
      {
         tx.rollback();

         stop();
      }
      catch (Exception e)
      {
         log.error("Failed to stop", e);
      }

      executor.execute(new CreateObjectsRunnable());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private synchronized void timeoutBatch()
   {
      if (!active)
      {
         return;
      }

      if (lastReceivedTime != -1 && count > 0)
      {
         long now = System.currentTimeMillis();

         if (now - lastReceivedTime >= maxBatchTime)
         {
            sendBatch();
         }
      }
   }

   private synchronized void sendBatch()
   {
      try
      {
         if (count == 0)
         {
            return;
         }

         // TODO - if batch size = 1 then don't need tx - actually we should use asynch send acknowledgement stream - then we don't need a transaction at all

         while (true)
         {
            MessageReference ref = refs.poll();

            if (ref == null)
            {
               break;
            }

            ref.getQueue().acknowledge(tx, ref);

            ServerMessage message = ref.getMessage();
            
            if (this.forClusterConnector)
            {
               //We make a shallow copy of the message, then we strip out the unwanted routing id headers and leave only
               //the one pertinent for the destination node - this is important since different queues on different nodes could have same queue ids
               //Note we must copy since same message may get routed to other nodes which require different headers
               message = message.copy();
               
               //TODO - we can optimise this
              
               Set<SimpleString> propNames = new HashSet<SimpleString>(message.getPropertyNames());
               
               byte[] queueIds = (byte[])message.getProperty(idsHeaderName);
               
               for (SimpleString propName: propNames)
               {
                  if (propName.startsWith(MessageImpl.HDR_ROUTE_TO_IDS))
                  {
                     message.removeProperty(propName);
                  }
               }
               
               message.putBytesProperty(MessageImpl.HDR_ROUTE_TO_IDS, queueIds);
               
               message.putBooleanProperty(MessageImpl.HDR_FROM_CLUSTER, Boolean.TRUE);
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
         }

         session.commit();

         tx.commit();

         createTx();

         busy = false;

         count = 0;

         queue.deliverAsync(executor);
      }
      catch (Exception e)
      {
         log.error("Failed to forward batch", e);

         fail();
      }
   }

   private void createTx()
   {
      tx = new TransactionImpl(storageManager);
   }

   // Inner classes -------------------------------------------------

   private class BatchSender implements Runnable
   {
      public void run()
      {
         sendBatch();
      }
   }

   private class BatchTimeout implements Runnable
   {
      public void run()
      {
         timeoutBatch();
      }
   }

   public SimpleString getName()
   {
      return name;
   }

   public Queue getQueue()
   {
      return queue;
   }

   public int getMaxBatchSize()
   {
      return maxBatchSize;
   }

   public long getMaxBatchTime()
   {
      return maxBatchTime;
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

}
