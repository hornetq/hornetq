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

import java.util.LinkedList;
import java.util.concurrent.Executor;

import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.cluster.Forwarder;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.Future;

/**
 * A ForwarderImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 12 Nov 2008 11:37:35
 *
 *
 */
public class ForwarderImpl implements Forwarder
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ForwarderImpl.class);

   // Attributes ----------------------------------------------------

   private final Queue queue;

   private Executor executor;

   private volatile boolean busy;

   private int maxBatchSize;

   private long maxBatchTime;

   private int count;

   private java.util.Queue<MessageReference> refs = new LinkedList<MessageReference>();

   private Transaction tx;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;
   
   private final Transformer transformer;

   private final ClientSessionFactory csf;
   
   private ClientSession session;

   private ClientProducer producer;
      
   private volatile boolean started;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public ForwarderImpl(final Queue queue,
                        final TransportConfiguration connectorConfig,
                        final Executor executor,
                        final int maxBatchSize,
                        final long maxBatchTime,
                        final StorageManager storageManager,
                        final PostOffice postOffice,
                        final HierarchicalRepository<QueueSettings> queueSettingsRepository,                        
                        final Transformer transformer) throws Exception
   {
      this.queue = queue;

      this.executor = executor;

      this.maxBatchSize = maxBatchSize;

      this.maxBatchTime = maxBatchTime;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.queueSettingsRepository = queueSettingsRepository;
      
      this.transformer = transformer;
      
      this.csf = new ClientSessionFactoryImpl(connectorConfig);      
   }
   
   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }
      
      createTx();
      
      session = csf.createSession(false, false, false);

      producer = session.createProducer(null);

      queue.addConsumer(this);       
      
      started = true;
   }

   public synchronized void stop() throws Exception
   {
      started = false;

      queue.removeConsumer(this);

      // Wait until all batches are complete

      Future future = new Future();

      executor.execute(future);

      boolean ok = future.await(10000);

      if (!ok)
      {
         log.warn("Timed out waiting for batch to be sent");
      }
      
      session.close();
      
      started = false;
   }
   
   public boolean isStarted()
   {
      return started;
   }

   // Consumer implementation ---------------------------------------

   public HandleStatus handle(final MessageReference reference) throws Exception
   {
      if (busy)
      {
         return HandleStatus.BUSY;
      }

      synchronized (this)
      {
         if (!started)
         {
            return HandleStatus.BUSY;
         }

         refs.add(reference);

         count++;

         if (count == maxBatchSize)
         {
            busy = true;

            executor.execute(new BatchSender());
         }

         return HandleStatus.HANDLED;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void sendBatch()
   {
      try
      {
         synchronized (this)
         {
            // TODO - duplicate detection on sendee and if batch size = 1 then don't need tx

            while (true)
            {
               MessageReference ref = refs.poll();

               if (ref == null)
               {
                  break;
               }

               tx.addAcknowledgement(ref);

               ServerMessage message = ref.getMessage();
               
               if (transformer != null)
               {
                  message = transformer.transform(message);
               }

               producer.send(message.getDestination(), message);
            }

            session.commit();

            tx.commit();

            createTx();

            busy = false;

            count = 0;
         }

         queue.deliverAsync(executor);
      }
      catch (Exception e)
      {
         log.error("Failed to forward batch", e);

         try
         {
            tx.rollback(queueSettingsRepository);
         }
         catch (Exception e2)
         {
            log.error("Failed to rollback", e2);
         }
      }
   }

   private void createTx()
   {
      tx = new TransactionImpl(storageManager, postOffice);
   }

   // Inner classes -------------------------------------------------

   private class BatchSender implements Runnable
   {
      public void run()
      {
         sendBatch();
      }
   }

}
