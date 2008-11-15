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

package org.jboss.messaging.core.server.impl;

import java.util.LinkedList;
import java.util.concurrent.Executor;

import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.Future;

/**
 * A Forwarder
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 12 Nov 2008 11:37:35
 *
 *
 */
public class Forwarder implements Consumer
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Forwarder.class);

   // Attributes ----------------------------------------------------

   private final Queue queue;

   private Executor executor;

   private volatile boolean busy;

   private int maxBatchSize;

   private long maxBatchTime;

   private int count;

   private java.util.Queue<MessageReference> refs = new LinkedList<MessageReference>();

   private boolean closed;

   private Transaction tx;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final ClientSession session;

   private final ClientProducer producer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Forwarder(final Queue queue,
                    final TransportConfiguration connectorConfig, final Executor executor, final int maxBatchSize,
                    final long maxBatchTime,
                    final StorageManager storageManager, final PostOffice postOffice,
                    final HierarchicalRepository<QueueSettings> queueSettingsRepository)
      throws Exception
   {
      this.queue = queue;
      
      this.executor = executor;
      
      this.maxBatchSize = maxBatchSize;
      
      this.maxBatchTime = maxBatchTime;
      
      this.storageManager = storageManager;
      
      this.postOffice = postOffice;
      
      this.queueSettingsRepository = queueSettingsRepository;
      
      createTx();
      
      ClientSessionFactory csf = new ClientSessionFactoryImpl(connectorConfig);
      
      session = csf.createSession(false, false, false);
      
      producer = session.createProducer(null);
      
      queue.addConsumer(this);
   }

   public synchronized void close() throws Exception
   {
      closed = true;
      
      queue.removeConsumer(this);

      // Wait until all batches are complete

      Future future = new Future();

      executor.execute(future);

      boolean ok = future.await(10000);

      if (!ok)
      {
         log.warn("Timed out waiting for batch to be sent");
      }
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
         if (closed)
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
            //TODO - duplicate detection on sendee and if batch size = 1 then don't need tx
   
            while (true)
            {
               MessageReference ref = refs.poll();
   
               if (ref == null)
               {
                  break;
               }
   
               tx.addAcknowledgement(ref);
   
               Message message = ref.getMessage();
               
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
