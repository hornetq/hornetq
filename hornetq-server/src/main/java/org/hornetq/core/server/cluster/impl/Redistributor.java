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

import java.util.concurrent.Executor;

import org.hornetq.utils.Pair;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.utils.FutureLatch;

/**
 * A Redistributor
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 8 Feb 2009 14:23:41
 *
 *
 */
public class Redistributor implements Consumer
{
   private boolean active;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final Executor executor;

   private final int batchSize;

   private final Queue queue;

   private int count;

   public Redistributor(final Queue queue,
                        final StorageManager storageManager,
                        final PostOffice postOffice,
                        final Executor executor,
                        final int batchSize)
   {
      this.queue = queue;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.executor = executor;

      this.batchSize = batchSize;
   }

   public Filter getFilter()
   {
      return null;
   }
   
   public String debug()
   {
      return toString();
   }

   public synchronized void start()
   {
      active = true;
   }

   public synchronized void stop() throws Exception
   {
      active = false;

      boolean ok = flushExecutor();

      if (!ok)
      {
         HornetQServerLogger.LOGGER.errorStoppingRedistributor();
      }
   }

   public synchronized void close()
   {
      boolean ok = flushExecutor();

      if (!ok)
      {
         throw new IllegalStateException("Timed out waiting for executor to complete");
      }

      active = false;
   }

   private boolean flushExecutor()
   {
      FutureLatch future = new FutureLatch();

      executor.execute(future);

      boolean ok = future.await(10000);
      return ok;
   }

   public synchronized HandleStatus handle(final MessageReference reference) throws Exception
   {
      if (!active)
      {
         return HandleStatus.BUSY;
      }
      
      final Transaction tx = new TransactionImpl(storageManager);

      final Pair<RoutingContext, ServerMessage> routingInfo = postOffice.redistribute(reference.getMessage(), queue, tx);

      if (routingInfo == null)
      {
         return HandleStatus.BUSY;
      }
      
      if (!reference.getMessage().isLargeMessage())
      {
         routingInfo.getB().finishCopy();

         postOffice.processRoute(routingInfo.getB(), routingInfo.getA(), false);

         ackRedistribution(reference, tx);
      }
      else
      {
         active = false;
         executor.execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  routingInfo.getB().finishCopy();

                  postOffice.processRoute(routingInfo.getB(), routingInfo.getA(), false);
      
                  ackRedistribution(reference, tx);
                  
                  synchronized (Redistributor.this)
                  {
                     active = true;
                     
                     count++;
                     
                     queue.deliverAsync();
                  }
               }
               catch (Exception e)
               {
                  try
                  {
                     tx.rollback();
                  }
                  catch (Exception e2)
                  {
                     // Nothing much we can do now
                     
                     // TODO log 
                     HornetQServerLogger.LOGGER.warn(e2.getMessage(), e2);
                  }
               }
            }
         });
      }

      return HandleStatus.HANDLED;
   }

   private void ackRedistribution(final MessageReference reference, final Transaction tx) throws Exception
   {
      reference.handled();

      queue.acknowledge(tx, reference);

      tx.commit();

      storageManager.afterCompleteOperations(new IOAsyncTask()
      {

         public void onError(final int errorCode, final String errorMessage)
         {
            HornetQServerLogger.LOGGER.ioErrorRedistributing(errorCode, errorMessage);
         }

         public void done()
         {
            execPrompter();
         }
      });
   }

   private void execPrompter()
   {
      count++;

      // We use >= as the large message redistribution will set count to max_int
      // so we are use the prompter will get called
      if (count >= batchSize)
      {
         // We continue the next batch on a different thread, so as not to keep the delivery thread busy for a very
         // long time in the case there are many messages in the queue
         active = false;

         executor.execute(new Prompter());

         count = 0;
      }

   }

   private class Prompter implements Runnable
   {
      public void run()
      {
         synchronized (Redistributor.this)
         {
            active = true;

            queue.deliverAsync();
         }
      }
   }
}
