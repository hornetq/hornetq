/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

import java.util.concurrent.Executor;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.Future;

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
   private static final Logger log = Logger.getLogger(Redistributor.class);

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
   
   public synchronized void start()
   {
      active = true;
   }
   
   public synchronized void stop() throws Exception
   {
      active = false;
            
      Future future = new Future();
      
      executor.execute(future);
      
      boolean ok = future.await(10000);
      
      if (!ok)
      {
         log.warn("Timed out waiting for tasks to complete");
      }
   }

   public synchronized void close()
   {
      Future future = new Future();

      executor.execute(future);

      boolean ok = future.await(10000);

      if (!ok)
      {
         throw new IllegalStateException("Timed out waiting for executor to complete");
      }

      active = false;
   }

   public synchronized HandleStatus handle(final MessageReference reference) throws Exception
   {
      if (!active)
      {
         return HandleStatus.BUSY;
      }
        
      Transaction tx = new TransactionImpl(storageManager);

      boolean routed = postOffice.redistribute(reference.getMessage(), queue.getName(), tx);

      if (routed)
      {
         queue.referenceHandled();

         queue.acknowledge(tx, reference);

         tx.commit();
  
         count++;

         if (count == batchSize)
         {
            // We continue the next batch on a different thread, so as not to keep the delivery thread busy for a very
            // long time, in the case there are many messages in the queue
            active = false;

            executor.execute(new Prompter());

            count = 0;
         }

         return HandleStatus.HANDLED;
      }
      else
      {
         return HandleStatus.BUSY;
      }
   }

   private class Prompter implements Runnable
   {
      public void run()
      {
         synchronized (Redistributor.this)
         {
            active = true;

            queue.deliverAsync(executor);
         }
      }
   }
}
