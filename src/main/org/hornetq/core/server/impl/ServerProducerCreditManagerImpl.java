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

package org.hornetq.core.server.impl;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagingStore;

/**
 * A ProducerCreditManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ServerProducerCreditManagerImpl implements ServerProducerCreditManager
{
   private static final Logger log = Logger.getLogger(ServerProducerCreditManagerImpl.class);

   private final Queue<WaitingEntry> waiting = new ConcurrentLinkedQueue<WaitingEntry>();

   private final PagingStore pagingStore;

   public ServerProducerCreditManagerImpl(final PagingStore pagingStore)
   {
      this.pagingStore = pagingStore;
   }

   private static final class WaitingEntry
   {
      final CreditsAvailableRunnable waiting;

      volatile int credits;

      volatile boolean closed;

      WaitingEntry(final CreditsAvailableRunnable waiting, final int credits)
      {
         this.waiting = waiting;

         this.credits = credits;
      }

      void close()
      {
         closed = true;
      }
   }

   public int creditsReleased(int credits)
   {
      int init = credits;

      while (true)
      {
         WaitingEntry entry = waiting.peek();

         if (entry != null)
         {
            if (entry.credits <= credits)
            {
               waiting.remove();

               boolean sent = sendCredits(entry.waiting, entry.credits);

               if (sent)
               {
                  credits -= entry.credits;

                  if (entry.credits == credits)
                  {
                     break;
                  }
               }
            }
            else
            {
               entry.credits -= credits;

               boolean sent = sendCredits(entry.waiting, credits);
               
               if (sent)
               {
                  credits = 0;

                  break;
               }
               else
               {
                  waiting.remove();
               }
            }
         }
         else
         {
            break;
         }
      }

      return init - credits;
   }

   public int acquireCredits(final int credits, final CreditsAvailableRunnable runnable)
   {
      int available = pagingStore.getAvailableProducerCredits(credits);

      if (available < credits)
      {
         WaitingEntry entry = new WaitingEntry(runnable, credits - available);

         waiting.add(entry);
      }

      return available;
   }

   public int waitingEntries()
   {
      return waiting.size();
   }

   private boolean sendCredits(final CreditsAvailableRunnable runnable, final int credits)
   {
      return runnable.run(credits);
   }

   public PagingStore getStore()
   {
      return pagingStore;
   }

}
