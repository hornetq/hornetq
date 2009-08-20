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

package org.hornetq.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * 
 * <p>This class will use the framework provided to by AbstractQueuedSynchronizer.</p>
 * <p>AbstractQueuedSynchronizer is the framework for any sort of concurrent synchronization, such as Semaphores, events, etc, based on AtomicIntegers.</p>
 * 
 * <p>The idea is, instead of providing each user specific Latch/Synchronization, java.util.concurrent provides the framework for reuses, based on an AtomicInteger (getState())</p>
 * 
 * <p>On HornetQ we have the requirement of increment and decrement a counter until the user fires a ready event (commit). At that point we just act as a regular countDown.</p>
 * 
 * <p>Note: This latch is reusable. Once it reaches zero, you can call up again, and reuse it on further waits.</p>
 * 
 * <p>For example: prepareTransaction will wait for the current completions, and further adds will be called on the latch. Later on when commit is called you can reuse the same latch.</p>
 * 
 * @author Clebert Suconic
 * */
public class VariableLatch
{
   /** 
    * Look at the doc and examples provided by AbstractQueuedSynchronizer for more information 
    * @see AbstractQueuedSynchronizer*/
   @SuppressWarnings("serial")
   private static class CountSync extends AbstractQueuedSynchronizer
   {
      public CountSync()
      {
         setState(0);
      }

      public int getCount()
      {
         return getState();
      }

      @Override
      public int tryAcquireShared(final int numberOfAqcquires)
      {
         return getState() == 0 ? 1 : -1;
      }

      public void add()
      {
         for (;;)
         {
            int actualState = getState();
            int newState = actualState + 1;
            if (compareAndSetState(actualState, newState))
            {
               return;
            }
         }
      }

      @Override
      public boolean tryReleaseShared(final int numberOfReleases)
      {
         for (;;)
         {
            int actualState = getState();
            if (actualState == 0)
            {
               return true;
            }

            int newState = actualState - numberOfReleases;

            if (compareAndSetState(actualState, newState))
            {
               return newState == 0;
            }
         }
      }
   }

   private final CountSync control = new CountSync();

   public int getCount()
   {
      return control.getCount();
   }

   public void up()
   {
      control.add();
   }

   public void down()
   {
      control.releaseShared(1);
   }

   public void waitCompletion() throws InterruptedException
   {
      control.acquireSharedInterruptibly(1);
   }

   public boolean waitCompletion(final long milliseconds) throws InterruptedException
   {
      return control.tryAcquireSharedNanos(1, TimeUnit.MILLISECONDS.toNanos(milliseconds));
   }
}
