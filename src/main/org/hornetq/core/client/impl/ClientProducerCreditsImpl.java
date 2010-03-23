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

package org.hornetq.core.client.impl;

import java.util.concurrent.Semaphore;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;

/**
 * A ClientProducerCreditsImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ClientProducerCreditsImpl implements ClientProducerCredits
{
   private static final Logger log = Logger.getLogger(ClientProducerCreditsImpl.class);

   private final Semaphore semaphore;

   private final int windowSize;

   private final SimpleString address;

   private final ClientSessionInternal session;

   private int arriving;
   
   private int refCount;
   
   public ClientProducerCreditsImpl(final ClientSessionInternal session,
                                    final SimpleString address,
                                    final int windowSize)
   {
      this.session = session;

      this.address = address;

      this.windowSize = windowSize / 2;

      // Doesn't need to be fair since session is single threaded

      semaphore = new Semaphore(0, false);

      // We initial request twice as many credits as we request in subsequent requests
      // This allows the producer to keep sending as more arrive, minimising pauses
      checkCredits(windowSize);
   }

   public void acquireCredits(final int credits) throws InterruptedException
   {
      checkCredits(credits);

      semaphore.acquire(credits);
   }

   public void receiveCredits(final int credits)
   {
      synchronized (this)
      {
         arriving -= credits;
      }
      
      semaphore.release(credits);
   }

   public synchronized void reset()
   {
      // Any arriving credits from before failover won't arrive, so we re-initialise

      semaphore.drainPermits();

      arriving = 0;

      checkCredits(windowSize * 2);
   }

   public void close()
   {
      // Closing a producer that is blocking should make it return

      semaphore.release(Integer.MAX_VALUE / 2);
   }
    
   public synchronized void incrementRefCount()
   {
      refCount++;
   }
   
   public synchronized int decrementRefCount()
   {
      return --refCount;
   }
   
   public synchronized void releaseOutstanding()
   {
      semaphore.drainPermits();
   }
   
   private void checkCredits(final int credits)
   {
      int needed = Math.max(credits, windowSize);

      int toRequest = -1;

      synchronized (this)
      {
         if (semaphore.availablePermits() + arriving < needed)
         {
            toRequest = needed - arriving;

            arriving += toRequest;
         }
      }

      if (toRequest != -1)
      {
         requestCredits(toRequest);
      }
   }

   private void requestCredits(final int credits)
   {
      session.sendProducerCreditsMessage(credits, address);
   }

}
