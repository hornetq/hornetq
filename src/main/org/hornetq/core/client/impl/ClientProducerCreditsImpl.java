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

import org.hornetq.core.logging.Logger;
import org.hornetq.utils.SimpleString;

/**
 * A ProducerCredits
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ClientProducerCreditsImpl implements ClientProducerCredits
{
   private static final Logger log = Logger.getLogger(ClientProducerCreditsImpl.class);

   private Semaphore semaphore;

   private final int windowSize;

   private final SimpleString destination;

   private final ClientSessionInternal session;

   private int arriving;

   private int offset;

   public ClientProducerCreditsImpl(final ClientSessionInternal session,
                                    final SimpleString destination,
                                    final int windowSize)
   {
      this.session = session;

      this.destination = destination;

      this.windowSize = windowSize / 2;

      // Doesn't need to be fair since session is single threaded

      semaphore = new Semaphore(0, false);

      // We initial request twice as many credits as we request in subsequent requests
      // This allows the producer to keep sending as more arrive, minimising pauses
      checkCredits(windowSize);
   }

   public void acquireCredits(int credits) throws InterruptedException
   {
      // credits += offset;
      
      checkCredits(credits);
            
      semaphore.acquire(credits);
   }

   public void receiveCredits(final int credits, final int offset)
   {
      synchronized (this)
      {
         arriving -= credits;

         this.offset = offset;
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
      session.sendProducerCreditsMessage(credits, destination);
   }

}
