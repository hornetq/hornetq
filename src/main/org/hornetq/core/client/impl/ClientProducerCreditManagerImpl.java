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

import java.util.HashMap;
import java.util.Map;

import org.hornetq.SimpleString;

/**
 * A ProducerCreditManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ClientProducerCreditManagerImpl implements ClientProducerCreditManager
{
   private final Map<SimpleString, ClientProducerCredits> producerCredits = new HashMap<SimpleString, ClientProducerCredits>();

   private final ClientSessionInternal session;

   private final int windowSize;

   public ClientProducerCreditManagerImpl(final ClientSessionInternal session, final int windowSize)
   {
      this.session = session;

      this.windowSize = windowSize;
   }

   public synchronized ClientProducerCredits getCredits(final SimpleString address)
   {
      ClientProducerCredits credits = producerCredits.get(address);

      if (credits == null)
      {
         // Doesn't need to be fair since session is single threaded
         credits = new ClientProducerCreditsImpl(session, address, windowSize);

         producerCredits.put(address, credits);
      }

      return credits;
   }

   public synchronized void receiveCredits(final SimpleString address, final int credits, final int offset)
   {
      ClientProducerCredits cr = producerCredits.get(address);

      if (cr != null)
      {
         cr.receiveCredits(credits, offset);
      }
   }

   public synchronized void reset()
   {
      for (ClientProducerCredits credits : producerCredits.values())
      {
         credits.reset();
      }
   }

   public synchronized void close()
   {
      for (ClientProducerCredits credits : producerCredits.values())
      {
         credits.close();
      }

      producerCredits.clear();
   }
}
