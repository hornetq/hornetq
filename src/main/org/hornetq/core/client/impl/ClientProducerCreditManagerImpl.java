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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.hornetq.api.SimpleString;
import org.hornetq.core.logging.Logger;

/**
 * A ProducerCreditManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ClientProducerCreditManagerImpl implements ClientProducerCreditManager
{
   private static final Logger log = Logger.getLogger(ClientProducerCreditManagerImpl.class);

   private static final int MAX_ANON_CREDITS_CACHE_SIZE = 1000;

   private final Map<SimpleString, ClientProducerCredits> producerCredits = new LinkedHashMap<SimpleString, ClientProducerCredits>();

   private final Map<SimpleString, ClientProducerCredits> anonCredits = new LinkedHashMap<SimpleString, ClientProducerCredits>();

   private final ClientSessionInternal session;

   private final int windowSize;

   public ClientProducerCreditManagerImpl(final ClientSessionInternal session, final int windowSize)
   {
      this.session = session;

      this.windowSize = windowSize;
   }
      
   public synchronized ClientProducerCredits getCredits(final SimpleString address, final boolean anon)
   {
      ClientProducerCredits credits = producerCredits.get(address);

      if (credits == null)
      {
         // Doesn't need to be fair since session is single threaded
         credits = new ClientProducerCreditsImpl(session, address, windowSize);

         producerCredits.put(address, credits);

         if (anon)
         {
            addToAnonCache(address, credits);
         }
      }

      if (!anon)
      {
         credits.incrementRefCount();
         
         //Remove from anon credits (if there)
         anonCredits.remove(address);                     
      }
      else
      {
         credits.setAnon();
      }
      
      return credits;
   }

   public synchronized void returnCredits(final SimpleString address)
   {
      ClientProducerCredits credits = producerCredits.get(address);

      if (credits != null && credits.decrementRefCount() == 0)
      {
         if (!credits.isAnon())
         {
            removeEntry(address, credits);
         }
         else
         {
            //All the producer refs have been removed but it's been used anonymously too so we add to the anon cache
            addToAnonCache(address, credits);
         }
      }
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
   
   private void addToAnonCache(final SimpleString address, final ClientProducerCredits credits)
   {
      anonCredits.put(address, credits);
      
      if (anonCredits.size() > MAX_ANON_CREDITS_CACHE_SIZE)
      {
         //Remove the oldest entry
         
         Iterator<Map.Entry<SimpleString, ClientProducerCredits>> iter = anonCredits.entrySet().iterator();
         
         Map.Entry<SimpleString, ClientProducerCredits> oldest = iter.next();
         
         iter.remove();
         
         removeEntry(oldest.getKey(), oldest.getValue());
      }
   }
   
   private void removeEntry(final SimpleString address, final ClientProducerCredits credits)
   {
      producerCredits.remove(address);
      
      credits.releaseOutstanding();

      credits.close();
   }

}
