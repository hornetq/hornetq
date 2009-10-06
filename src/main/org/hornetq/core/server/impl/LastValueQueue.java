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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.utils.SimpleString;

/**
 * A queue that will discard messages if a newer message with the same MessageImpl.HDR_LAST_VALUE_NAME property value.
 * In other words it only retains the last value
 * This is useful for example, for stock prices, where you're only interested in the latest value
 * for a particular stock
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class LastValueQueue extends QueueImpl
{
   private static final Logger log = Logger.getLogger(LastValueQueue.class);

   private final Map<SimpleString, ServerMessage> map = new HashMap<SimpleString, ServerMessage>();

   private final PagingManager pagingManager;

   private final StorageManager storageManager;

   public LastValueQueue(final long persistenceID,
                        final SimpleString address,
                        final SimpleString name,
                        final Filter filter,
                        final boolean durable,
                        final boolean temporary,
                        final ScheduledExecutorService scheduledExecutor,
                        final PostOffice postOffice,
                        final StorageManager storageManager,
                        final HierarchicalRepository<AddressSettings> addressSettingsRepository)
   {
      super(persistenceID,
            address,
            name,
            filter,
            durable,
            temporary,
            scheduledExecutor,
            postOffice,
            storageManager,
            addressSettingsRepository);
      this.pagingManager = postOffice.getPagingManager();
      this.storageManager = storageManager;
   }

   public void route(final ServerMessage message, final Transaction tx) throws Exception
   {
      SimpleString prop = (SimpleString)message.getProperty(MessageImpl.HDR_LAST_VALUE_NAME);
      if (prop != null)
      {
         synchronized (map)
         {
            ServerMessage msg = map.put(prop, message);
            // if an older message existed then we discard it
            if (msg != null)
            {
               MessageReference ref;
               if (tx != null)
               {
                  discardMessage(msg.getMessageID(), tx);
               }
               else
               {
                  ref = removeReferenceWithID(msg.getMessageID());
                  if (ref != null)
                  {
                     discardMessage(ref, tx);
                  }
               }

            }
         }
      }
      super.route(message, tx);
   }

   public MessageReference reroute(final ServerMessage message, final Transaction tx) throws Exception
   {
      SimpleString prop = (SimpleString)message.getProperty(MessageImpl.HDR_LAST_VALUE_NAME);
      if (prop != null)
      {
         synchronized (map)
         {
            ServerMessage msg = map.put(prop, message);
            if (msg != null)
            {
               if (tx != null)
               {
                  rediscardMessage(msg.getMessageID(), tx);
               }
               else
               {
                  MessageReference ref = removeReferenceWithID(msg.getMessageID());
                  rediscardMessage(ref);
               }
            }
         }
      }
      return super.reroute(message, tx);
   }

   public void acknowledge(final MessageReference ref) throws Exception
   {
      super.acknowledge(ref);
      SimpleString prop = (SimpleString)ref.getMessage().getProperty(MessageImpl.HDR_LAST_VALUE_NAME);
      if (prop != null)
      {
         synchronized (map)
         {
            ServerMessage serverMessage = map.get(prop);
            if (serverMessage != null && ref.getMessage().getMessageID() == serverMessage.getMessageID())
            {
               map.remove(prop);
            }
         }
      }
   }

   public void cancel(final Transaction tx, final MessageReference ref) throws Exception
   {
      SimpleString prop = (SimpleString)ref.getMessage().getProperty(MessageImpl.HDR_LAST_VALUE_NAME);
      if (prop != null)
      {
         synchronized (map)
         {
            ServerMessage msg = map.get(prop);
            if (msg.getMessageID() == ref.getMessage().getMessageID())
            {
               super.cancel(tx, ref);
            }
            else
            {
               discardMessage(ref, tx);
            }
         }
      }
      else
      {
         super.cancel(tx, ref);
      }
   }

   void postRollback(final LinkedList<MessageReference> refs) throws Exception
   {
      List<MessageReference> refsToDiscard = new ArrayList<MessageReference>();
      List<SimpleString> refsToClear = new ArrayList<SimpleString>();
      synchronized (map)
      {
         for (MessageReference ref : refs)
         {
            SimpleString prop = (SimpleString)ref.getMessage().getProperty(MessageImpl.HDR_LAST_VALUE_NAME);
            if (prop != null)
            {
               ServerMessage msg = map.get(prop);
               if (msg != null)
               {
                  if (msg.getMessageID() != ref.getMessage().getMessageID())
                  {
                     refsToDiscard.add(ref);
                  }
                  else
                  {
                     refsToClear.add(prop);
                  }
               }
            }
         }
         for (SimpleString simpleString : refsToClear)
         {
            map.remove(simpleString);
         }
      }
      for (MessageReference ref : refsToDiscard)
      {
         refs.remove(ref);
         discardMessage(ref, null);
      }
      super.postRollback(refs);
   }

   final void discardMessage(MessageReference ref, Transaction tx) throws Exception
   {
      deliveringCount.decrementAndGet();
      PagingStore store = pagingManager.getPageStore(ref.getMessage().getDestination());
      store.addSize(-ref.getMemoryEstimate());
      QueueImpl queue = (QueueImpl)ref.getQueue();
      ServerMessage msg = ref.getMessage();
      boolean durableRef = msg.isDurable() && queue.isDurable();

      if (durableRef)
      {
         int count = msg.decrementDurableRefCount();

         if (count == 0)
         {
            if (tx == null)
            {
               storageManager.deleteMessage(msg.getMessageID());
            }
            else
            {
               storageManager.deleteMessageTransactional(tx.getID(), getID(), msg.getMessageID());
            }
         }
      }
   }

   final void discardMessage(Long id, Transaction tx) throws Exception
   {
      RefsOperation oper = getRefsOperation(tx);
      Iterator<MessageReference> iterator = oper.refsToAdd.iterator();

      while (iterator.hasNext())
      {
         MessageReference ref = iterator.next();

         if (ref.getMessage().getMessageID() == id)
         {
            iterator.remove();
            discardMessage(ref, tx);
            break;
         }
      }

   }

   final void rediscardMessage(long id, Transaction tx) throws Exception
   {
      RefsOperation oper = getRefsOperation(tx);
      Iterator<MessageReference> iterator = oper.refsToAdd.iterator();

      while (iterator.hasNext())
      {
         MessageReference ref = iterator.next();

         if (ref.getMessage().getMessageID() == id)
         {
            iterator.remove();
            rediscardMessage(ref);
            break;
         }
      }
   }

   final void rediscardMessage(MessageReference ref) throws Exception
   {
      deliveringCount.decrementAndGet();
      PagingStore store = pagingManager.getPageStore(ref.getMessage().getDestination());
      store.addSize(-ref.getMemoryEstimate());
   }
}
