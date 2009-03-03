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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.utils.SimpleString;

/**
 * A queue that will discard messages if a newer message with the same MessageImpl.HDR_SOLE_MESSAGE property value.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SoloQueueImpl extends QueueImpl
{
   private static final Logger log = Logger.getLogger(SoloQueueImpl.class);

   private final Map<SimpleString, ServerMessage> map = new HashMap<SimpleString, ServerMessage>();

   private final PagingManager pagingManager;

   private final StorageManager storageManager;

   public SoloQueueImpl(final long persistenceID,
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
      SimpleString prop = (SimpleString)message.getProperty(MessageImpl.HDR_SOLE_MESSAGE);
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
      SimpleString prop = (SimpleString)message.getProperty(MessageImpl.HDR_SOLE_MESSAGE);
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
      SimpleString prop = (SimpleString)ref.getMessage().getProperty(MessageImpl.HDR_SOLE_MESSAGE);
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
      SimpleString prop = (SimpleString)ref.getMessage().getProperty(MessageImpl.HDR_SOLE_MESSAGE);
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
            SimpleString prop = (SimpleString)ref.getMessage().getProperty(MessageImpl.HDR_SOLE_MESSAGE);
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
               storageManager.deleteMessageTransactional(tx.getID(), getPersistenceID(), msg.getMessageID());
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
