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

package org.jboss.messaging.core.transaction.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;

/**
 * A ResourceManagerImpl
 * <p/>
 * TODO - implement timeouts
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ResourceManagerImpl implements ResourceManager
{
   private static final Logger log = Logger.getLogger(ResourceManagerImpl.class);

   private final ConcurrentMap<Xid, Transaction> transactions = new ConcurrentHashMap<Xid, Transaction>();

   private final int defaultTimeoutSeconds;

   private volatile int timeoutSeconds;

   private final ScheduledExecutorService executorService;

   private final Map<Xid, ScheduledFuture<Boolean>> scheduledTimeoutTxs = new HashMap<Xid, ScheduledFuture<Boolean>>();

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   public ResourceManagerImpl(final int defaultTimeoutSeconds,
                              final ScheduledExecutorService scheduledExecutor,
                              final StorageManager storageManager,
                              final PostOffice postOffice,
                              final HierarchicalRepository<QueueSettings> queueSettingsRepository)
   {
      this.defaultTimeoutSeconds = defaultTimeoutSeconds;
      this.executorService = scheduledExecutor;
      this.storageManager = storageManager;
      this.postOffice = postOffice;
      this.queueSettingsRepository = queueSettingsRepository;
   }

   // ResourceManager implementation ---------------------------------------------

   public Transaction getTransaction(final Xid xid)
   {
      return transactions.get(xid);
   }

   public boolean putTransaction(final Xid xid, final Transaction tx)
   {
      boolean added = transactions.putIfAbsent(xid, tx) == null;
      if (added && timeoutSeconds > 0)
      {
         ScheduledFuture<Boolean> future = executorService.schedule(new TxTimeoutHandler(tx),
                                                                    timeoutSeconds,
                                                                    TimeUnit.SECONDS);
         scheduledTimeoutTxs.put(xid, future);
      }
      return added;
   }

   public Transaction removeTransaction(final Xid xid)
   {
      ScheduledFuture<Boolean> future = scheduledTimeoutTxs.get(xid);
      if (future != null)
      {
         future.cancel(true);
      }
      return transactions.remove(xid);
   }

   public int getTimeoutSeconds()
   {
      return this.timeoutSeconds;
   }

   public boolean setTimeoutSeconds(final int timeoutSeconds)
   {
      if (timeoutSeconds == 0)
      {
         // reset to default
         this.timeoutSeconds = defaultTimeoutSeconds;
      }
      else
      {
         this.timeoutSeconds = timeoutSeconds;
      }

      return false;
   }

   public List<Xid> getPreparedTransactions()
   {
      List<Xid> xids = new ArrayList<Xid>();
      for (Xid xid : transactions.keySet())
      {
         if (transactions.get(xid).getState() == Transaction.State.PREPARED)
         {
            xids.add(xid);
         }
      }
      return xids;
   }

   private class TxTimeoutHandler implements Callable
   {
      final Transaction tx;

      public TxTimeoutHandler(final Transaction tx)
      {
         this.tx = tx;
      }

      public Object call() throws Exception
      {
         transactions.remove(tx.getXid());

         log.warn("transaction with xid " + tx.getXid() + " timed out");

         List<MessageReference> rolledBack = tx.timeout();

         Map<Queue, LinkedList<MessageReference>> queueMap = new HashMap<Queue, LinkedList<MessageReference>>();

         // TODO - this code is duplicated in ServerSessionImpl - combine
         for (MessageReference ref : rolledBack)
         {
            if (ref.cancel(storageManager, postOffice, queueSettingsRepository))
            {
               Queue queue = ref.getQueue();

               LinkedList<MessageReference> list = queueMap.get(queue);

               if (list == null)
               {
                  list = new LinkedList<MessageReference>();

                  queueMap.put(queue, list);
               }

               list.add(ref);
            }
         }

         for (Map.Entry<Queue, LinkedList<MessageReference>> entry : queueMap.entrySet())
         {
            LinkedList<MessageReference> refs = entry.getValue();

            entry.getKey().addListFirst(refs);
         }

         return null;
      }
   }
}
