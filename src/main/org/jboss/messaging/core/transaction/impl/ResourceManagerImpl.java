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
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.TimerTask;
import java.util.Set;
import java.util.HashSet;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.MessagingComponent;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.settings.HierarchicalRepository;

/**
 * A ResourceManagerImpl
 * <p/>
 * TODO - implement timeouts
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ResourceManagerImpl implements ResourceManager, MessagingComponent
{
   private static final Logger log = Logger.getLogger(ResourceManagerImpl.class);

   private final ConcurrentMap<Xid, Transaction> transactions = new ConcurrentHashMap<Xid, Transaction>();

   private final int defaultTimeoutSeconds;

   private volatile int timeoutSeconds;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private boolean started = false;

   private Timer timer;

   private TimerTask task;

   private final long txTimeoutScanPeriod;

   public ResourceManagerImpl(final int defaultTimeoutSeconds,
                              final long txTimeoutScanPeriod,
                              final StorageManager storageManager,
                              final PostOffice postOffice,
                              final HierarchicalRepository<QueueSettings> queueSettingsRepository)
   {
      this.defaultTimeoutSeconds = defaultTimeoutSeconds;
      this.timeoutSeconds = defaultTimeoutSeconds;
      this.txTimeoutScanPeriod = txTimeoutScanPeriod;
      this.storageManager = storageManager;
      this.postOffice = postOffice;
      this.queueSettingsRepository = queueSettingsRepository;
   }

   // MessagingComponent implementation

   public void start() throws Exception
   {
      if (started)
      {
         return;
      }
      timer = new Timer(true);
      task = new TxTimeoutHandler();
      timer.schedule(task, txTimeoutScanPeriod, txTimeoutScanPeriod);
      started = true;
   }

   public void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      if (timer != null)
      {
         task.cancel();

         task = null;

         timer.cancel();

         timer = null;
      }

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   // ResourceManager implementation ---------------------------------------------

   public Transaction getTransaction(final Xid xid)
   {
      return transactions.get(xid);
   }

   public boolean putTransaction(final Xid xid, final Transaction tx)
   {
      return transactions.putIfAbsent(xid, tx) == null;
   }

   public Transaction removeTransaction(final Xid xid)
   {
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
         //reset to default
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

   class TxTimeoutHandler extends TimerTask
   {
      public void run()
      {
         Set<Transaction> timedoutTransactions = new HashSet<Transaction>();

         long now = System.currentTimeMillis();

         for (Transaction tx : transactions.values())
         {
            if (tx.getState() != Transaction.State.PREPARED && now > (tx.getCreateTime() + timeoutSeconds * 1000))
            {
               transactions.remove(tx.getXid());
               log.warn("transaction with xid " + tx.getXid() + " timed out");
               timedoutTransactions.add(tx);
            }
         }

         for (Transaction failedTransaction : timedoutTransactions)
         {
            try
            {
               List<MessageReference> rolledBack = failedTransaction.timeout();
               Map<Queue, LinkedList<MessageReference>> queueMap = new HashMap<Queue, LinkedList<MessageReference>>();

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
            }
            catch (Exception e)
            {
               log.error("failed to timeout transaction, xid:" + failedTransaction.getXid(), e);
            }
         }
      }

   }
}
