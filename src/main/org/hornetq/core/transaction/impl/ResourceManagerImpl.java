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

package org.hornetq.core.transaction.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;

/**
 * A ResourceManagerImpl
 *
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ResourceManagerImpl implements ResourceManager, HornetQComponent
{
   private static final Logger log = Logger.getLogger(ResourceManagerImpl.class);

   private final ConcurrentMap<Xid, Transaction> transactions = new ConcurrentHashMap<Xid, Transaction>();

   private List<HeuristicCompletionHolder> heuristicCompletions = new ArrayList<HeuristicCompletionHolder>();

   private final int defaultTimeoutSeconds;

   private volatile int timeoutSeconds;

   private boolean started = false;

   private TxTimeoutHandler task;

   private final long txTimeoutScanPeriod;

   private final ScheduledExecutorService scheduledThreadPool;
   
   public ResourceManagerImpl(final int defaultTimeoutSeconds, 
                              final long txTimeoutScanPeriod, 
                              final ScheduledExecutorService scheduledThreadPool)
   {
      this.defaultTimeoutSeconds = defaultTimeoutSeconds;
      this.timeoutSeconds = defaultTimeoutSeconds;
      this.txTimeoutScanPeriod = txTimeoutScanPeriod;
      this.scheduledThreadPool = scheduledThreadPool;
   }

   // HornetQComponent implementation

   public void start() throws Exception
   {
      if (started)
      {
         return;
      }
      task = new TxTimeoutHandler();
      Future<?> future = scheduledThreadPool.scheduleAtFixedRate(task, txTimeoutScanPeriod, txTimeoutScanPeriod, TimeUnit.MILLISECONDS);
      task.setFuture(future);
      
      started = true;
   }

   public void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      if (task != null)
      {
         task.close();
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
         // reset to default
         this.timeoutSeconds = defaultTimeoutSeconds;
      }
      else
      {
         this.timeoutSeconds = timeoutSeconds;
      }

      return true;
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

   public Map<Xid, Long> getPreparedTransactionsWithCreationTime()
   {
      List<Xid> xids = getPreparedTransactions();
      Map<Xid, Long> xidsWithCreationTime = new HashMap<Xid, Long>();

      for (Xid xid : xids)
      {
         xidsWithCreationTime.put(xid, transactions.get(xid).getCreateTime());
      }
      return xidsWithCreationTime;
   }
   
   public void putHeuristicCompletion(final long recordID, final Xid xid, final boolean isCommit)
   {
      heuristicCompletions.add(new HeuristicCompletionHolder(recordID, xid, isCommit));
   }
   
   public List<Xid>getHeuristicCommittedTransactions()
   {
      return getHeuristicCompletedTransactions(true);
   }
   
   public List<Xid>getHeuristicRolledbackTransactions()
   {
      return getHeuristicCompletedTransactions(false);
   }

   public long removeHeuristicCompletion(Xid xid)
   {
      Iterator<HeuristicCompletionHolder> iterator = heuristicCompletions.iterator();
      while (iterator.hasNext())
      {
         ResourceManagerImpl.HeuristicCompletionHolder holder = (ResourceManagerImpl.HeuristicCompletionHolder)iterator.next();
         if (holder.xid.equals(xid))
         {
            iterator.remove();
            return holder.recordID;
         }
      }
      return -1;
   }
   
   private List<Xid>getHeuristicCompletedTransactions(boolean isCommit)
   {
      List<Xid> xids = new ArrayList<Xid>();
      for (HeuristicCompletionHolder holder : heuristicCompletions)
      {
         if (holder.isCommit == isCommit)
         {
            xids.add(holder.xid);
         }
      }
      return xids;
   }

   class TxTimeoutHandler implements Runnable
   {
      private boolean closed = false;
      
      private Future<?> future;

      public void run()
      {
         if (closed)
         {
            return;
         }
         
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
               failedTransaction.rollback();
            }
            catch (Exception e)
            {
               log.error("failed to timeout transaction, xid:" + failedTransaction.getXid(), e);
            }
         }
      }

      synchronized void setFuture(Future<?> future)
      {
         this.future = future;
      }
      
      void close()
      {
         if (future != null)
         {
            future.cancel(false);
         }
         
         closed = true;
      }

   }
   
   private class HeuristicCompletionHolder
   {
      public final boolean isCommit;
      public final Xid xid;
      public final long recordID;

      public HeuristicCompletionHolder(long recordID, Xid xid, boolean isCommit)
      {
         this.recordID = recordID;
         this.xid = xid;
         this.isCommit = isCommit;
      }
   }
}
