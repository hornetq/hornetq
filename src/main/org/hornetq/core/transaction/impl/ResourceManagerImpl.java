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

package org.hornetq.core.transaction.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.hornetq.core.server.MessagingComponent;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;

/**
 * A ResourceManagerImpl
 *
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ResourceManagerImpl implements ResourceManager, MessagingComponent
{
   private static final Logger log = Logger.getLogger(ResourceManagerImpl.class);

   private final ConcurrentMap<Xid, Transaction> transactions = new ConcurrentHashMap<Xid, Transaction>();

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

   // MessagingComponent implementation

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
}
