/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.paging.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.persistence.OperationContext;

/**
 * This will batch multiple calls waiting to perform a sync in a single call
 *
 * @author clebertsuconic
 *
 *
 */
public class PageSyncTimer
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PageSyncTimer.class);


   // Attributes ----------------------------------------------------
   
   private final PagingStore store;
   
   private final ScheduledExecutorService scheduledExecutor;
   
   private boolean pendingSync;
   
   private final long timeSync;
   
   private final Runnable runnable = new Runnable() 
   {
      public void run()
      {
         tick();
      }
   };
   
   private List<OperationContext> syncOperations = new LinkedList<OperationContext>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public PageSyncTimer(PagingStore store, ScheduledExecutorService scheduledExecutor, long timeSync)
   {
      this.store = store;
      this.scheduledExecutor = scheduledExecutor;
      this.timeSync = timeSync;
   }

   // Public --------------------------------------------------------

   public synchronized void addSync(OperationContext ctx)
   {
      ctx.pageSyncLineUp();
      if (!pendingSync)
      {
         pendingSync = true;
         scheduledExecutor.schedule(runnable, timeSync, TimeUnit.NANOSECONDS);
      }
      syncOperations.add(ctx);
   }
   
   private void tick()
   {
      OperationContext [] pendingSyncsArray;
      synchronized (this)
      {
         
         pendingSync = false;
         pendingSyncsArray = new OperationContext[syncOperations.size()];
         pendingSyncsArray = syncOperations.toArray(pendingSyncsArray);
         syncOperations.clear();
      }
      
      try
      {
         if (pendingSyncsArray.length != 0)
         {
            store.ioSync();
         }
      }
      catch (Exception e)
      {
         for (OperationContext ctx : pendingSyncsArray)
         {
            ctx.onError(HornetQException.IO_ERROR, e.getMessage());
         }
      }
      finally
      {
         // In case of failure, The context should propage an exception to the client
         // We send an exception to the client even on the case of a failure
         // to avoid possible locks and the client not getting the exception back
         for (OperationContext ctx : pendingSyncsArray)
         {
            ctx.pageSyncDone();
         }
      }
   }
   
   
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
