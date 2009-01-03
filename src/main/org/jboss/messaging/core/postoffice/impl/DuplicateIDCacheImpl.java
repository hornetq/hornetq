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

package org.jboss.messaging.core.postoffice.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.DuplicateIDCache;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.TransactionOperation;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

/**
 * A DuplicateIDCacheImpl
 * 
 * A fixed size rotating cache of last X duplicate ids.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 8 Dec 2008 16:35:55
 *
 *
 */
public class DuplicateIDCacheImpl implements DuplicateIDCache
{
   private static final Logger log = Logger.getLogger(DuplicateIDCacheImpl.class);
   
   public static volatile boolean debug;
   
   private static Set<DuplicateIDCacheImpl> caches = new ConcurrentHashSet<DuplicateIDCacheImpl>();

   public static void dumpCaches()
   {
      for (DuplicateIDCacheImpl cache : caches)
      {
         log.info("Dumping cache for address: " + cache.address);
         log.info("First the set:");
         for (SimpleString duplID : cache.cache)
         {
            log.info(duplID);
         }
         log.info("End set");
         log.info("Now the list:");
         for (Pair<SimpleString, Long> id : cache.ids)
         {
            log.info(id.a + ":" + id.b);
         }
         log.info("End dump");
      }
   }

   private final Set<SimpleString> cache = new ConcurrentHashSet<SimpleString>();

   private final SimpleString address;

   // Note - deliberately typed as ArrayList since we want to ensure fast indexed
   // based array access
   private final ArrayList<Pair<SimpleString, Long>> ids;

   private int pos;

   private int cacheSize;

   private final StorageManager storageManager;
   
   private final boolean persist;
   
   public DuplicateIDCacheImpl(final SimpleString address, final int size, final StorageManager storageManager,
                               final boolean persist)
   {
      this.address = address;

      this.cacheSize = size;

      this.ids = new ArrayList<Pair<SimpleString, Long>>(size);

      this.storageManager = storageManager;
      
      this.persist = persist;
      
      if (debug)
      {
         caches.add(this);
      }
   }

   protected void finalize() throws Throwable
   {
      if (debug)
      {
         caches.remove(this);
      }

      super.finalize();
   }

   public void load(final List<Pair<SimpleString, Long>> theIds) throws Exception
   {
      int count = 0;

      long txID = -1;
      
      for (Pair<SimpleString, Long> id : theIds)
      {
         if (count < cacheSize)
         {
            cache.add(id.a);
            
            ids.add(id);
         }
         else
         {
            // cache size has been reduced in config - delete the extra records
            if (txID == -1)
            {
               txID = storageManager.generateUniqueID();
            }

            storageManager.deleteDuplicateIDTransactional(txID, id.b);
         }

         count++;
      }

      if (txID != -1)
      {
         storageManager.commit(txID);
      }

      pos = theIds.size();
   }

   public boolean contains(final SimpleString duplID)
   {
      return cache.contains(duplID);
   }
   
   public synchronized void addToCache(final SimpleString duplID) throws Exception
   {
      long recordID = storageManager.generateUniqueID();
      
      if (persist)
      {
         storageManager.storeDuplicateID(address, duplID, recordID);
      }

      addToCacheInMemory(duplID, recordID);
   }

   public synchronized void addToCache(final SimpleString duplID, final Transaction tx) throws Exception
   {
      long recordID = storageManager.generateUniqueID();

      if (persist)
      {
         tx.addDuplicateID(address, duplID, recordID);
      }

      // For a tx, it's important that the entry is not added to the cache until commit (or prepare)
      // since if the client fails then resends them tx we don't want it to get rejected
      tx.addOperation(new AddDuplicateIDOperation(duplID, recordID));      
   }

   private void addToCacheInMemory(final SimpleString duplID, final long recordID) throws Exception
   {
      cache.add(duplID);

      Pair<SimpleString, Long> id;

      if (pos < ids.size())
      {
         // Need fast array style access here -hence ArrayList typing
         id = ids.get(pos);

         cache.remove(id.a);

         // Record already exists - we delete the old one and add the new one
         // Note we can't use update since journal update doesn't let older records get
         // reclaimed
         id.a = duplID;
         
         storageManager.deleteDuplicateID(id.b);

         id.b = recordID;
      }
      else
      {
         id = new Pair<SimpleString, Long>(duplID, recordID);

         ids.add(id);
      }

      if (pos++ == cacheSize - 1)
      {
         pos = 0;
      }
   }

   private class AddDuplicateIDOperation implements TransactionOperation
   {      
      final SimpleString duplID;

      final long recordID;

      volatile boolean done;

      AddDuplicateIDOperation(final SimpleString duplID, final long recordID)
      {
         this.duplID = duplID;

         this.recordID = recordID;
      }

      private void process() throws Exception
      {
         if (!done)
         {
            addToCacheInMemory(duplID, recordID);

            done = true;
         }
      }
      
      public void beforeCommit(final Transaction tx) throws Exception
      {
      }

      public void beforePrepare(final Transaction tx) throws Exception
      {
      }

      public void beforeRollback(final Transaction tx) throws Exception
      {
      }

      public void afterCommit(final Transaction tx) throws Exception
      {
         process();
      }

      public void afterPrepare(final Transaction tx) throws Exception
      {
         process();
      }

      public void afterRollback(final Transaction tx) throws Exception
      {
      }

   }
}
