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

package org.hornetq.core.postoffice.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.server.Queue;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;

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

   private final Set<ByteArrayHolder> cache = new org.hornetq.utils.ConcurrentHashSet<ByteArrayHolder>();

   private final SimpleString address;

   // Note - deliberately typed as ArrayList since we want to ensure fast indexed
   // based array access
   private final ArrayList<Pair<ByteArrayHolder, Long>> ids;

   private int pos;

   private final int cacheSize;

   private final StorageManager storageManager;

   private final boolean persist;

   public DuplicateIDCacheImpl(final SimpleString address,
                               final int size,
                               final StorageManager storageManager,
                               final boolean persist)
   {
      this.address = address;

      cacheSize = size;

      ids = new ArrayList<Pair<ByteArrayHolder, Long>>(size);

      this.storageManager = storageManager;

      this.persist = persist;
   }

   public void load(final List<Pair<byte[], Long>> theIds) throws Exception
   {
      int count = 0;

      long txID = -1;

      for (Pair<byte[], Long> id : theIds)
      {
         if (count < cacheSize)
         {
            ByteArrayHolder bah = new ByteArrayHolder(id.a);

            Pair<ByteArrayHolder, Long> pair = new Pair<ByteArrayHolder, Long>(bah, id.b);

            cache.add(bah);

            ids.add(pair);
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

      pos = ids.size();

      if (pos == cacheSize)
      {
         pos = 0;
      }

   }

   public boolean contains(final byte[] duplID)
   {
      return cache.contains(new ByteArrayHolder(duplID));
   }

   public synchronized void addToCache(final byte[] duplID, final Transaction tx) throws Exception
   {
      long recordID = storageManager.generateUniqueID();

      if (tx == null)
      {
         if (persist)
         {
            storageManager.storeDuplicateID(address, duplID, recordID);
         }

         addToCacheInMemory(duplID, recordID);
      }
      else
      {
         if (persist)
         {
            storageManager.storeDuplicateIDTransactional(tx.getID(), address, duplID, recordID);

            tx.setContainsPersistent();
         }

         // For a tx, it's important that the entry is not added to the cache until commit (or prepare)
         // since if the client fails then resends them tx we don't want it to get rejected
         tx.addOperation(new AddDuplicateIDOperation(duplID, recordID));
      }
   }

   private synchronized void addToCacheInMemory(final byte[] duplID, final long recordID)
   {
      cache.add(new ByteArrayHolder(duplID));

      Pair<ByteArrayHolder, Long> id;

      if (pos < ids.size())
      {
         // Need fast array style access here -hence ArrayList typing
         id = ids.get(pos);

         cache.remove(id.a);

         // Record already exists - we delete the old one and add the new one
         // Note we can't use update since journal update doesn't let older records get
         // reclaimed
         id.a = new ByteArrayHolder(duplID);

         try
         {
            storageManager.deleteDuplicateID(id.b);
         }
         catch (Exception e)
         {
            DuplicateIDCacheImpl.log.warn("Error on deleting duplicate cache", e);
         }

         id.b = recordID;
      }
      else
      {
         id = new Pair<ByteArrayHolder, Long>(new ByteArrayHolder(duplID), recordID);

         ids.add(id);
      }

      if (pos++ == cacheSize - 1)
      {
         pos = 0;
      }
   }

   private class AddDuplicateIDOperation implements TransactionOperation
   {
      final byte[] duplID;

      final long recordID;

      volatile boolean done;

      AddDuplicateIDOperation(final byte[] duplID, final long recordID)
      {
         this.duplID = duplID;

         this.recordID = recordID;
      }

      private void process()
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

      public void afterCommit(final Transaction tx)
      {
         process();
      }

      public void afterPrepare(final Transaction tx)
      {
         process();
      }

      public void afterRollback(final Transaction tx)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#getDistinctQueues()
       */
      public Collection<Queue> getDistinctQueues()
      {
         return Collections.emptySet();
      }

   }

   private static final class ByteArrayHolder
   {
      ByteArrayHolder(final byte[] bytes)
      {
         this.bytes = bytes;
      }

      final byte[] bytes;

      int hash;

      @Override
      public boolean equals(final Object other)
      {
         if (other instanceof ByteArrayHolder)
         {
            ByteArrayHolder s = (ByteArrayHolder)other;

            if (bytes.length != s.bytes.length)
            {
               return false;
            }

            for (int i = 0; i < bytes.length; i++)
            {
               if (bytes[i] != s.bytes[i])
               {
                  return false;
               }
            }

            return true;
         }
         else
         {
            return false;
         }
      }

      @Override
      public int hashCode()
      {
         if (hash == 0)
         {
            for (byte b : bytes)
            {
               hash = 31 * hash + b;
            }
         }

         return hash;
      }
   }
}
