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
import org.jboss.messaging.core.transaction.TransactionPropertyIndexes;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.SimpleString;

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

   private final Set<ByteArrayHolder> cache = new org.jboss.messaging.utils.ConcurrentHashSet<ByteArrayHolder>();

   private final SimpleString address;

   // Note - deliberately typed as ArrayList since we want to ensure fast indexed
   // based array access
   private final ArrayList<Pair<ByteArrayHolder, Long>> ids;

   private int pos;

   private int cacheSize;

   private final StorageManager storageManager;

   private final boolean persist;

   
   public DuplicateIDCacheImpl(final SimpleString address,
                               final int size,
                               final StorageManager storageManager,
                               final boolean persist)
   {
      this.address = address;

      this.cacheSize = size;

      this.ids = new ArrayList<Pair<ByteArrayHolder, Long>>(size);

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

      pos = theIds.size();
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

            tx.putProperty(TransactionPropertyIndexes.CONTAINS_PERSISTENT, true);
         }

         // For a tx, it's important that the entry is not added to the cache until commit (or prepare)
         // since if the client fails then resends them tx we don't want it to get rejected
         tx.addOperation(new AddDuplicateIDOperation(duplID, recordID));
      }
   }

   private void addToCacheInMemory(final byte[] duplID, final long recordID) throws Exception
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

         storageManager.deleteDuplicateID(id.b);

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
   
   private static final class ByteArrayHolder
   {
      ByteArrayHolder(final byte[] bytes)
      {
         this.bytes = bytes;
      }

      final byte[] bytes;

      int hash;

      public boolean equals(Object other)
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

      public int hashCode()
      {
         if (hash == 0)
         {
            for (int i = 0; i < bytes.length; i++)
            {
               hash = 31 * hash + bytes[i];
            }
         }

         return hash;
      }
   }
}
