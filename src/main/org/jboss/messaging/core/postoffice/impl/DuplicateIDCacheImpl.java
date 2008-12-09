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

import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.DuplicateIDCache;
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
   private final Set<SimpleString> cache = new ConcurrentHashSet<SimpleString>();

   private final SimpleString address;
   
   //Note - deliberately typed as ArrayList since we want to ensure fast indexed
   //based array access
   private final ArrayList<Pair<SimpleString, Long>> ids;

   private int pos;
   
   private int cacheSize;
   
   private final StorageManager storageManager;

   public DuplicateIDCacheImpl(final SimpleString address, final int size, final StorageManager storageManager)
   {
      this.address = address;
      
      this.cacheSize = size;
      
      this.ids = new ArrayList<Pair<SimpleString, Long>>(size);
            
      this.storageManager = storageManager;
   }

   public void load(final List<Pair<SimpleString, Long>> theIds) throws Exception
   {
      int count = 0;
      
      long txID = -1;
      
      for (Pair<SimpleString, Long> id: ids)
      {
         if (count < cacheSize)
         {
            cache.add(id.a);
            
            ids.add(id);
         }
         else
         {
            //cache size has been reduced in config - delete the extra records
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

   public synchronized void addToCache(final SimpleString duplID, final long txID) throws Exception
   {
      cache.add(duplID);
      
      Pair<SimpleString, Long> id;
      
      long recordID = storageManager.generateUniqueID();
      
      if (pos < ids.size())
      {
         //Need fast array style access here -hence ArrayList typing
         id = ids.get(pos);
         
         cache.remove(id.a);
         
         //Record already exists - we delete the old one and add the new one
         //Note we can't use update since journal update doesn't let older records get
         //reclaimed
         id.a = duplID;
         
         if (txID == -1)
         {
            storageManager.deleteDuplicateID(id.b);
         }
         else
         {
            storageManager.deleteDuplicateIDTransactional(txID, id.b);
         }     
         
         id.b = recordID;
      }
      else
      {
         id = new Pair<SimpleString, Long>(duplID, recordID);
         
         ids.set(pos, id);
      }
 
      if (txID == -1)
      {
         storageManager.storeDuplicateID(address, duplID, recordID);
      }
      else
      {
         storageManager.storeDuplicateIDTransactional(txID, address, duplID, recordID);
      }       
     
      if (pos++ == cacheSize)
      {
         pos = 0;
      }
   }
}
