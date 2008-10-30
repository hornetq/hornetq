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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;

/**
 * 
 * A ResourceManagerImpl
 * 
 * TODO - implement timeouts
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ResourceManagerImpl implements ResourceManager
{
   private final ConcurrentMap<Xid, Transaction> transactions = new ConcurrentHashMap<Xid, Transaction>();
   
   private final int defaultTimeoutSeconds;
   
   private volatile int timeoutSeconds;
   
   public ResourceManagerImpl(final int defaultTimeoutSeconds)
   {      
      this.defaultTimeoutSeconds = defaultTimeoutSeconds;
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
      
      return true;
   }

   public List<Xid> getPreparedTransactions()
   {
      List<Xid> xids = new ArrayList<Xid>();
      for (Xid xid : transactions.keySet())
      {
         if(transactions.get(xid).getState() == Transaction.State.PREPARED)
         {
            xids.add(xid);
         }
      }
      return xids;
   }
}
