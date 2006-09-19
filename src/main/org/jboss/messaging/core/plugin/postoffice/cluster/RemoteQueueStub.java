/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.util.Iterator;
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;

/**
 * A RemoteQueueStub
 * 
 * TODO to avoid having to implement a lot of methods that throw
 * UnsupportedOperationException should define an interface that only declares
 * the required methods and implement that
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class RemoteQueueStub implements ClusteredQueue
{
   private static final Logger log = Logger.getLogger(RemoteQueueStub.class);
      
   private String nodeId;
   
   private String name;
   
   private long id;
   
   private Filter filter;
   
   private boolean recoverable;
   
   private PersistenceManager pm;
   
   private QueueStats stats;
   
   RemoteQueueStub(String nodeId, String name, long id, boolean recoverable, PersistenceManager pm, Filter filter)
   {
      this.nodeId = nodeId;
      
      this.name = name;
      
      this.id = id;
      
      this.recoverable = recoverable;
      
      this.pm = pm;
      
      this.filter = filter;
   }
   
   public String getNodeId()
   {
      return nodeId;
   }
   
   public boolean isLocal()
   {
      return false;
   }
   
   public void setStats(QueueStats stats)
   {
      this.stats = stats;
   }
   
   public QueueStats getStats()
   {
      return stats;
   }
   
   public Delivery handle(DeliveryObserver observer, MessageReference reference, Transaction tx)
   {
      if (filter != null && !filter.accept(reference))
      {
         Delivery del = new SimpleDelivery(this, reference, false, false);
         
         return del;
      }
      
      if (recoverable && reference.isReliable())
      {
         try
         {
            //If the message is persistent and we are recoverable then we persist here, *before*
            //the message is sent across the network
            
            log.info("Adding ref: " + reference + " in channel " + id);
            
            pm.addReference(id, reference, tx);
         }
         catch (Exception e)
         {
            log.error("Failed to add reference", e);
            return null;
         }
      }
  
      return new SimpleDelivery(this, reference, false);      
   }

   public Filter getFilter()
   {
      return filter;
   }

   public String getName()
   {
      return name;
   }

   public boolean acceptReliableMessages()
   {
      return true;
   }

   public List browse()
   {
      throw new UnsupportedOperationException();
   }

   public List browse(Filter filter)
   {
      throw new UnsupportedOperationException();
   }

   public void clear()
   {
      throw new UnsupportedOperationException();
   }

   public void close()
   {
   }

   public void deliver(boolean synchronous)
   {
      throw new UnsupportedOperationException();
   }

   public List delivering(Filter filter)
   {
      throw new UnsupportedOperationException();
   }

   public long getChannelID()
   {
      return id;
   }

   public boolean isRecoverable()
   {
      return recoverable;
   }

   public int messageCount()
   {
      throw new UnsupportedOperationException();
   }

   public void removeAllReferences() throws Throwable
   {
      throw new UnsupportedOperationException();
   }

   public List undelivered(Filter filter)
   {
      throw new UnsupportedOperationException();
   }

   public void acknowledge(Delivery d, Transaction tx) throws Throwable
   {
      throw new UnsupportedOperationException();
   }

   public void cancel(Delivery d) throws Throwable
   {
      throw new UnsupportedOperationException();
   }

   public boolean add(Receiver receiver)
   {
      // TODO Auto-generated method stub
      return false;
   }

   public boolean contains(Receiver receiver)
   {
      throw new UnsupportedOperationException();
   }

   public Iterator iterator()
   {
      throw new UnsupportedOperationException();
   }

   public boolean remove(Receiver receiver)
   {
      throw new UnsupportedOperationException();
   }
   
   public int numberOfReceivers()
   {
      throw new UnsupportedOperationException();
   }

   public void activate()
   {
      throw new UnsupportedOperationException();
   }

   public void deactivate()
   {
      throw new UnsupportedOperationException();
   }

   public void load() throws Exception
   {
      throw new UnsupportedOperationException();
   }

   public void unload() throws Exception
   {
      throw new UnsupportedOperationException();
   }
   
   public boolean isActive()
   {
      throw new UnsupportedOperationException();
   }
   
}
