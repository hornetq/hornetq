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
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.message.MessageReference;
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
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class RemoteQueueStub implements ClusteredQueue
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RemoteQueueStub.class);

   // Static --------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private int nodeID;
   private String name;
   private long channelID;
   private Filter filter;
   private boolean recoverable;
   private PersistenceManager pm;
   private QueueStats stats;

   // Constructors --------------------------------------------------

   public RemoteQueueStub(int nodeId, String name, long id, boolean recoverable,
                          PersistenceManager pm, Filter filter)
   {
      this.nodeID = nodeId;
      this.name = name;
      this.channelID = id;
      this.recoverable = recoverable;
      this.pm = pm;
      this.filter = filter;
   }

   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver observer, MessageReference reference, Transaction tx)
   {
      if (trace) { log.trace(this + " handling " + reference); }

      if (filter != null && !filter.accept(reference.getMessage()))
      {
         if (trace) { log.trace(this + " rejecting " + reference + " because it doesn't match filter"); }
         return new SimpleDelivery(this, reference, false, false);
      }

      if (recoverable && reference.getMessage().isReliable())
      {
         try
         {
            // If the message is persistent and we are recoverable then we persist here, *before*
            // the message is sent across the network. This will increment any channelcount on the
            // message in storage.
            pm.addReference(channelID, reference, tx);
         }
         catch (Exception e)
         {
            log.error("Failed to add reference", e);
            return null;
         }
      }

      if (trace) { log.trace(this + " accepting " + reference + " for delivery"); }
      return new SimpleDelivery(this, reference);
   }

   // Distributor implementation ------------------------------------

   public boolean contains(Receiver receiver)
   {
      throw new UnsupportedOperationException();
   }

   public Iterator iterator()
   {
      throw new UnsupportedOperationException();
   }

   public boolean add(Receiver receiver)
   {
      // TODO Auto-generated method stub
      return false;
   }

   public boolean remove(Receiver receiver)
   {
      throw new UnsupportedOperationException();
   }

   public int getNumberOfReceivers()
   {
      throw new UnsupportedOperationException();
   }

   // DeliveryObserver implementation -------------------------------

   public void acknowledge(Delivery d, Transaction tx) throws Throwable
   {
      if (recoverable && d.getReference().getMessage().isReliable())
      {
         pm.removeReference(this.channelID, d.getReference(), tx);
      }
   }

   public void cancel(Delivery d) throws Throwable
   {
      throw new UnsupportedOperationException();
   }

   // Channel implemenation -----------------------------------------

   public long getChannelID()
   {
      return channelID;
   }

   public boolean isRecoverable()
   {
      return recoverable;
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

   public void deliver(boolean synchronous)
   {
      throw new UnsupportedOperationException();
   }

   public void close()
   {
   }

   public List delivering(Filter filter)
   {
      throw new UnsupportedOperationException();
   }

   public List undelivered(Filter filter)
   {
      throw new UnsupportedOperationException();
   }

   public void clear()
   {
      throw new UnsupportedOperationException();
   }

   public int getMessageCount()
   {
      throw new UnsupportedOperationException();
   }

   public void removeAllReferences() throws Throwable
   {
      throw new UnsupportedOperationException();
   }

   public void load() throws Exception
   {
   }

   public void unload() throws Exception
   {
   }

   public void activate()
   {
   }

   public void deactivate()
   {
   }

   public boolean isActive()
   {
      throw new UnsupportedOperationException();
   }

   public List recoverDeliveries(List messageIds)
   {
      throw new UnsupportedOperationException();
   }
   
   public int getDeliveringCount()
   {
      throw new UnsupportedOperationException();
   }

   public int getMaxSize()
   {
      throw new UnsupportedOperationException();
   }

   public int getMessagesAdded()
   {
      throw new UnsupportedOperationException();
   }
   
   public void setMaxSize(int newSize)
   {
      throw new UnsupportedOperationException();
   }
   
   public int getScheduledCount()
   {
      throw new UnsupportedOperationException();
   }


   // Queue implementation ------------------------------------------

   public String getName()
   {
      return name;
   }

   public Filter getFilter()
   {
      return filter;
   }

   public boolean isClustered()
   {
      return true;
   }

   // ClusteredQueue implementation ---------------------------------

   public QueueStats getStats()
   {
      return stats;
   }

   public int getNodeId()
   {
      return nodeID;
   }

   public boolean isLocal()
   {
      return false;
   }

   // Public --------------------------------------------------------

   public void setStats(QueueStats stats)
   {
      this.stats = stats;
   }

   public String toString()
   {
      return "RemoteQueueStub[" + channelID + "/" + name + " -> " + nodeID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
