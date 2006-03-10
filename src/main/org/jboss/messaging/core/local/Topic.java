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
package org.jboss.messaging.core.local;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Router;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.tx.Transaction;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox"jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class Topic implements CoreDestination, ManageableTopic
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Topic.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   protected Router router;
   
   protected long destinationId;
   
   private int fullSize;
   private int pageSize;
   private int downCacheSize;
   
   // Constructors --------------------------------------------------
   
   public Topic(long id, int fullSize, int pageSize, int downCacheSize)
   {
      router = new PointToMultipointRouter();
      
      if (log.isTraceEnabled()) { log.trace(this + " created"); }
      
      this.destinationId = id;
      
      this.fullSize = fullSize;
      this.pageSize = pageSize;
      this.downCacheSize = downCacheSize;
   }

   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver sender, Routable r, Transaction tx)
   {
      if (trace){ log.trace(this + " handles " + r + (tx == null ? " non-transactionally" : " in transaction: " + tx) ); }

      Set deliveries = router.handle(sender, r, tx);

      // must insure that all deliveries are completed, otherwise we risk losing messages
      for(Iterator i = deliveries.iterator(); i.hasNext(); )
      {
         Delivery d = (Delivery)i.next();
         if (d == null || !d.isDone())
         {
            log.error("the point to multipoint delivery was not completed correctly: " + d);
            throw new IllegalStateException("Incomplete delivery");
         }
      }
      
      return new SimpleDelivery(true);   
   }

   // Distributor implementation --------------------------------------

   public boolean add(Receiver receiver)
   {
      return router.add(receiver);
   }

   public void clear()
   {
      router.clear();
   }

   public boolean contains(Receiver receiver)
   {
      return router.contains(receiver);
   }

   public Iterator iterator()
   {
      return router.iterator();
   }

   public boolean remove(Receiver receiver)
   {
      boolean removed = router.remove(receiver);
      if (trace) { log.trace(this + (removed ? " removed " : " did NOT remove ") + receiver); }
      return removed;
   }
   
   // CoreDestination implementation -------------------------------
   
   public long getId()
   {
      return destinationId;
   }

   /**
    * @see CoreDestination#getFullSize()
    */
   public int getFullSize()
   {
      return fullSize;
   }
   
   /**
    * @see CoreDestination#getPageSize()
    */
   public int getPageSize()
   {
      return pageSize;
   }
   
   /**
    * @see CoreDestination#getDownCacheSize()
    */
   public int getDownCacheSize()
   {
      return downCacheSize;
   }
   
   // ManageableTopic implementation --------------------------------
   
   /**
    * @see ManageableCoreDestination#removeAllMessages()
    */
   public void removeAllMessages()
   {
      // XXX How to lock down all subscriptions?
      Iterator iter = iterator();
      while (iter.hasNext())
      {
         Object sub = iter.next();
         ((CoreSubscription)sub).removeAllMessages();
      }
   }
 
   /**
    * @see ManageableTopic#subscriptionCount()
    */
   public int subscriptionCount()
   {
      int count = 0;
      Iterator iter = iterator();
      while (iter.hasNext())
      {
         count++;
         iter.next();
      }
      return count;
   }

   /**
    * @see ManageableTopic#subscriptionCount(boolean)
    */
   public int subscriptionCount(boolean durable)
   {
      int count = 0;
      Iterator iter = iterator();
      while (iter.hasNext())
      {
         Object sub = iter.next();
         if ((sub instanceof CoreDurableSubscription) ^ (!durable))
            count++;
      }
      return count;
   }
   
   /**
    * @see ManageableTopic#getSubscriptions()
    */
   public List getSubscriptions()
   {
      ArrayList list = new ArrayList();
      Iterator iter = iterator();
      while (iter.hasNext())
      {
         CoreSubscription sub = (CoreSubscription)iter.next();
         if (sub instanceof CoreDurableSubscription)
            list.add(new String[]{ 
                        Long.toString(sub.getChannelID()), 
                        ((CoreDurableSubscription)sub).getClientID(),
                        ((CoreDurableSubscription)sub).getName()});
         else
            list.add(new String[]{ 
                  Long.toString(sub.getChannelID()), "", ""});
      }
      return list;
   }
   
   /**
    * @see ManageableTopic#getSubscriptions(boolean)
    */
   public List getSubscriptions(boolean durable)
   {
      ArrayList list = new ArrayList();
      Iterator iter = iterator();
      while (iter.hasNext())
      {
         CoreSubscription sub = (CoreSubscription)iter.next();
         if (sub instanceof CoreDurableSubscription && durable)
            list.add(new String[]{ 
                        Long.toString(sub.getChannelID()), 
                        ((CoreDurableSubscription)sub).getClientID(),
                        ((CoreDurableSubscription)sub).getName()});
         else if (!(sub instanceof CoreDurableSubscription) && !durable)
            list.add(new String[]{ 
                  Long.toString(sub.getChannelID()), "", ""});
      }
      return list;
   }
   
   // Public --------------------------------------------------------

   public String toString()
   {
      return "CoreTopic[" + destinationId + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
