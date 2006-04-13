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

import java.util.Iterator;
import java.util.Set;

import org.jboss.logging.Logger;
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
public class Topic implements CoreDestination
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Topic.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private int fullSize;
   
   private int pageSize;
   
   private int downCacheSize;
   
   private boolean trace = log.isTraceEnabled();
   
   protected Router router;
   
   protected long destinationId;
   
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

   public boolean isQueue()
   {
      return false;
   }
   
   public int getFullSize()
   {
      return fullSize;
   }
   
   public int getPageSize()
   {
      return pageSize;
   }
   
   public int getDownCacheSize()
   {
      return downCacheSize;
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
