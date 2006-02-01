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


import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Router;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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
   
   private boolean trace = log.isTraceEnabled();
   
   protected Router router;
   protected String name;
   protected MessageStore ms;

   // Constructors --------------------------------------------------

   public Topic(String name, MessageStore ms)
   {
      this.name = name;
      this.ms = ms;
      router = new PointToMultipointRouter();

      if (log.isTraceEnabled()) { log.trace(this + " created"); }
   }

   // CoreDestination implementation --------------------------------

   public String getName()
   {
      return name;
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

   // Public --------------------------------------------------------

   public String toString()
   {
      return "CoreTopic[" + getName() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
