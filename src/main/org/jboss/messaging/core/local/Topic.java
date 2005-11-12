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


import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Router;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.tx.Transaction;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class Topic implements CoreDestination
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected Router router;
   protected String name;

   // Constructors --------------------------------------------------

   public Topic(String name)
   {
      this.name = name;
      router = new PointToMultipointRouter();
   }

   // Public --------------------------------------------------------
   
   public Delivery handle(DeliveryObserver sender, Routable r, Transaction tx)
   {
      router.handle(sender, r, tx);
      return new SimpleDelivery(true);
   }

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
      return router.remove(receiver);
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
