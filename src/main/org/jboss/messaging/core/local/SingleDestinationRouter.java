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

import org.jboss.messaging.core.Router;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.logging.Logger;

import java.util.Set;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class SingleDestinationRouter implements Router
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SingleDestinationRouter.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Receiver receiver;

   // Constructors --------------------------------------------------

   // Router implementation -----------------------------------------

   public synchronized Set handle(DeliveryObserver observer, Routable routable, Transaction tx)
   {
      if (receiver == null)
      {
         return Collections.EMPTY_SET;
      }

      Set deliveries = new HashSet(1);
      try
      {
         Delivery d = receiver.handle(observer, routable, tx);

         if (d != null && !d.isCancelled())
         {
            deliveries.add(d);
         }
      }
      catch(Throwable t)
      {
         // broken receiver - log the exception and ignore it
         log.error("The receiver " + receiver + " is broken" + t);
      }
      return deliveries;
   }

   public synchronized boolean add(Receiver r)
   {
      if (receiver == r)
      {
         return false;
      }
      receiver = r;
      return true;
   }


   public synchronized boolean remove(Receiver r)
   {
      boolean result = false;
      if (receiver == r)
      {
         result = true;
      }
      receiver = null;
      return result;
   }

   public synchronized void clear()
   {
      receiver = null;
   }

   public synchronized boolean contains(Receiver r)
   {
      return receiver == r;
   }

   public synchronized Iterator iterator()
   {
      if (receiver == null)
      {
         return Collections.EMPTY_LIST.iterator();
      }
      List l = new ArrayList();
      l.add(receiver);
      return l.iterator(); 
   }




   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
