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
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class PointToMultipointRouter implements Router
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PointToMultipointRouter.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   List receivers;

   // Constructors --------------------------------------------------

   public PointToMultipointRouter()
   {
      receivers = new ArrayList();
   }

   // Router implementation -----------------------------------------

   public Set handle(DeliveryObserver observer, Routable routable, Transaction tx)
   {
      Set deliveries = new HashSet();

      synchronized(receivers)
      {
         for(Iterator i = receivers.iterator(); i.hasNext(); )
         {
            Receiver receiver = (Receiver)i.next();

            try
            {
               Delivery d = receiver.handle(observer, routable, tx);

               if (log.isTraceEnabled()) { log.trace("receiver " + receiver + " handled  " + routable + " and returned " + d); }

               if (d != null)
               {
                  deliveries.add(d);
               }
            }
            catch(Throwable t)
            {
               // broken receiver - log the exception and ignore it
               log.error("The receiver " + receiver + " is broken", t);
            }
         }
      }
      return deliveries;
   }

   public boolean add(Receiver r)
   {
      synchronized(receivers)
      {
         if (receivers.contains(r))
         {
            return false;
         }
         receivers.add(r);
      }
      return true;
   }


   public boolean remove(Receiver r)
   {
      synchronized(receivers)
      {
         return receivers.remove(r);
      }
   }

   public void clear()
   {
      synchronized(receivers)
      {
         receivers.clear();
      }
   }

   public boolean contains(Receiver r)
   {
      synchronized(receivers)
      {
         return receivers.contains(r);
      }
   }

   public Iterator iterator()
   {
      synchronized(receivers)
      {
         return receivers.iterator();
      }
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
