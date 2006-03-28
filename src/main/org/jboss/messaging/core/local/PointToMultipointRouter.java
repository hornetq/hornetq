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

import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class PointToMultipointRouter implements Router
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PointToMultipointRouter.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   List receivers;
   
   ReadWriteLock lock;

   // Constructors --------------------------------------------------

   public PointToMultipointRouter()
   {
      receivers = new ArrayList();
      
      lock = new WriterPreferenceReadWriteLock();
   }

   // Router implementation -----------------------------------------
   
   public Set handle(DeliveryObserver observer, Routable routable, Transaction tx)
   {
      Set deliveries = new HashSet();

      boolean acquired;
      try
      {
         lock.readLock().acquire();
         acquired = true;
      }
      catch (InterruptedException e)
      {
         throw new RuntimeException("Failed to obtain lock", e);
      }
      
      try
      {      
         for(Iterator i = receivers.iterator(); i.hasNext(); )
         {
            Receiver receiver = (Receiver)i.next();

            try
            {
               Delivery d = receiver.handle(observer, routable, tx);

               if (trace) { log.trace("receiver " + receiver + " handled " + routable + " and returned " + d); }

               if (d != null && !d.isCancelled())
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
      finally
      {
         if (acquired)
         {
            lock.readLock().release();
         }
      }
      
      return deliveries;
   }

   public boolean add(Receiver r)
   {
      boolean acquired;
      try
      {
         lock.writeLock().acquire();
         acquired = true;
      }
      catch (InterruptedException e)
      {
         throw new RuntimeException("Failed to obtain lock", e);
      }
      try
      {
         if (receivers.contains(r))
         {
            return false;
         }
         receivers.add(r);
      
         return true;
      }
      finally
      {
         if (acquired)
         {
            lock.writeLock().release();
         }
      }
   }


   public boolean remove(Receiver r)
   {
      boolean acquired;
      try
      {
         lock.writeLock().acquire();
         acquired = true;
      }
      catch (InterruptedException e)
      {
         throw new RuntimeException("Failed to obtain lock", e);
      }
      try
      {
         return receivers.remove(r);
      }
      finally
      {
         if (acquired)
         {
            lock.writeLock().release();
         }
      }     
   }

   public void clear()
   {
      boolean acquired;
      try
      {
         lock.writeLock().acquire();
         acquired = true;
      }
      catch (InterruptedException e)
      {
         throw new RuntimeException("Failed to obtain lock", e);
      }
      try
      {
         receivers.clear();
      }
      finally
      {
         if (acquired)
         {
            lock.writeLock().release();
         }
      }
   }

   public boolean contains(Receiver r)
   {
      boolean acquired;
      try
      {
         lock.writeLock().acquire();
         acquired = true;
      }
      catch (InterruptedException e)
      {
         throw new RuntimeException("Failed to obtain lock", e);
      }
      try
      {
         return receivers.contains(r);      
      }
      finally
      {
         if (acquired)
         {
            lock.writeLock().release();
         }
      }
   }

   public Iterator iterator()
   {
      boolean acquired;
      try
      {
         lock.writeLock().acquire();
         acquired = true;
      }
      catch (InterruptedException e)
      {
         throw new RuntimeException("Failed to obtain lock", e);
      }
      try
      {
         return receivers.iterator();
      }
      finally
      {
         if (acquired)
         {
            lock.writeLock().release();
         }
      }
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
