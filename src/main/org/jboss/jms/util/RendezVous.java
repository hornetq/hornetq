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
package org.jboss.jms.util;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RendezVous
{
   // Constants -----------------------------------------------------

   public static final Logger log = Logger.getLogger(RendezVous.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean threadWaiting = false;
   private Object object;


   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * This method is used by the receiver thread, which registers with the redezvous and blocks
    * until an object becomes available. Only one thread can be waiting, if the method is called
    * by other threads while one is waiting, will throw unchecked exception.
    *
    * @param timeout - the timeout value in milliseconds. A zero timeout never expires, and the
    *        call blocks indefinitely.
    *
    * @return the object or null if the timeout expires and no Object was put.
    */
   public Object get(long timeout)
   {
      synchronized(this)
      {
         if (threadWaiting)
         {
            throw new RuntimeException("Another thread waiting!");
         }

         threadWaiting = true;

         try
         {
            wait(timeout);
         }
         catch(InterruptedException e)
         {
            log.warn(e);
         }
         finally
         {
            threadWaiting = false;
            Object tmp = object;
            object = null;
            if (log.isTraceEnabled()) { log.trace("get() returns " + tmp); }
            return tmp;
         }
      }
   }

   /**
    * Method used by the sender thread, that only puts the object IF there is a receiver thread
    * waiting.
    *
    * @return true if the object was transferred to the receiver thread, or false if there is
    *         no receiver thread waiting.
    */
   public boolean put(Object o)
   {
      synchronized(this)
      {
         if (!threadWaiting || object != null)
         {
            return false;
         }

         object = o;
         notifyAll();
         if (log.isTraceEnabled()) { log.trace("put(" + o + ")"); }
         return true;
      }
   }


   public synchronized boolean isOccupied()
   {
      return threadWaiting;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
