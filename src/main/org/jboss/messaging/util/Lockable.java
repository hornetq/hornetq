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
package org.jboss.messaging.util;

import EDU.oswego.cs.dl.util.concurrent.ReentrantLock;
import org.jboss.logging.Logger;

/**
 * A simple wrapper around a ReentrantLock that provides a locking mechanism to subclasses.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Lockable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Lockable.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private ReentrantLock lock = new ReentrantLock();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   /**
    * The thread calling this method acquires a reentrant lock on the current instance. The method
    * can wait forever if the lock is being hold by another thread and not released.
    */
   public final void lock()
   {
      while(true)
      {
         try
         {
            lock.acquire();
            if (log.isTraceEnabled()) { log.trace("locked " + this); }
            return;
         }
         catch(InterruptedException e)
         {
            log.warn("failed to acquire " + this + "'s lock", e);
         }
      }
   }

   /**
    * The thread calling attempts to acquire the reentrant lock for the specified time.
    * @return true if succesful.
    */
   public final boolean lock(long millisecs)
   {
      long t1 = System.currentTimeMillis();
      while(true)
      {
         try
         {
            boolean locked = lock.attempt(millisecs - System.currentTimeMillis() + t1);
            if (log.isTraceEnabled()) { log.trace((locked ? "locked " : "could not acquire lock on ") + this); }
            return locked;
         }
         catch(InterruptedException e)
         {
            log.warn(e);
         }
      }
   }


   /**
    * The thread calling it gives up the lock on the current instance, if any.
    */
   public final void unlock()
   {
      lock.release();
      if (log.isTraceEnabled()) { log.trace("unlocked " + this); }
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
