/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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
