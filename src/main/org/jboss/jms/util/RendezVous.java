/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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
