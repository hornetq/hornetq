/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.util;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RendezVous
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean waiting;
   private Object object;


   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * This method is used by the receiver thread, which registers with the redezvous and blocks
    * until an object becomes available. Only one thread can be waiting, if the method is called
    * by other threads while one is waiting, will throw unchecked exception.
    *
    * @return the object or null if the timeout expires and no Object was put.
    */
   public Object get(long timeout)
   {
      synchronized(this)
      {
         if (waiting)
         {
            throw new RuntimeException("Thread already waiting!");
         }

         waiting = true;

         try
         {
            wait(timeout);
         }
         catch(InterruptedException e)
         {
         }
         finally
         {
            waiting = false;
            Object tmp = object;
            object = null;
            return tmp;
         }
      }
   }

   /**
    * Method used by the sender thread, that only puts the object IF there is a receiver thread
    * waiting.
    * @return true if the object was transferred to the receiver thread, or false if there is
    *         no receiver thread waiting.
    */
   public boolean put(Object o)
   {
      synchronized(this)
      {
         if (!waiting)
         {
            return false;
         }

         object = o;
         notifyAll();
         return true;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
