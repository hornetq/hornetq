/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class Lock
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private volatile Thread owner;

   // Constructors --------------------------------------------------

   public Lock()
   {
   }

   // Public --------------------------------------------------------

   public synchronized void acquire() throws Exception
   {
      if (owner != null)
      {
         throw new Exception("LOCKED by " + owner);
      }

      owner = Thread.currentThread();
   }

   public synchronized void release()
   {
      if (Thread.currentThread() == owner)
      {
         owner = null;
      }
   }

   public synchronized boolean isAcquired()
   {
      return owner != null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
