/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting;

import org.jboss.logging.Logger;
import org.jboss.jms.perf.framework.protocol.KillRequest;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class Executor implements Context
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Executor.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private Lock lock;
   private Context delegator;

   // Constructors --------------------------------------------------

   public Executor(Context delegator)
   {
      lock = new Lock();
      this.delegator = delegator;
   }

   // Context implementation ----------------------------------------

   public boolean isColocated()
   {
      return delegator.isColocated();
   }

   // Public --------------------------------------------------------

   public Result execute(Request request) throws Exception
   {
      log.debug(delegator + " received " + request);

      // kill requests override the lock, so better know what you're doing

      if (request instanceof KillRequest)
      {
         if (lock.isAcquired())
         {
            log.warn("Executor currently executing job, killing regardless ...");
         }
         return request.execute(this);
      }
      else
      {
         try
         {
            lock.acquire();

            Result result = request.execute(this);

            log.debug(request + " executed successfully");

            return result;
         }
         catch(Exception e)
         {
            log.error(delegator + " failed to execute request", e);
            throw e;
         }
         finally
         {
            lock.release();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
