/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;

import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.messaging.core.Routable;
import org.jboss.logging.Logger;

/**
 * A PooledExecutor job that contains the message to be delivered asynchronously to the client. The
 * delivery is always carried on a thread pool thread.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
class Delivery extends Callback implements Runnable
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected transient Logger log;
   protected transient InvokerCallbackHandler callbackHandler;

   // Constructors --------------------------------------------------

   public Delivery(InvokerCallbackHandler callbackHandler, Routable r, Logger log)
   {
      super(r);
      this.callbackHandler = callbackHandler;
      this.log = log;
   }

   // Runnable implementation ---------------------------------------

   public void run()
   {
      try
      {
         if (log.isTraceEnabled()) { log.trace("sending the message to the client"); }
         callbackHandler.handleCallback(this);
      }
      catch(Throwable t)
      {
         log.error("Failed to deliver the message to the client", t);
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
