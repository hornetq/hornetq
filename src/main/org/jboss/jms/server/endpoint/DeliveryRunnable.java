/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;

import javax.jms.JMSException;

import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.messaging.core.Message;
import org.jboss.logging.Logger;

/**
 * A PooledExecutor job that contains the message to be delivered asynchronously to the client. The
 * delivery is always carried on a thread pool thread.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class DeliveryRunnable extends Callback implements Runnable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 8375144805659344430L;

   private static final Logger log = Logger.getLogger(DeliveryRunnable.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected transient InvokerCallbackHandler callbackHandler;
   protected transient ServerConnectionDelegate connection;

   // Constructors --------------------------------------------------

   public DeliveryRunnable(ServerConnectionDelegate connection,
                           InvokerCallbackHandler callbackHandler,
                           Message m)
   {
      super(m);
      this.callbackHandler = callbackHandler;
      this.connection = connection;
   }

   // Runnable implementation ---------------------------------------

   public void run()
   {
      try
      {
         if (log.isTraceEnabled()) { log.trace("handing the message " + this.getCallbackObject() + " over to the remoting layer"); }
         callbackHandler.handleCallback(this);
      }
      catch(Throwable t)
      {
         log.error("Failed to deliver the message to the client, closing connection", t);
         
         //Close the connection
         try
         {
            connection.close();
         }
         catch (JMSException e)
         {
            log.error("Failed to close connection", e);
         }
         
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
