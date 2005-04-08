/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.AbstractDestination;
import org.jboss.jms.delegate.ServerSessionDelegate;
import org.jboss.jms.client.remoting.NACKCallbackException;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.remoting.InvokerCallbackHandler;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.HandleCallbackException;

import java.io.Serializable;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;



/**
 * A Consumer endpoint. Lives on the boundary between Messaging Core and the JMS Facade.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Consumer implements Receiver
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Consumer.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String id;
   protected AbstractDestination destination;
   protected ServerSessionDelegate sessionEndpoint;
   protected InvokerCallbackHandler callbackHandler;

   // Constructors --------------------------------------------------

   public Consumer(String id, AbstractDestination destination,
                   InvokerCallbackHandler callbackHandler,
                   ServerSessionDelegate sessionEndpoint)
   {
      this.id = id;
      this.destination = destination;
      this.sessionEndpoint = sessionEndpoint;
      this.callbackHandler = callbackHandler;

      // register myself with the destination
      destination.add(this);
   }

   // Receiver implementation ---------------------------------------

   public Serializable getReceiverID()
   {
      return id;
   }

   public boolean handle(Routable r)
   {
      // I only accept messages if my connection is started
      if (!sessionEndpoint.getConnectionEndpoint().isStarted())
      {
         // nack
         return false;
      }

      if (log.isTraceEnabled()) { log.trace("receiving routable " + r + " from the core"); }

      // push the message to the client
      InvocationRequest req = new InvocationRequest(null, null, r, null, null, null);

      try
      {
         // TODO what happens if handling takes a long time?
         callbackHandler.handleCallback(req);
      }
      catch(HandleCallbackException e)
      {
         Throwable cause = e.getCause();

         // according http://jira.jboss.com/jira/browse/JBREM-93, the HandleCallbackException
         // always wraps the original exception that was generated on the client.
         if (cause instanceof NACKCallbackException)
         {
            return false;
         }
         log.error("The client failed to acknowledge the message", cause);
         return false;
      }

      return true;
   }

   // Public --------------------------------------------------------

   /**
    * The facade initiated a blocking wait, so this method makes sure that any messages hold by
    * the destination are delivered.
    *
    * @throws JBossJMSException - wraps an InterruptedException
    */
   public void initiateAsynchDelivery() throws JBossJMSException
   {
      // the delivery must be always initiate from another thread than the caller's. This is to
      // avoid the situation when the caller thread re-acquires reentrant locks in an in-VM
      // situation

      PooledExecutor threadPool =
            sessionEndpoint.getConnectionEndpoint().getServerPeer().getThreadPool();

      try
      {
         threadPool.execute(new Runnable()
         {
            public void run()
            {
               destination.deliver();
            }
         });
      }
      catch(InterruptedException e)
      {
         throw new JBossJMSException("interrupted asynchonous delivery", e);
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
