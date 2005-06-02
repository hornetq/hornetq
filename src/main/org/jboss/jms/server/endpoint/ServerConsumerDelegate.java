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
import org.jboss.jms.util.JBossJMSException;
import org.jboss.jms.client.Closeable;
import org.jboss.remoting.InvokerCallbackHandler;

import java.io.Serializable;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;

import javax.jms.JMSException;



/**
 * A Consumer endpoint. Lives on the boundary between Messaging Core and the JMS Facade.
 *
 * It doesn't implement ConsumerDelegate because ConsumerDelegate's methods will never land on the
 * server side, they will be taken care of by the client-side interceptor chain.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerConsumerDelegate implements Receiver, Closeable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerDelegate.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String id;
   protected AbstractDestination destination;
   protected ServerSessionDelegate sessionEndpoint;
   protected InvokerCallbackHandler callbackHandler;
	
   protected PooledExecutor threadPool;

   // Constructors --------------------------------------------------

   public ServerConsumerDelegate(String id, AbstractDestination destination,
                                 InvokerCallbackHandler callbackHandler,
                                 ServerSessionDelegate sessionEndpoint)
   {
      this.id = id;
      this.destination = destination;
      this.sessionEndpoint = sessionEndpoint;
      this.callbackHandler = callbackHandler;
      threadPool = sessionEndpoint.getConnectionEndpoint().getServerPeer().getThreadPool();
   }

   // Receiver implementation ---------------------------------------

   public Serializable getReceiverID()
   {
      return id;
   }

   public boolean handle(Routable r)
   {
      if (log.isTraceEnabled()) { log.trace("receiving routable " + r + " from the core"); }

      // deliver the message on a different thread than the core thread that brought it here

      try
      {
         threadPool.execute(new Delivery(callbackHandler, r, log));
      }
      catch(InterruptedException e)
      {
         log.warn("Interrupted asynchronous delivery", e);
      }

      // always NACK the message; it will be asynchronously ACKED later
      return false;
   }
	
  

   // Closeable implementation --------------------------------------

   public void closing() throws JMSException
   {
      
   }

   public void close() throws JMSException
   {
		this.setStarted(false);
   }
   

   // Public --------------------------------------------------------

   void setStarted(boolean s)
   {
      if (s)
      {
         destination.add(this);
      }
      else
      {
         destination.remove(id);
      }
   }

   /**
    * @deprecated
    *
    * TODO get rid of it
    * 
    * The facade initiated a blocking wait, so this method makes sure that any messages hold by
    * the destination are delivered.    *
    *
    * @throws JBossJMSException - wraps an InterruptedException
    */
   public void initiateAsynchDelivery() throws JBossJMSException
   {

      // the delivery must be always initiate from another thread than the caller's. This is to
      // avoid the situation when the caller thread re-acquires reentrant locks in an in-VM
      // situation
      try
      {
         threadPool.execute(new Runnable()
         {
            public void run()
            {
               if (log.isTraceEnabled()) { log.trace(id + " initiates asynchronous delivery on " + destination.getReceiverID()); };
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
