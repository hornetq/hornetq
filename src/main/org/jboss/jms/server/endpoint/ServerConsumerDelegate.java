/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;

import java.io.Serializable;

import javax.jms.JMSException;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.DurableSubscriptionHolder;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.AbstractDestination;
import org.jboss.remoting.InvokerCallbackHandler;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;




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
   protected boolean noLocal;
   protected Selector messageSelector;
   protected DurableSubscriptionHolder subscription;
	
   protected PooledExecutor threadPool;

   // Constructors --------------------------------------------------

   public ServerConsumerDelegate(String id, AbstractDestination destination,
                                 InvokerCallbackHandler callbackHandler,
                                 ServerSessionDelegate sessionEndpoint,
                                 String selector,
                                 boolean noLocal,
                                 DurableSubscriptionHolder subscription)
      throws JMSException
   {
      this.id = id;
      this.destination = destination;
      this.sessionEndpoint = sessionEndpoint;
      this.callbackHandler = callbackHandler;
      threadPool = sessionEndpoint.getConnectionEndpoint().getServerPeer().getThreadPool();
      this.noLocal = noLocal;
      if (selector != null)
      {
         messageSelector = new Selector(selector);
      }
      this.subscription = subscription;
   }

   // Receiver implementation ---------------------------------------

   public Serializable getReceiverID()
   {
      return id;
   }

   public boolean handle(Routable r)
   {
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
   
   
   public boolean accept(Routable r)
   {
      boolean accept = true;
      if (messageSelector != null)
      {
         accept = messageSelector.accept(r);
      }
      if (accept)
      {
         if (noLocal)
         {
            String conId =
               ((JBossMessage)r).getConnectionID();
            if (conId != null)
            {
               accept = conId.equals(sessionEndpoint.connectionEndpoint.connectionID);
            }
         }
      }
      return accept;
   }
  

   // Closeable implementation --------------------------------------

   public void closing() throws JMSException
   {
      
   }

   public void close() throws JMSException
   {
		this.setStarted(false);
      this.sessionEndpoint.connectionEndpoint.receivers.remove(id);
      if (subscription != null)
      {
         subscription.setHasConsumer(false);
      }
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
   
   void acknowledge(String messageID)
   {
      //We acknowledge on the destination itself
      destination.acknowledge(messageID, this.getReceiverID());
   }

   // Package protected ---------------------------------------------
   

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
