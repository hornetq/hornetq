/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;


import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;


import org.jboss.jms.client.Closeable;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.DurableSubscriptionHolder;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.Message;
import org.jboss.remoting.callback.InvokerCallbackHandler;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;


/**
 * A Consumer endpoint. Lives on the boundary between Messaging Core and the JMS Facade.
 *
 * It doesn't implement ConsumerDelegate because ConsumerDelegate's methods will never land on the
 * server side, they will be taken care of by the client-side interceptor chain.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerConsumerDelegate implements Receiver, Closeable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerDelegate.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String id;
   protected Channel destination;
   protected ServerSessionDelegate sessionEndpoint;
   protected InvokerCallbackHandler callbackHandler;
   protected boolean noLocal;
   protected Selector messageSelector;
   protected DurableSubscriptionHolder subscription;
	
   protected PooledExecutor threadPool;

   // <messageID-Delivery>
   private Map deliveries;


   // Constructors --------------------------------------------------

   public ServerConsumerDelegate(String id, Channel destination,
                                 InvokerCallbackHandler callbackHandler,
                                 ServerSessionDelegate sessionEndpoint,
                                 String selector,
                                 boolean noLocal,
                                 DurableSubscriptionHolder subscription)
      throws InvalidSelectorException
   {
      if (log.isTraceEnabled()) log.trace("Creating ServerConsumerDelegate:" + id + ", noLocal:" + noLocal);
      this.id = id;
      this.destination = destination;
      this.sessionEndpoint = sessionEndpoint;
      this.callbackHandler = callbackHandler;
      threadPool = sessionEndpoint.getConnectionEndpoint().getServerPeer().getThreadPool();
      this.noLocal = noLocal;
      if (selector != null)
      {
         if (log.isTraceEnabled()) log.trace("creating selector:" + selector);
         this.messageSelector = new Selector(selector);
         if (log.isTraceEnabled()) log.trace("created selector");
      }
      this.subscription = subscription;
      deliveries = new HashMap();
   }

   // Receiver implementation ---------------------------------------

   public Delivery handle(DeliveryObserver observer, Routable routable)
   {
      // deliver the message on a different thread than the core thread that brought it here
      if (log.isTraceEnabled()) log.trace(this.id + " received " + routable);

      Delivery delivery = null;

      try
      {
         Message message = routable.getMessage();
         if (log.isTraceEnabled()) { log.trace("dereferenced message: " + message); }

         boolean accept = this.accept(message);

         if (!accept)
         {
            if (log.isTraceEnabled()) { log.trace("consumer DOES NOT accept the message"); }
            return null;
         }

         delivery = new SimpleDelivery(observer, routable);
         deliveries.put(routable.getMessageID(), delivery);

         if (log.isTraceEnabled()) { log.trace("queueing the message " + message + " for delivery"); }
         threadPool.execute(new DeliveryRunnable(callbackHandler, message, log));
      }
      catch(InterruptedException e)
      {
         log.warn("Interrupted asynchronous delivery", e);
      }

      return delivery;
   }
   
   
   public boolean accept(Routable r)
   {
      boolean accept = true;
      if (messageSelector != null)
      {
         accept = messageSelector.accept(r);

         if (log.isTraceEnabled())
         {
            log.trace("message selector accepts the message");
         }
      }

      if (accept)
      {
         if (noLocal)
         {
            String conId = ((JBossMessage)r).getConnectionID();
            if (log.isTraceEnabled()) { log.trace("message connection id: " + conId); }
            if (conId != null)
            {
               if (log.isTraceEnabled()) { log.trace("current connection connection id: " + sessionEndpoint.connectionEndpoint.connectionID); }
               accept = !conId.equals(sessionEndpoint.connectionEndpoint.connectionID);
               if (log.isTraceEnabled()) { log.trace("accepting? " + accept); }
            }
         }
      }
      return accept;
   }
  

   // Closeable implementation --------------------------------------

   public void closing() throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace(this.id + " closing"); }
   }

   public void close() throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace(this.id + " close"); }
		this.setStarted(false);
      this.sessionEndpoint.connectionEndpoint.receivers.remove(id);
      this.sessionEndpoint.consumers.remove(id);
   }
   

   // Public --------------------------------------------------------

   void setStarted(boolean start)
   {     
      if (start)
      {
         if (log.isTraceEnabled()) log.trace("attempting to add receiver");

         boolean added = destination.add(this);
         if (log.isTraceEnabled()) log.trace("added receiver: " + this.id + ", success=" + added);
      }
      else
      {
         if (log.isTraceEnabled()) log.trace("attempting to remove receiver from destination: " + destination);

         boolean removed = destination.remove(this);
         if (log.isTraceEnabled()) log.trace("receiver " + (removed ? "" : "NOT ")  + "removed");

         for(Iterator i = deliveries.keySet().iterator(); i.hasNext(); )
         {
            Object messageID = i.next();
            Delivery d = (Delivery)deliveries.get(messageID);
            try
            {
               d.cancel();
            }
            catch(Throwable t)
            {
               log.error("Cannot cancel delivery: " + d, t);
            }
            i.remove();
         }
      }
   }
   
   void acknowledge(String messageID)
   {
      try
      {
         Delivery d = (Delivery)deliveries.get(messageID);
         d.acknowledge();
         deliveries.remove(messageID);
      }
      catch(Throwable t)
      {
         log.error("Message " + messageID + "cannot be acknowledged to the source");
      }
   }


   // Package protected ---------------------------------------------
   
   void redeliver() throws JMSException
   {
      // TODO I need to do this atomically, otherwise only some of the messages may be redelivered
      // TODO and some old deliveries may be lost

      if (log.isTraceEnabled()) { log.trace("redeliver"); }
      
      List old = new ArrayList();
      synchronized(deliveries)
      {

         for(Iterator i = deliveries.keySet().iterator(); i.hasNext();)
         {
            old.add(deliveries.get(i.next()));
            i.remove();
         }
      }
      
      if (log.isTraceEnabled()) { log.trace("There are " + old.size() + " deliveries to redeliver"); }

      for(Iterator i = old.iterator(); i.hasNext();)
      {
         try
         {
            Delivery d = (Delivery)i.next();
            d.redeliver(this);
         }
         catch(Throwable t)
         {
            String msg = "Failed to initiate redelivery";
            log.error(msg, t);
            throw new JMSException(msg);
         }
      }
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
