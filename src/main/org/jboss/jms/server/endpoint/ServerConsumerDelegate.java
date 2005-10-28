/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.jboss.jms.client.Closeable;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.selector.Selector;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.Subscription;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.remoting.callback.InvokerCallbackHandler;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;


/**
 * A Consumer endpoint. Lives on the boundary between Messaging Core and the JMS Facade.
 *
 * It doesn't implement ConsumerDelegate because ConsumerDelegate's methods will never land on the
 * server side, they will be taken care of by the client-side interceptor chain.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerConsumerDelegate implements Receiver, Filter, Closeable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerDelegate.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String id;
   
   protected Channel channel;
   
   protected ServerSessionDelegate sessionEndpoint;
   
   protected InvokerCallbackHandler callbackHandler;
   
   protected boolean noLocal;
   
   protected Selector messageSelector;
   
   protected PooledExecutor threadPool;
   
   protected boolean started;
   
   protected boolean disconnected = false;

   // deliveries must be maintained in order they were received
   private List deliveries;


   protected boolean closed;

   // List<Serializable> contains messageIDs
   private List messagesToReturn;


   // Constructors --------------------------------------------------

   public ServerConsumerDelegate(String id, Channel channel,
                                 InvokerCallbackHandler callbackHandler,
                                 ServerSessionDelegate sessionEndpoint,
                                 String selector, boolean noLocal)
      throws InvalidSelectorException
   {
      log.debug("creating ServerConsumerDelegate[" + id + "]");

      this.id = id;
      this.channel = channel;
      this.sessionEndpoint = sessionEndpoint;
      this.callbackHandler = callbackHandler;
      this.threadPool = sessionEndpoint.getConnectionEndpoint().getServerPeer().getThreadPool();
      this.noLocal = noLocal;
      if (selector != null)
      {
         if (log.isTraceEnabled()) log.trace("creating selector:" + selector);
         this.messageSelector = new Selector(selector);
         if (log.isTraceEnabled()) log.trace("created selector");
      }
      //this.subscription = subscription;
      this.deliveries = new ArrayList();
      this.started = sessionEndpoint.connectionEndpoint.started;
      this.channel.add(this);

   }

   // Receiver implementation ---------------------------------------

   public synchronized Delivery handle(DeliveryObserver observer, Routable reference, Transaction tx)
   {
      if (closed)
      {
         if (log.isTraceEnabled()) { log.trace("consumer " + this + " closed, rejecting message" ); }
         return null;
      }
      
      //If the consumer is stopped then we don't accept the message, it should go back into the channel
      //for delivery later
      if (!started)
      {
         return null;
      }
      
      //deliver the message on a different thread than the core thread that brought it here

      Delivery delivery = null;

      Message message = reference.getMessage();

      try
      {
         message = JBossMessage.copy((javax.jms.Message)message);
      }
      catch(JMSException e)
      {
         // TODO - review this, http://jira.jboss.org/jira/browse/JBMESSAGING-132
         String msg = "Cannot make a copy of the message";
         log.error(msg, e);
         throw new java.lang.IllegalStateException(msg);
      }

      if (log.isTraceEnabled()) { log.trace("dereferenced message: " + message); }

      boolean accept = this.accept(message);

      if (!accept)
      {
         if (log.isTraceEnabled()) { log.trace("consumer DOES NOT accept the message"); }
         return null;
      }

      if (reference.isRedelivered())
      {
         if (log.isTraceEnabled())
         {
            log.trace("Message is redelivered - setting jmsredelivered to true");
         }
         message.setRedelivered(true);
      }
      
      delivery = new SimpleDelivery(observer, (MessageReference)reference);
      deliveries.add(delivery);
      
      if (log.isTraceEnabled()) { log.trace("queueing message " + message + " for delivery"); }
      try
      {
         threadPool.execute(new DeliveryRunnable(this.sessionEndpoint.connectionEndpoint, callbackHandler, message));
      }
      catch (InterruptedException e)
      {
         log.warn("Thread interrupted", e);
         //Ignore
      }


      return delivery;
   }

   // Filter implementation -----------------------------------------
   
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

   public synchronized void closing() throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace(this.id + " closing"); }
   }

   public synchronized void close() throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Consumer is already closed");
      }

      if (log.isTraceEnabled()) { log.trace(this.id + " close"); }

      closed = true;

      if (messagesToReturn != null)
      {
         boolean canceled = false;
         for(Iterator i = deliveries.iterator(); i.hasNext();)
         {
            Delivery d = (Delivery)i.next();
            Object messageID = d.getReference().getMessageID();
            if (messagesToReturn.contains(messageID))
            {
               try
               {
                  if (log.isTraceEnabled()) { log.trace("returning message " + messageID + " to destination for redelivery"); }
                  d.cancel();
                  i.remove();
                  canceled = true;
               }
               catch(Throwable t)
               {
                  log.error(d + " cannot be canceled");
               }
            }
         }

         if (canceled)
         {
            channel.deliver();
         }
      }

      //On close we only disconnect the consumer from the Channel we don't actually remove it
      //This is because it may still contain deliveries that may well be acknowledged after
      //the consumer has closed. This is perfectly valid.
      disconnect();
      messagesToReturn = null;
   }
   
   synchronized void setStarted(boolean started)
   {
      if (log.isTraceEnabled()) { log.trace("setStarted: " + started); } 
      
      this.started = started;   
      
      if (started)
      {
         //need to prompt delivery
         channel.deliver();
      }
   }
   

   // Public --------------------------------------------------------

   /**
    * TODO this is a hack, replace with something smarter
    */
   public void setMessagesToReturnToDestination(List messageIDs)
   {
      if (log.isTraceEnabled()) { log.trace("marking " + (messageIDs == null ? "null " : Integer.toString(messageIDs.size()) )+ " messages to be returned to destination"); }

      this.messagesToReturn = messageIDs;
   }

   public String toString()
   {
      return "ServerConsumerDelegate[" + id + "]";
   }

   // Package protected ---------------------------------------------
  
   /** Actually remove the consumer and clear up any deliveries it may have */
   synchronized void remove() throws JMSException
   {
      if (log.isTraceEnabled()) log.trace("attempting to remove receiver " + this + " from destination " + channel);
 
      for(Iterator i = deliveries.iterator(); i.hasNext(); )
      {
         Delivery d = (Delivery)i.next();
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
      
      if (!disconnected)
      {
         close();
      }
      
      this.sessionEndpoint.connectionEndpoint.receivers.remove(id);

      if (this.channel instanceof Subscription)
      {
         ((Subscription)channel).closeConsumer(this.sessionEndpoint.serverPeer.getPersistenceManager());
      }
      
      this.sessionEndpoint.consumers.remove(this.id);
   }  
   
   
   
   void acknowledge(String messageID, Transaction tx)
   {
      if (log.isTraceEnabled()) { log.trace("acknowledging " + messageID); }

      try
      {
         for(Iterator i = deliveries.iterator(); i.hasNext(); )
         {
            Delivery d = (Delivery)i.next();
            d.acknowledge(tx);
            i.remove();
         }
      }
      catch(Throwable t)
      {
         log.error("Message " + messageID + "cannot be acknowledged to the source");
      }
   }
   
   void redeliver() throws JMSException
   {
      // TODO I need to do this atomically, otherwise only some of the messages may be redelivered
      // TODO and some old deliveries may be lost

      if (log.isTraceEnabled()) { log.trace("redeliver"); }                        
      
      List old = new ArrayList();
      synchronized(deliveries)
      {
         for(Iterator i = deliveries.iterator(); i.hasNext();)
         {
            Delivery d = (Delivery)i.next();
            old.add(d);
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
   
   /**
    * Disconnect this consumer from the Channel that feeds it.
    * This method does not clear up any deliveries
    *
    */
   protected void disconnect()
   {
      boolean removed = channel.remove(this);

      if (log.isTraceEnabled()) log.trace("receiver " + (removed ? "" : "NOT ")  + "removed");
      
      if (removed)
      {
         disconnected = true;
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
