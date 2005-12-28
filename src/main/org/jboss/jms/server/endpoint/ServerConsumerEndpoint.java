/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.jms.server.endpoint;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.jboss.aop.Dispatcher;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.SingleReceiverDelivery;
import org.jboss.messaging.core.local.Subscription;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.remoting.callback.InvokerCallbackHandler;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;


/**
 * Concrete implementation of ConsumerEndpoint. Lives on the boundary between Messaging Core and the
 * JMS Facade.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerConsumerEndpoint implements Receiver, Filter, ConsumerEndpoint
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 535443606137461274L;
   
   private static final Logger log = Logger.getLogger(ServerConsumerEndpoint.class);
   
   // Static --------------------------------------------------------
   
   private static final int MAX_DELIVERY_ATTEMPTS = 10;
   
   // Attributes ----------------------------------------------------
   
   protected String id;
   
   protected Channel channel;
   
   protected ServerSessionEndpoint sessionEndpoint;
   
   protected InvokerCallbackHandler callbackHandler;
   
   protected boolean noLocal;
   
   protected Selector messageSelector;
   
   protected PooledExecutor threadPool;
   
   protected volatile boolean started;
   
   protected boolean disconnected = false;
   
   // deliveries must be maintained in order they were received
   private List deliveries;
   
   protected volatile boolean closed;
   
   protected volatile boolean ready;
   
   protected volatile boolean grabbing;
   
   protected Message toGrab;
   
   //protected QueuedExecutor executor = new QueuedExecutor();
   
   // Constructors --------------------------------------------------
   
   ServerConsumerEndpoint(String id, Channel channel,
         InvokerCallbackHandler callbackHandler,
         ServerSessionEndpoint sessionEndpoint,
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
      
      this.deliveries = new ArrayList();
      
      this.started = sessionEndpoint.connectionEndpoint.started;
      
      this.channel.add(this);
      
         }
   
   // Receiver implementation --------------------------------------- 
   
   public synchronized Delivery handle(DeliveryObserver observer, Routable reference, Transaction tx)
   {
      if (log.isTraceEnabled()) { log.trace("Attempting to handle ref: " + reference.getMessageID()); }
      
      if (!wantReference())
      {
         return null;
      }
      
      try
      {
         
         if (log.isTraceEnabled()) { log.trace("Delivering ref " + reference.getMessageID()); }
         
         Delivery delivery = null;
         Message message = reference.getMessage();
         message.setDeliveryCount(reference.getDeliveryCount());
         
         
         boolean accept = this.accept(message);
         if (!accept)
         {
            if (log.isTraceEnabled()) { log.trace("consumer DOES NOT accept the message"); }
            return null;
         }
         
         //TODO - We need to put the message in a DLQ
         //For now we just ack it otherwise the message will keep being retried
         //and we'll never get anywhere
         if (reference.getDeliveryCount() > MAX_DELIVERY_ATTEMPTS)
         {
            log.warn("Message has exceed maximum delivery attempts and will be removed " + message);
            delivery = new SimpleDelivery(observer, (MessageReference)reference, true);
            return delivery;
         }                         
         
         delivery = new SimpleDelivery(observer, (MessageReference)reference);                  
         deliveries.add(delivery);
         
         if (!grabbing)
         {
            //We want to asynchronously deliver the message to the consumer
            //deliver the message on a different thread than the core thread that brought it here
            
            try
            {
               if (log.isTraceEnabled()) { log.trace("queueing message " + message + " for delivery to client"); }
               threadPool.execute(new DeliveryRunnable(callbackHandler, message));
            }
            catch (InterruptedException e)
            {
               log.warn("Thread interrupted", e);
            }
         }
         else
         {
            //The message is being "grabbed" and returned for receiveNoWait semantics
            toGrab = message;
         }
         
         return delivery;     
      }
      finally
      {
         ready = false;
         grabbing = false;
      }
   }
   
   // Filter implementation -----------------------------------------
   
   public boolean accept(Routable r)
   {
      boolean accept = true;
      if (messageSelector != null)
      {
         accept = messageSelector.accept(r);
         
         if (log.isTraceEnabled()) { log.trace("message selector " + (accept ? "accepts " :  "DOES NOT accept ") + "the message"); }
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
      
      //On close we only disconnect the consumer from the Channel we don't actually remove it
      //This is because it may still contain deliveries that may well be acknowledged after
      //the consumer has closed. This is perfectly valid.
      disconnect();
      
      Dispatcher.singleton.unregisterTarget(this.id);
   }
   
   // ConsumerEndpoint implementation -------------------------------
   
   public synchronized void cancelMessage(Serializable messageID) throws JMSException
   {      
      boolean cancelled = false;
      Iterator iter = deliveries.iterator();
      try
      {         
         while (iter.hasNext())
         {
            SingleReceiverDelivery del = (SingleReceiverDelivery)iter.next();
            if (del.getReference().getMessageID().equals(messageID))
            {
               if (del.cancel())
               {
                  cancelled = true;
                  iter.remove();
                  break;
               }
               else
               {
                  throw new JMSException("Failed to cancel delivery: " + messageID);
               }
            }
         }
      }
      catch (Throwable t)
      {
         throw new JBossJMSException("Failed to cancel message", t);
      }
      if (!cancelled)
      {
         throw new IllegalStateException("Cannot find delivery to cancel");
      }
      else
      {         
         promptDelivery();
      }
   }
   
   /**
    * We attempt to get the message directly fron the channel first. If we find one, we return that.
    * Otherwise, we register as being interested in receiving a message asynchronously, then return
    * and wait for it on the client side.
    */
   public synchronized javax.jms.Message getMessageNow() throws JMSException
   {           
      try
      {
         grabbing = true;
         
         //This will always deliver a message (if there is one) on the same thread
         promptDelivery();
         
         javax.jms.Message ret = (javax.jms.Message)toGrab;
         
         return ret;
      }
      finally
      {
         toGrab = null;
         
         grabbing = false;
      }               
   }
   
   public synchronized void deactivate() throws JMSException
   {
      ready = false;
      
      if (log.isTraceEnabled()) { log.trace("set ready to false"); }
   }
   
   
   public synchronized void activate() throws JMSException
   {
      if (closed)
      {
         //Do nothing
         return;
      }
      
      ready = true;
      
      promptDelivery();
   }
   
   // Public --------------------------------------------------------
   
   
   
   public String toString()
   {
      return "ServerConsumerDelegate[" + id + "]";
   }
   
   // Package protected ---------------------------------------------
   
   /**
    * Actually remove the consumer and clear up any deliveries it may have
    * */
   synchronized void remove() throws JMSException
   {
      if (log.isTraceEnabled()) log.trace("attempting to remove receiver " + this + " from destination " + channel);
      
      for(Iterator i = deliveries.iterator(); i.hasNext(); )
      {
         SingleReceiverDelivery d = (SingleReceiverDelivery)i.next();
         try
         {
            d.cancel();
         }
         catch(Throwable t)
         {
            throw new JBossJMSException("Failed to cancel delivery", t);
         }
         i.remove();
      }
      
      if (!disconnected)
      {
         close();
      }
      
      this.sessionEndpoint.connectionEndpoint.consumers.remove(id);
      
      if (this.channel instanceof Subscription)
      {
         ((Subscription)channel).closeConsumer(this.sessionEndpoint.serverPeer.getPersistenceManager());
      }
      
      this.sessionEndpoint.consumers.remove(this.id);
   }  
   
   synchronized void acknowledgeAll() throws JMSException
   {
      try
      {     
         for(Iterator i = deliveries.iterator(); i.hasNext(); )
         {
            SingleReceiverDelivery d = (SingleReceiverDelivery)i.next();
            d.acknowledge(null);
            i.remove();            
         }
      }
      catch(Throwable t)
      {
         throw new JBossJMSException("Failed to acknowledge deliveries", t);
      }
   }
   
   synchronized void acknowledge(String messageID, Transaction tx) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("acknowledging " + messageID); }
      
      try
      {
         for(Iterator i = deliveries.iterator(); i.hasNext(); )
         {
            SingleReceiverDelivery d = (SingleReceiverDelivery)i.next();
            if (d.getReference().getMessageID().equals(messageID))
            {
               d.acknowledge(tx);
               i.remove();
            }
         }
      }
      catch(Throwable t)
      {
         throw new JBossJMSException("Message " + messageID + "cannot be acknowledged to the source", t);
      }
   }
   
   synchronized void cancelAllDeliveries() throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace(this + " cancels deliveries"); }
      
      //Need to cancel starting at the end of the list and working to the front
      //in order that the messages end up back in the correct order in the channel
      
      for (int i = deliveries.size() - 1; i >= 0; i--)
      {   
         SingleReceiverDelivery d = (SingleReceiverDelivery)deliveries.get(i);
         try
         {
            boolean cancelled = d.cancel();
            if (!cancelled)
            {
               throw new JMSException("Failed to cancel delivery:" + d.getReference().getMessageID());
            }
            
            if (log.isTraceEnabled()) { log.trace(d +  " canceled"); }
         }
         catch(Throwable t)
         {
            log.error("Cannot cancel delivery: " + d, t);
         }            
      }
      deliveries.clear();
      promptDelivery();
   }
   
   synchronized void setStarted(boolean started)
   {
      if (log.isTraceEnabled()) { log.trace("setStarted: " + started); } 
      
      this.started = started;   
      
      if (started)
      {
         //need to prompt delivery   
         promptDelivery();
      }
   }
   
   // Protected -----------------------------------------------------
   
   protected void promptDelivery()
   {
      if (log.isTraceEnabled()) { log.trace("promptDelivery:" + this); }
      if (ready || grabbing)
      {
         channel.redeliver(this);
      }      
   }
   
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
   
   /*
    * Do we want to handle the message? (excluding filter check)
    */
   protected boolean wantReference()
   {
      //If the client side consumer is not ready to accept a message and have it sent to it
      //or we're not grabbing a message for receiveNoWait
      //we return null to refuse the message
      if (!ready && !grabbing)
      {
         if (log.isTraceEnabled()) { log.trace("Not ready for message so returning null"); }
         return false;
      }
      
      if (closed)
      {
         if (log.isTraceEnabled()) { log.trace("consumer " + this + " closed, rejecting message" ); }
         return false;
      }
      
      // If the consumer is stopped then we don't accept the message, it should go back into the
      // channel for delivery later.
      if (!started)
      {
         return false;
      }
      
      //TODO nice all the message headers and properties are in the reference we can do the 
      //filter check in here too.
      
      return true;
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
  
}
