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


import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.jms.IllegalStateException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.plugin.contract.ThreadPool;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.server.subscription.Subscription;
import org.jboss.jms.util.MessagingJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.SingleReceiverDelivery;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionException;
import org.jboss.messaging.core.tx.TxCallback;

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

   private boolean trace = log.isTraceEnabled();

   private int id;

   private Channel channel;

   private ServerSessionEndpoint sessionEndpoint;

   private boolean noLocal;

   private Selector messageSelector;

   private ThreadPool threadPoolDelegate;

   private volatile boolean started;

   private boolean disconnected = false;

   // deliveries must be maintained in order they were received
   private Map deliveries;

   private boolean closed;

   private boolean active;

   private boolean grabbing;

   private MessageProxy toGrab;

   private DeliveryCallback deliveryCallback;

   private boolean selectorRejected;
   
   private JBossDestination destination;

   // We record the id of the last message delivered to the client consumer
   private long lastMessageIDDelivered = -1;

   // Constructors --------------------------------------------------

   protected ServerConsumerEndpoint(int id, Channel channel,
                                    ServerSessionEndpoint sessionEndpoint,
                                    String selector, boolean noLocal, JBossDestination dest)
                                    throws InvalidSelectorException
   {
      if (trace) { log.trace("creating consumer endpoint " + id); }

      this.id = id;
      this.channel = channel;
      this.sessionEndpoint = sessionEndpoint;
      this.threadPoolDelegate =
         sessionEndpoint.getConnectionEndpoint().getServerPeer().getThreadPoolDelegate();
      this.noLocal = noLocal;
      this.destination = dest;

      if (selector != null)
      {
         if (trace) log.trace("creating selector:" + selector);
         this.messageSelector = new Selector(selector);
         if (trace) log.trace("created selector");
      }

      this.deliveries = new LinkedHashMap();
      this.started = this.sessionEndpoint.getConnectionEndpoint().isStarted();

      // adding the consumer to the channel
      this.channel.add(this);

      log.debug(this + " created");
   }

   // Receiver implementation ---------------------------------------

   // There is no need to synchronize this method. The channel synchronizes delivery to its
   // consumers
   public Delivery handle(DeliveryObserver observer, Routable reference, Transaction tx)
   {
      if (trace) { log.trace(this + " receives reference " + reference.getMessageID() + " for delivery"); }

      MessageReference ref = (MessageReference)reference;

      if (!isReady())
      {
         if (trace) { log.trace(this + " rejects reference with ID " + ref.getMessageID()); }
         return null;
      }

      try
      {
         Delivery delivery = null;

         JBossMessage message = (JBossMessage)ref.getMessage();

         // TODO - We need to put the message in a DLQ
         // For now we just ack it otherwise the message will keep being retried and we'll never get
         // anywhere
         if (ref.getDeliveryCount() > MAX_DELIVERY_ATTEMPTS)
         {
            log.warn(message + " has exceed maximum delivery attempts and will be removed");
            delivery = new SimpleDelivery(observer, ref, true);
            return delivery;
         }

         selectorRejected = !this.accept(message);

         delivery = new SimpleDelivery(observer, ref, false, !selectorRejected);
         deliveries.put(new Long(ref.getMessageID()), delivery);

         if (selectorRejected)
         {
            // we "arrest" the message so we can get the next one
            // TODO this DOES NOT scale. With a poor usage pattern, we may end with a lot of
            // arrested messages sitting here for nothing. Review this:
            // http://jira.jboss.org/jira/browse/JBMESSAGING-275
            if (trace) { log.trace(this + " DOES NOT accept the message because the selector rejected it"); }

            // ... however, keep asking for messages, the fact that this one wasn't accepted doesn't
            // mean that the next one it won't.

            return delivery;
         }

         // We don't send the message as-is, instead we create a MessageProxy instance. This allows
         // local fields such as deliveryCount to be handled by the proxy but global data to be
         // fielded by the same underlying Message instance. This allows us to avoid expensive
         // copying of messages

         MessageProxy md = JBossMessage.createThinDelegate(message, ref.getDeliveryCount());

         if (!grabbing)
         {
            // We want to asynchronously deliver the message to the consumer. Deliver the message on
            // a different thread than the core thread that brought it here.

            try
            {
               if (trace) { log.trace("queueing message " + message + " for delivery to client"); }
               threadPoolDelegate.execute(
                  new DeliveryRunnable(md, id, sessionEndpoint.getConnectionEndpoint(), trace));
            }
            catch (InterruptedException e)
            {
               log.warn("Thread interrupted", e);
            }
         }
         else
         {
            // The message is being "grabbed" and returned for receiveNoWait semantics
            toGrab = md;
         }

         lastMessageIDDelivered = md.getMessage().getMessageID();

         return delivery;
      }
      finally
      {
         // reset the "active" state, but only if the current message hasn't been rejected by
         // selector, because otherwise we want to get more messages
         // TODO this is a kludge that will be cleared by http://jira.jboss.org/jira/browse/JBMESSAGING-275
         if (!selectorRejected)
         {
            active = false;
            grabbing = false;
         }
      }
   }

   // Filter implementation -----------------------------------------

   public boolean accept(Routable r)
   {
      boolean accept = true;
      if (this.destination.isQueue())
      {
         //For subscriptions message selection is handled in the Subscription itself
         //we do not want to do the check twice
         if (messageSelector != null)
         {
            accept = messageSelector.accept(r);
   
            if (trace) { log.trace("message selector " + (accept ? "accepts " :  "DOES NOT accept ") + "the message"); }
         }
      }

      if (accept)
      {
         if (noLocal)
         {
            int conId = ((JBossMessage)r).getConnectionID();
            if (trace) { log.trace("message connection id: " + conId); }

            if (trace) { log.trace("current connection connection id: " + sessionEndpoint.getConnectionEndpoint().getConnectionID()); }
            accept = conId != sessionEndpoint.getConnectionEndpoint().getConnectionID();
            if (trace) { log.trace("accepting? " + accept); }

         }
      }
      return accept;
   }


   // Closeable implementation --------------------------------------

   public void closing() throws JMSException
   {
      if (trace) { log.trace(this + " closing"); }
   }

   public void close() throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Consumer is already closed");
      }

      if (trace) { log.trace(this + " close"); }

      closed = true;

      // On close we only disconnect the consumer from the Channel we don't actually remove it
      // This is because it may still contain deliveries that may well be acknowledged after
      // the consumer has closed. This is perfectly valid.
      disconnect();

      JMSDispatcher.instance.unregisterTarget(new Integer(id));
      
      //If it's a subscription, remove it
      if (channel instanceof Subscription)
      {
         Subscription sub = (Subscription)channel;
         try
         {
            if (!sub.isRecoverable())
            {
               //We don't disconnect durable subs
               sub.disconnect();
            }
         }
         catch (Exception e)
         {
            throw new MessagingJMSException("Failed to disconnect", e);
         }
      }           
   }

   // ConsumerEndpoint implementation -------------------------------

   public void cancelDelivery(long messageID) throws JMSException
   {
      SingleReceiverDelivery del = (SingleReceiverDelivery)deliveries.remove(new Long(messageID));
      if (del != null)
      {
         try
         {
            del.cancel();
         }
         catch (Throwable t)
         {
            throw new MessagingJMSException("Failed to cancel delivery " + del, t);
         }
         promptDelivery();
      }
      else
      {
         throw new IllegalStateException("Cannot find delivery to cancel:" + messageID);
      }
   }

   public void cancelDeliveries(List messageIDs) throws JMSException
   {
      //Cancel in reverse order to preserve order in queue

      for (int i = messageIDs.size() - 1; i >= 0; i--)
      {
         Long id = (Long)messageIDs.get(i);

         cancelDelivery(id.longValue());
      }
   }

   /**
    * We attempt to get the message directly fron the channel first. If we find one, we return that.
    * Otherwise, if wait = true, we register as being interested in receiving a message
    * asynchronously, then return and wait for it on the client side.
    */
   public MessageProxy getMessageNow(boolean wait) throws JMSException
   {
      synchronized (channel)
      {
         try
         {
            grabbing = true;

            // This will always deliver a message (if there is one) on the same thread
            promptDelivery();

            if (wait && toGrab == null)
            {
               active = true;
            }

            return toGrab;
         }
         finally
         {
            toGrab = null;
            grabbing = false;
         }
      }
   }

   public long deactivate() throws JMSException
   {
      synchronized (channel)
      {
         active = false;
         if (trace) { log.trace(this + " deactivated"); }

         return lastMessageIDDelivered;
      }
   }
   
   public void activate() throws JMSException
   {
      synchronized (channel)
      {
         if (closed)
         {
            //Do nothing
            return;
         }
         
         active = true;
         if (trace) { log.trace(this + " just activated"); }
         
         promptDelivery();
      }
   }
   
   // Public --------------------------------------------------------
   
   public String toString()
   {
      return "ConsumerEndpoint[" + id + "]" + (active ? "(active)" : "");
   }
   
   public JBossDestination getDestination()
   {
      return destination;
   }
   
   public ServerSessionEndpoint getSessionEndpoint()
   {
      return sessionEndpoint;
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   /**
    * Actually remove the consumer and clear up any deliveries it may have
    * */
   protected void remove() throws JMSException
   {
      if (trace) log.trace("attempting to remove receiver " + this + " from destination " + channel);
      
      for(Iterator i = deliveries.values().iterator(); i.hasNext(); )
      {
         SingleReceiverDelivery d = (SingleReceiverDelivery)i.next();
         try
         {
            d.cancel();
         }
         catch(Throwable t)
         {
            throw new MessagingJMSException("Failed to cancel delivery", t);
         }
      }
      deliveries.clear();
      
      if (!disconnected)
      {
         close();
      }
      
      sessionEndpoint.getConnectionEndpoint().
         getServerPeer().removeConsumerEndpoint(new Integer(id));                  
            
      sessionEndpoint.removeConsumerEndpoint(id);
   }  
   
   protected void acknowledgeAll() throws JMSException
   {
      // acknowledge all "pending" deliveries, except the ones corresponding to messages rejected
      // by selector, which are cancelled
      try
      {     
         for(Iterator i = deliveries.values().iterator(); i.hasNext(); )
         {
            SingleReceiverDelivery d = (SingleReceiverDelivery)i.next();

            //TODO - Selector kludge - remove this
            if (d.isSelectorAccepted())
            {
               d.acknowledge(null);
            }
            else
            {
               d.cancel();
            }
         }

         deliveries.clear();
      }
      catch(Throwable t)
      {
         throw new MessagingJMSException("Failed to acknowledge deliveries", t);
      }
   }
   
   
   protected void acknowledgeTransactionally(long messageID, Transaction tx) throws JMSException
   {
      if (trace) { log.trace("acknowledging " + messageID); }
      
      SingleReceiverDelivery d = null;
              
      // The actual removal of the deliveries from the delivery list is deferred until tx commit
      d = (SingleReceiverDelivery)deliveries.get(new Long(messageID));
      if (deliveryCallback == null)
      {
         deliveryCallback = new DeliveryCallback();
         tx.addCallback(deliveryCallback);
      }
      deliveryCallback.addMessageID(messageID);
         
      
      if (d != null)
      {
         try
         {
            d.acknowledge(tx);
         }
         catch(Throwable t)
         {
            throw new MessagingJMSException("Message " + messageID +
                                            "cannot be acknowledged to the source", t);
         } 
      }
      else
      {
         throw new IllegalStateException("Failed to acknowledge delivery " + d);
      }       
   }
   
   protected void removeDelivery(String messageID) throws JMSException
   {      
      if (deliveries.remove(messageID) == null)
      {
         throw new IllegalStateException("Cannot find delivery to remove:" + messageID);
      }      
   }
   
   protected void cancelAllDeliveries() throws JMSException
   {
      if (trace) { log.trace(this + " cancels deliveries"); }
           
      // Need to cancel starting at the end of the list and working to the front in order that the
      // messages end up back in the correct order in the channel.
      
      List toCancel = new ArrayList();
      
      Iterator iter = deliveries.values().iterator();
      while (iter.hasNext())
      {
         SingleReceiverDelivery d = (SingleReceiverDelivery)iter.next();
         toCancel.add(d);
      }
      
      for (int i = toCancel.size() - 1; i >= 0; i--)
      {   
         SingleReceiverDelivery d = (SingleReceiverDelivery)toCancel.get(i);
         try
         {
            d.cancel();      
            if (trace) { log.trace(d +  " canceled"); }
         }
         catch(Throwable t)
         {
            log.error("Failed to cancel delivery: " + d, t);
         }     
      }     
      
      deliveries.clear();
      promptDelivery();      
   }
   
   protected void setStarted(boolean started)
   {
      if (trace) { log.trace(this + (started ? " started" : " stopped")); }
      
      this.started = started;   
      
      if (started)
      {
         //need to prompt delivery   
         promptDelivery();
      }
   }
   
   protected void promptDelivery()
   {
      if (active || grabbing)
      {
         // prompt delivery in a loop, since this consumer may "arrest" a message not accepted
         // by the selector, while it still wants to get the next one
         // TODO this is a kludge that will be cleared by http://jira.jboss.org/jira/browse/JBMESSAGING-275
         while(true)
         {
            if (trace) { log.trace(this + " prompts delivery"); }

            selectorRejected = false;

            channel.deliver(this);

            if (!selectorRejected)
            {
               break;
            }
         }
      }      
   }
   
   /**
    * Disconnect this consumer from the Channel that feeds it. This method does not clear up
    * deliveries, except the "arrested" ones
    */
   protected void disconnect()
   {
      // clean up "arrested" deliveries, no acknowledgment will ever come for them
      for(Iterator i = deliveries.values().iterator(); i.hasNext(); )
      {
         SingleReceiverDelivery d = (SingleReceiverDelivery)i.next();
         if (!d.isSelectorAccepted())
         {
            try
            {
               d.cancel();
            }
            catch(Throwable t)
            {
               log.error("Failed to cancel delivery " + d, t);
            }
         }
      }

      boolean removed = channel.remove(this);
      
      if (removed)
      {
         disconnected = true;
         if (trace) { log.trace(this + " removed from the channel"); }
      }
   }
   
   /*
    * Do we want to handle the message? (excluding filter check)
    */
   protected boolean isReady()
   {
      // If the client side consumer is not ready to accept a message and have it sent to it
      // or we're not grabbing a message for receiveNoWait we return null to refuse the message
      if (!active && !grabbing)
      {
         if (trace) { log.trace(this + " not ready"); }
         return false;
      }
      
      if (closed)
      {
         if (trace) { log.trace(this + " closed"); }
         return false;
      }
      
      // If the consumer is stopped then we don't accept the message, it should go back into the
      // channel for delivery later.
      if (!started)
      {
         // this is a common programming error, make this visible in the debug logs
         // TODO: anaylize performance implications
         log.debug(this + " NOT started yet!");
         return false;
      }
      
      //TODO nice all the message headers and properties are in the reference we can do the 
      //filter check in here too.
      
      return true;
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
   
   class DeliveryCallback implements TxCallback
   {
      List delList = new ArrayList();
      
      public void beforePrepare()
      {         
         //NOOP
      }
      
      public void beforeCommit(boolean onePhase)
      {         
         //NOOP
      }
      
      public void beforeRollback(boolean onePhase)
      {         
         //NOOP
      }
      
      public void afterPrepare()
      {         
         //NOOP
      }
      
      public void afterCommit(boolean onePhase) throws TransactionException
      {
         //We remove the deliveries from the delivery map
         Iterator iter = delList.iterator();
         while (iter.hasNext())
         {
            Long messageID = (Long)iter.next();
            
            if (deliveries.remove(messageID) == null)
            {
               throw new TransactionException("Failed to remove delivery " + messageID);
            }
         }
         deliveryCallback = null;
      }
      
      public void afterRollback(boolean onePhase) throws TransactionException
      { 
         // Cancel the deliveries
         // Need to be cancelled in reverse order to maintain ordering
         Iterator iter = delList.iterator();
         while (iter.hasNext())
         {
            Long messageID = (Long)iter.next();
            
            SimpleDelivery del;
            
            if ((del = (SimpleDelivery)deliveries.remove(messageID)) == null)
            {
               throw new TransactionException("Failed to remove delivery " + messageID);
            }
            
            //Cancel the delivery
            try
            {
               del.cancel();
            }
            catch (Throwable t)
            {
               throw new TransactionException("Failed to cancel delivery " + del, t);
            }
         }
         
         deliveryCallback = null;
      }
      
      void addMessageID(long messageID)
      {
         delList.add(new Long(messageID));
      }
   }
   
}
