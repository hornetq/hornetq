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

import javax.jms.IllegalStateException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.destination.TopicService;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.server.remoting.MessagingMarshallable;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.remoting.Invoker;
import org.jboss.remoting.Client;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.HandleCallbackException;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;

/**
 * Concrete implementation of ConsumerEndpoint.
 * 
 * Lives on the boundary between Messaging Core and the JMS Facade. Handles delivery of messages
 * from the server to the client side consumer.
 * 
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerConsumerEndpoint implements Receiver, ConsumerEndpoint
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerEndpoint.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private int id;

   private Channel messageQueue;
   
   private String queueName;

   private ServerSessionEndpoint sessionEndpoint;
   
   private ServerInvokerCallbackHandler callbackHandler;
   
   private byte versionToUse;

   private boolean noLocal;

   private Selector messageSelector;

   private JBossDestination destination;
   
   private Queue dlq;
   
   private Queue expiryQueue;
   
   private long redeliveryDelay;
   
   private boolean started;
   
   //This lock protects starting and stopping
   private Object startStopLock;

   // Must be volatile
   private volatile boolean clientAccepting;

   // Constructors ---------------------------------------------------------------------------------

   ServerConsumerEndpoint(int id, Channel messageQueue, String queueName,
                          ServerSessionEndpoint sessionEndpoint,
                          String selector, boolean noLocal, JBossDestination dest,
                          Queue dlq, Queue expiryQueue, long redeliveryDelay)
                          throws InvalidSelectorException
   {
      if (trace) { log.trace("constructing consumer endpoint " + id); }

      this.id = id;
      
      this.messageQueue = messageQueue;
      
      this.queueName = queueName;
      
      this.sessionEndpoint = sessionEndpoint;
      
      this.callbackHandler = sessionEndpoint.getConnectionEndpoint().getCallbackHandler();
      
      this.versionToUse = sessionEndpoint.getConnectionEndpoint().getUsingVersion();
            
      this.noLocal = noLocal;
      
      this.destination = dest;
      
      this.dlq = dlq;
      
      this.redeliveryDelay = redeliveryDelay;
      
      this.expiryQueue = expiryQueue;
      
      //Always start as false - wait for consumer to initiate
      this.clientAccepting = false;
      
      this.startStopLock = new Object();
      
      if (selector != null)
      {
         if (trace) log.trace("creating selector:" + selector);
         this.messageSelector = new Selector(selector);
         if (trace) log.trace("created selector");
      }
       
      this.started = this.sessionEndpoint.getConnectionEndpoint().isStarted();

      // adding the consumer to the queue
      this.messageQueue.add(this);
      
      // prompt delivery
      promptDelivery();

      log.debug(this + " constructed");
   }

   // Receiver implementation ----------------------------------------------------------------------

   /*
    * The queue ensures that handle is never called concurrently by more than one thread.
    */
   public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
   {
      if (trace) { log.trace(this + " receives " + ref + " for delivery"); }
      
      // This is ok to have outside lock - is volatile
      if (!clientAccepting)
      {
         if (trace) { log.trace(this + "'s client is NOT accepting messages!"); }
         
         return null;
      }
      
      if (ref.isExpired())
      {
         SimpleDelivery delivery = new SimpleDelivery(observer, ref, true);
         
         try
         {
            sessionEndpoint.expireDelivery(delivery, expiryQueue);
         }
         catch (Throwable t)
         {
            log.error("Failed to expire delivery: " + delivery, t);
         }
         
         return delivery;
      }
        
      synchronized (startStopLock)
      {         
         // If the consumer is stopped then we don't accept the message, it should go back into the
         // queue for delivery later.
         if (!started)
         {
            if (trace) { log.trace(this + " NOT started yet!"); }
   
            return null;
         }
   
         if (trace) { log.trace(this + " has startStopLock lock, preparing the message for delivery"); }
   
         JBossMessage message = (JBossMessage)ref.getMessage();
         
         boolean selectorRejected = !this.accept(message);
   
         SimpleDelivery delivery = new SimpleDelivery(observer, ref, false, !selectorRejected);
         
         if (selectorRejected)
         {
            return delivery;
         }
                 
         long deliveryId =
            sessionEndpoint.addDelivery(delivery, id, dlq, expiryQueue, redeliveryDelay);
   
         // We don't send the message as-is, instead we create a MessageProxy instance. This allows
         // local fields such as deliveryCount to be handled by the proxy but global data to be
         // fielded by the same underlying Message instance. This allows us to avoid expensive
         // copying of messages
   
         MessageProxy mp = JBossMessage.
            createThinDelegate(deliveryId, message, ref.getDeliveryCount());
    
         // We send the message to the client on the current thread. The message is written onto the
         // transport and then the thread returns immediately without waiting for a response.
         
         // FIXME - how can we ensure that a later send doesn't overtake an earlier send - this
         // might happen if they are using different underlying TCP connections (e.g. from pool)
         
         ClientDelivery del = new ClientDelivery(mp, id);
         
         MessagingMarshallable mm = new MessagingMarshallable(versionToUse, del);
         
         Callback callback = new Callback(mm);
           
         try
         {
            //FIXME - due a design (flaw??) in the socket based transports, they use a pool of TCP
            // connections, so subsequent invocations can end up using different underlying
            // connections meaning that later invocations can overtake earlier invocations, if there
            // are more than one user concurrently invoking on the same transport. We need someway
            // of pinning the client object to the underlying invocation. For now we just serialize
            // all access so that only the first connection in the pool is ever used - bit this is
            // far from ideal!!!
            // See http://jira.jboss.com/jira/browse/JBMESSAGING-789

            Client clientInvoker = callbackHandler.getCallbackClient();
            Object invoker = null;

            if (clientInvoker != null)
            {
               invoker = clientInvoker.getInvoker();
            }
            else
            {
               // TODO: dummy synchronization object, in case there's no clientInvoker. This will
               // happen during the first invocation anyway. It's a kludge, I know, but this whole
               // synchronization thing is a huge kludge. Needs to be reviewed.
               invoker = new Object();
            }

            synchronized (invoker)
            {
               // one way invocation, no acknowledgment sent back by the client
               if (trace) { log.trace(this + " submitting message " + message + " to the remoting layer to be sent asynchronously"); }
               callbackHandler.handleCallbackOneway(callback);
            }
         }
         catch (HandleCallbackException e)
         {
            // it's an oneway callback, so exception could only have happened on the server, while
            // trying to send the callback. This is a good reason to smack the whole connection.
            // I trust remoting to have already done its own cleanup via a CallbackErrorHandler,
            // I need to do my own cleanup at ConnectionManager level.

            log.debug(this + " failed to handle callback", e);

            ServerConnectionEndpoint sce = sessionEndpoint.getConnectionEndpoint();
            ConnectionManager cm = sce.getServerPeer().getConnectionManager();

            cm.handleClientFailure(sce.getRemotingClientSessionID(), false);

            // we're practically cut, from connection down

            return null;
         }
              
         return delivery;      
      }
   }      
   
   

   // Filter implementation ------------------------------------------------------------------------

   public boolean accept(Routable r)
   {
      boolean accept = true;
      
      if (destination.isQueue())
      {
         // For subscriptions message selection is handled in the Subscription itself
         // we do not want to do the check twice
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
            
            if (trace) { log.trace("message connection id: " + conId + " current connection connection id: " + sessionEndpoint.getConnectionEndpoint().getConnectionID()); }   
                 
            accept = conId != sessionEndpoint.getConnectionEndpoint().getConnectionID();
                
            if (trace) { log.trace("accepting? " + accept); }            
         }
      }
      return accept;
   }


   // Closeable implementation ---------------------------------------------------------------------

   public void closing() throws JMSException
   {
      try
      {
         if (trace) { log.trace(this + " closing"); }
         
         stop(); 
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " closing");
      }     
   }
   
   public void close() throws JMSException
   {      
      try
      {
         if (trace) { log.trace(this + " close"); }
         
         localClose();
         
         sessionEndpoint.removeConsumer(id);         
      }   
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      }
   }
           
   // ConsumerEndpoint implementation --------------------------------------------------------------
   
   
   public void changeRate(float newRate) throws JMSException
   {
      if (trace) { log.trace(this + " changing rate to " + newRate); }
      
      try
      {      
         // For now we just support a binary on/off.
         // The client will send newRate = 0, to say it does not want any more messages when its
         // client side buffer gets full or it will send an arbitrary non zero number to say it
         // does want more messages, when its client side buffer empties to half its full size.
         // Note the client does not wait until the client side buffer is empty before sending a
         // newRate(+ve) message since this would add extra latency.
         
         // In the future we can fine tune this by allowing the client to specify an actual rate in
         // the newRate value so this is basically a placeholder for the future so we don't have to
         // change the wire format when we support it.
         
         // No need to synchronize - clientAccepting is volatile
         
         if (newRate == 0)
         {
            clientAccepting = false;
         }
         else
         {
            clientAccepting = true;
            promptDelivery();
         }            
      }   
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " changeRate");
      }
   }
   
   /*
    * This method is always called between closing() and close() being called
    * Instead of having a new method we could perhaps somehow pass the last delivery id
    * in with closing - then we don't need another message
    */
   public void cancelInflightMessages(long lastDeliveryId) throws JMSException
   {
      if (trace) { log.trace(this + " cancelInflightMessages: " + lastDeliveryId); }
      
      try
      {      
         //Cancel all deliveries made by this consumer with delivery id > lastDeliveryId
         
         sessionEndpoint.cancelDeliveriesForConsumerAfterDeliveryId(id, lastDeliveryId);      
      }   
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " cancelInflightMessages");
      }            
   }
   
   
   public boolean isClosed() throws JMSException
   {
      throw new IllegalStateException("isClosed should never be handled on the server side");
   }

   // Public ---------------------------------------------------------------------------------------
   
   public String toString()
   {
      return "ConsumerEndpoint[" + id + "]";
   }
   
   public JBossDestination getDestination()
   {
      return destination;
   }
   
   public ServerSessionEndpoint getSessionEndpoint()
   {
      return sessionEndpoint;
   }
   
   // Package protected ----------------------------------------------------------------------------
   
   Queue getDLQ()
   {
      return dlq;
   }
   
   Queue getExpiryQueue()
   {
      return expiryQueue;
   }
   
   long getRedliveryDelay()
   {
      return redeliveryDelay;
   }
     
   void localClose() throws Throwable
   {      
      if (trace) { log.trace(this + " grabbed the main lock in close() " + this); }

      messageQueue.remove(this); 
      
      JMSDispatcher.instance.unregisterTarget(new Integer(id));
      
      // If this is a consumer of a non durable subscription then we want to unbind the
      // subscription and delete all its data.

      if (destination.isTopic())
      {
         PostOffice postOffice = 
            sessionEndpoint.getConnectionEndpoint().getServerPeer().getPostOfficeInstance();
         
         Binding binding = postOffice.getBindingForQueueName(queueName);

         //Note binding can be null since there can many competing subscribers for the subscription  - 
         //in which case the first will have removed the subscription and subsequently
         //ones won't find it
         
         if (binding != null && !binding.getQueue().isRecoverable())
         {
            postOffice.unbindQueue(queueName);
            
            String counterName = TopicService.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + queueName;
            
            MessageCounter counter = 
               sessionEndpoint.getConnectionEndpoint().getServerPeer().getMessageCounterManager().unregisterMessageCounter(counterName);
            
            if (counter == null)
            {
               throw new IllegalStateException("Cannot find counter to remove " + counterName);
            }
         }
      }
     
   }        
           
   void start()
   {             
      synchronized (startStopLock)
      {      
         if (started)
         {
            return;
         }
         
         started = true;
      }
            
      // Prompt delivery
      promptDelivery();
   }
   
   void stop() throws Throwable
   {           
      synchronized (startStopLock)
      {         
         if (!started)
         {
            return;
         }
         
         started = false;                  

         // Any message deliveries already transit to the consumer, will just be ignored by the
         // MessageCallbackHandler since it will be closed.
         //
         // To clarify, the close protocol (from connection) is as follows:
         //
         // 1) MessageCallbackHandler::close() - any messages in buffer are cancelled to the server
         //    session, and any subsequent receive messages will be ignored.
         //
         // 2) ServerConsumerEndpoint::closing() causes stop() this flushes any deliveries yet to
         //    deliver to the client callback handler.
         //
         // 3) MessageCallbackHandler::cancelInflightMessages(long lastDeliveryId) - any deliveries
         //    after lastDeliveryId for the consumer will be considered in flight and cancelled.
         //
         // 4) ServerConsumerEndpoint:close() - endpoint is deregistered.
         //
         // 5) Session.close() - acks or cancels any remaining deliveries in the SessionState as
         //    appropriate.
         //
         // 6) ServerSessionEndpoint::close() - cancels any remaining deliveries and deregisters
         //    session.
         //
         // 7) Client side session executor is shutdown.
         //
         // 8) ServerConnectionEndpoint::close() - connection is deregistered.
         //
         // 9) Remoting connection listener is removed and remoting connection stopped.

      }
   }
         
   // Protected ------------------------------------------------------------------------------------
      
   // Private --------------------------------------------------------------------------------------
   
   private void promptDelivery()
   {
      messageQueue.deliver(Channel.ASYNCRHONOUS);
   }
   
   // Inner classes --------------------------------------------------------------------------------
     
}
