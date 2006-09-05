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

import org.jboss.jms.client.remoting.HandleMessageResponse;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.server.remoting.MessagingMarshallable;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.local.MessageQueue;
import org.jboss.messaging.core.plugin.contract.Exchange;
import org.jboss.messaging.core.plugin.exchange.Binding;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionException;
import org.jboss.messaging.core.tx.TxCallback;
import org.jboss.messaging.util.Future;

import EDU.oswego.cs.dl.util.concurrent.Executor;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

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
public class ServerConsumerEndpoint implements Receiver, ConsumerEndpoint
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 535443606137461274L;

   private static final Logger log = Logger.getLogger(ServerConsumerEndpoint.class);

   // Static --------------------------------------------------------

   private static final int MAX_DELIVERY_ATTEMPTS = 10;

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private int id;

   private MessageQueue messageQueue;
   
   private String queueName;

   private ServerSessionEndpoint sessionEndpoint;

   private boolean noLocal;

   private Selector messageSelector;

   private JBossDestination destination;

   private List toDeliver;

   // Must be volatile
   private volatile boolean clientConsumerFull;

   // Must be volatile
   private volatile boolean bufferFull;

   // Must be volatile
   private volatile boolean started;

   // No need to be volatile - is protected by lock
   private boolean closed;

   // No need to be volatile
   private boolean disconnected;

   private Executor executor;

   private int prefetchSize;

   private Object lock;

   private Map deliveries;
   
   // Constructors --------------------------------------------------

   protected ServerConsumerEndpoint(int id, MessageQueue messageQueue, String queueName,
                                    ServerSessionEndpoint sessionEndpoint,
                                    String selector, boolean noLocal, JBossDestination dest,
                                    int prefetchSize)
                                    throws InvalidSelectorException
   {
      if (trace) { log.trace("constructing consumer endpoint " + id); }

      this.id = id;
      this.messageQueue = messageQueue;
      this.queueName = queueName;
      this.sessionEndpoint = sessionEndpoint;
      this.prefetchSize = prefetchSize;

      // We always created with clientConsumerFull = true. This prevents the SCD sending messages to
      // the client before the client has fully finished creating the MessageCallbackHandler.
      this.clientConsumerFull = true;

      // We allocate an executor from the rotating pool for each consumer based on it's id
      // This gives better latency than each consumer for the destination using the same
      // executor
      QueuedExecutorPool pool =
         sessionEndpoint.getConnectionEndpoint().getServerPeer().getQueuedExecutorPool();

      this.executor = (QueuedExecutor)pool.get();
             
      // Note that using a PooledExecutor with a linked queue is not sufficient to ensure that
      // deliveries for the same consumer happen serially, since even if they are queued serially
      // the actual deliveries can happen in parallel, resulting in a later one "overtaking" an
      // earlier non-deterministicly depending on thread scheduling.
      // Consequently we use a QueuedExecutor to ensure the deliveries happen sequentially. We do
      // not want each ServerConsumerEndpoint instance to have its own instance - since we would
      // end up using too many threads, neither do we want to share the same instance amongst all
      // consumers - we do not want to serialize delivery to all consumers. So we maintain a bag of
      // QueuedExecutors and give them out to consumers as required. Different consumers can end up
      // using the same queuedexecutor concurrently if there are a lot of active consumers.

      this.noLocal = noLocal;
      this.destination = dest;
      
      this.toDeliver = new ArrayList();
      
      this.lock = new Object();

      if (selector != null)
      {
         if (trace) log.trace("creating selector:" + selector);
         this.messageSelector = new Selector(selector);
         if (trace) log.trace("created selector");
      }

      //FIXME - 
      //We really need to get rid of this delivery list - it's only purpose in life is to solve
      //the race condition where acks or cancels can come in before handle has returned - and
      //that can be solved in a simpler way anyway.
      //It adds extra complexity both in all the extra code necessary to maintain it, the extra memory
      //needed to maintain it, the extra complexity in synchronization on this class to protect access to it
      //and when we do clustering we will have to replicate it too!!
      //Let's GET RID OF IT!!!!!!!!!!!
      this.deliveries = new LinkedHashMap();
            
      this.started = this.sessionEndpoint.getConnectionEndpoint().isStarted();

      // adding the consumer to the queue
      this.messageQueue.add(this);
      
      // prompt delivery
      messageQueue.deliver(false);
      
      log.debug(this + " constructed");
   }

   // Receiver implementation ---------------------------------------

   /*
    * The queue ensures that handle is never called concurrently by more than one thread.
    */
   public Delivery handle(DeliveryObserver observer, Routable reference, Transaction tx)
   {
      if (trace) { log.trace(this + " receives " + reference + " for delivery"); }
      
      // This is ok to have outside lock - is volatile
      if (bufferFull)
      {
         // We buffer a maximum of PREFETCH_LIMIT messages at once
         
         if (trace) { log.trace(this + " has reached prefetch size will not accept any more references"); }
         
         return null;
      }
       
      // Need to synchronized around the whole block to prevent setting started = false
      // but handle is already running and a message is deposited during the stop procedure.
      synchronized (lock)
      {  
         // If the consumer is stopped then we don't accept the message, it should go back into the
         // queue for delivery later.
         if (!started)
         {
            // this is a common programming error, make this visible in the debug logs
            // TODO: analyse performance implications
            log.debug(this + " NOT started yet!");
            return null;
         }
            
         MessageReference ref = (MessageReference)reference;
                     
         JBossMessage message = (JBossMessage)ref.getMessage();
         
         boolean selectorRejected = !this.accept(message);
   
         SimpleDelivery delivery = new SimpleDelivery(observer, ref, false, !selectorRejected);
         
         if (selectorRejected)
         {
            return delivery;
         }
            
         checkDeliveryCount(delivery);
         
         if (delivery.isDone())
         {
            return delivery;
         }
   
         deliveries.put(new Long(ref.getMessageID()), delivery);                 
   
         // We don't send the message as-is, instead we create a MessageProxy instance. This allows
         // local fields such as deliveryCount to be handled by the proxy but global data to be
         // fielded by the same underlying Message instance. This allows us to avoid expensive
         // copying of messages
   
         MessageProxy mp = JBossMessage.createThinDelegate(message, ref.getDeliveryCount());
    
         // Add the proxy to the list to deliver
                           
         toDeliver.add(mp);     
          
         bufferFull = toDeliver.size() >= prefetchSize;
             
         if (!clientConsumerFull)
         {            
            try
            {
               this.executor.execute(new Deliverer());
            }
            catch (InterruptedException e)
            {
               log.warn("Thread interrupted", e);
            }
         }
                             
         return delivery;              
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
         synchronized (lock)
         { 
            // On close we only disconnect the consumer from the queue we don't actually remove
            // it. This is because it may still contain deliveries that may well be acknowledged
            // after the consumer has closed. This is perfectly valid.

            //TODO - The deliveries should really be stored in the session endpoint, not here
            //that is their natural place, that would mean we wouldn't have to mess around with keeping
            //deliveries after this is closed
                  
            disconnect(); 
            
            JMSDispatcher.instance.unregisterTarget(new Integer(id));
            
            //If this is a consumer of a non durable subscription
            //then we want to unbind the subscription and delete all it's data
            
            if (destination.isTopic())
            {
               Exchange topicExchange = 
                  sessionEndpoint.getConnectionEndpoint().getServerPeer().getTopicExchangeDelegate();
               
               Binding binding = topicExchange.getBindingForName(queueName);
               
               if (binding == null)
               {
                  throw new IllegalStateException("Cannot find queue with name " + queueName);
               }
               
               if (!binding.isDurable())
               {                 
                  topicExchange.unbindQueue(queueName);
               }
            }
                        
            closed = true;
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      }
   }
               
   // ConsumerEndpoint implementation -------------------------------
   
   /*
    * This is called by the client consumer to tell the server to wake up and start sending more
    * messages if available
    */
   public void more() throws JMSException
   {           
      try
      {
         /*
         Set clientConsumerFull to false
         NOTE! This must be done using a Runnable on the delivery executor - this is to
         prevent the following race condition:
         1) Messages are delivered to the client, causing it to be full
         2) The messages are consumed very quickly on the client causing more to be called()
         3) more() hits the server BEFORE the deliverer thread has returned from delivering to the client
         causing clientConsumerFull to be set to false and adding a deliverer to the queue.
         4) The deliverer thread returns and sets clientConsumerFull to true
         5) The next deliverer runs but doesn't do anything since clientConsumerFull = true even
         though the client needs messages
         */
         this.executor.execute(new Runnable() { public void run() { clientConsumerFull = false; } });         
                           
         //Run a deliverer to deliver any existing ones
         this.executor.execute(new Deliverer());
         
         //TODO Why do we need to wait for it to execute??
         //Why not just return immediately?
         
         //Now wait for it to execute
         Future result = new Future();
         
         this.executor.execute(new Waiter(result));
         
         result.getResult();
                  
         //Now we know the deliverer has delivered any outstanding messages to the client buffer
         
         messageQueue.deliver(false);
      }
      catch (InterruptedException e)
      {
         log.warn("Thread interrupted", e);
      }       
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " more");
      }
   }
   
   
   // Public --------------------------------------------------------
   
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
   
   public int getId()
   {
      return id;
   }
    
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------   
   
   protected void acknowledgeTransactionally(long messageID, Transaction tx) throws Throwable
   {
      if (trace) { log.trace("acknowledging transactionally " + messageID); }
      
      Delivery d = null;
                 
      // The actual removal of the deliveries from the delivery list is deferred until tx commit
      synchronized (lock)
      {
         d = (Delivery)deliveries.get(new Long(messageID));
      }
      
      DeliveryCallback deliveryCallback = (DeliveryCallback)tx.getCallback(this);
            
      if (deliveryCallback == null)
      {
         deliveryCallback = new DeliveryCallback();
         tx.addCallback(deliveryCallback, this);
      }
      deliveryCallback.addMessageID(messageID);
         
      if (d != null)
      {
         d.acknowledge(tx);
      }
      else
      {
         throw new IllegalStateException("Failed to acknowledge delivery " + d);
      }             
   }      
   
   protected void acknowledge(long messageID) throws Throwable
   {  
      // acknowledge a delivery   
      Delivery d;
        
      synchronized (lock)
      {
         d = (Delivery)deliveries.remove(new Long(messageID));
      }
      
      if (d != null)
      {
         d.acknowledge(null);
      }
      else
      {     
         throw new IllegalStateException("Cannot find delivery to acknowledge:" + messageID);
      }      
   }
   
   /**
    * Actually remove the consumer and clear up any deliveries it may have
    * This is called by the session on session.close()
    * We can get rid of this when we store the deliveries on the session
    *
    **/
   protected void remove() throws Throwable
   {         
      if (trace) log.trace("attempting to remove receiver " + this + " from destination " + messageQueue);
      
      boolean wereDeliveries = false;
      for(Iterator i = deliveries.values().iterator(); i.hasNext(); )
      {

         Delivery d = (Delivery)i.next();

         d.cancel();
         wereDeliveries = true;
      }
      deliveries.clear();           
      
      if (!disconnected)
      {
         if (!closed)
         {
            close();
         }
      }
      
      sessionEndpoint.getConnectionEndpoint().
         getServerPeer().removeConsumerEndpoint(new Integer(id));                  
            
      sessionEndpoint.removeConsumerEndpoint(id);
      
      if (wereDeliveries)
      {
         //If we cancelled any deliveries we need to force a deliver on the queue
         //This is because there may be other waiting competing consumers who need a chance to get
         //any of the cancelled messages
         messageQueue.deliver(false);
      }
   }  
   
   protected void promptDelivery()
   {
      messageQueue.deliver(false);
   }
   
   protected void cancelDelivery(Long messageID) throws Throwable
   {
      Delivery del = (Delivery)deliveries.remove(messageID);
      if (del != null)
      {  
          del.getReference().decrementDeliveryCount();    
          del.cancel();
      }
      else
      {
          throw new IllegalStateException("Cannot find delivery to cancel:" + id);
      }
   }
               
   protected void start()
   {             
      synchronized (lock)
      {
         // Can't start or stop it if it is closed.
         if (closed)
         {
            return;
         }
         
         if (started)
         {
            return;
         }
         
         started = true;
      }
      
      // Prompt delivery
      messageQueue.deliver(false);
   }
   
   protected void stop() throws Throwable
   {     
      //We need to:
      //Stop accepting any new messages in the SCE
      //Flush any messages from the SCE to the buffer
      //If the client consumer is now full, then we need to cancel the ones in the toDeliver list

      //We need to lock since otherwise we could set started to false but the handle method was already executing
      //and messages might get deposited after
      synchronized (lock)
      {
         //can't start or stop it if it is closed
         if (closed)
         {
            return;
         }
         
         started = false;
      }
      
      //Now we know no more messages will be accepted in the SCE
            
      try
      {
         //Flush any messages waiting to be sent to the client
         this.executor.execute(new Deliverer());
         
         //Now wait for it to execute
         Future result = new Future();
         
         this.executor.execute(new Waiter(result));
         
         result.getResult();
             
         //Now we know any deliverer has delivered any outstanding messages to the client buffer
      }
      catch (InterruptedException e)
      {
         log.warn("Thread interrupted", e);
      }
            
      //Now we know that there are no in flight messages on the way to the client consumer.
      
      //But there may be messages still in the toDeliver list since the client consumer might be full
      //So we need to cancel these
            
      if (!toDeliver.isEmpty())
      { 
         synchronized (lock)
         {
            for (int i = toDeliver.size() - 1; i >= 0; i--)
            {
               MessageProxy proxy = (MessageProxy)toDeliver.get(i);
               
               long id = proxy.getMessage().getMessageID();
               
               cancelDelivery(new Long(id));
            }
         }
                 
         toDeliver.clear();
         
         bufferFull = false;
      }      
   }
      
   // Private -------------------------------------------------------
   
   /**
    * Disconnect this consumer from the queue that feeds it. This method does not clear up
    * deliveries
    */
   private void disconnect()
   {

      boolean removed = messageQueue.remove(this);
      
      if (removed)
      {
         disconnected = true;
         if (trace) { log.trace(this + " removed from the queue"); }
      }
   }
     
   private void checkDeliveryCount(SimpleDelivery del)
   {
      //TODO - We need to put the message in a DLQ
      // For now we just ack it otherwise the message will keep being retried and we'll never get
      // anywhere
      if (del.getReference().getDeliveryCount() > MAX_DELIVERY_ATTEMPTS)
      {
         log.warn(del.getReference() + " has exceed maximum delivery attempts and will be removed");
         
         try
         {
            del.acknowledge(null);
         }
         catch (Throwable t)
         {
            log.error("Failed to acknowledge delivery", t);
         }
      }                 
   }
   
   // Inner classes -------------------------------------------------   
   
   /*
    * Delivers messages to the client 
    * TODO - We can make this a bit more intelligent by letting it measure the rate
    * the client is consuming messages and send messages at that rate.
    * This would mean the client consumer wouldn't be full so often and more wouldn't have to be called
    * This should give higher throughput.
    */
   private class Deliverer implements Runnable
   {
      public void run()
      {
         // Is there anything to deliver?
         // This is ok outside lock - is volatile
         if (clientConsumerFull)
         {
            // Do nothing
            return;
         }
         
         List list = null;
             
         synchronized (lock)
         {           
            if (!toDeliver.isEmpty())
            {
               list = new ArrayList(toDeliver);
               
               toDeliver.clear();
               
               bufferFull = false;
            }
         }
                                                           
         if (list != null)
         {         
            ServerConnectionEndpoint connection =
               ServerConsumerEndpoint.this.sessionEndpoint.getConnectionEndpoint();

            try
            {
               if (trace) { log.trace(ServerConsumerEndpoint.this + "handing " + list.size() + " message(s) over to the remoting layer"); }
            
               ClientDelivery del = new ClientDelivery(list, id);
               
               // TODO How can we ensure that messages for the same consumer aren't delivered
               // concurrently to the same consumer on different threads?
               MessagingMarshallable mm = new MessagingMarshallable(connection.getUsingVersion(), del);
               
               MessagingMarshallable resp = (MessagingMarshallable)connection.getCallbackClient().invoke(mm);

               if (trace) { log.trace(ServerConsumerEndpoint.this + "handed messages over to the remoting layer"); }
                
               HandleMessageResponse result = (HandleMessageResponse)resp.getLoad();

               // For now we don't look at how many messages are accepted since they all will be.
               // The field is a placeholder for the future.
               if (result.clientIsFull())
               {
                  // Stop the server sending any more messages to the client.
                  // This is ok outside lock.
                  clientConsumerFull = true;       
               }                               
            }
            catch(Throwable t)
            {
               log.warn("Failed to deliver the message to the client. See the server log for more details.");
               log.debug(ServerConsumerEndpoint.this + " failed to deliver the message to the client.", t);
               
               ConnectionManager mgr = connection.getServerPeer().getConnectionManager();
               
               mgr.handleClientFailure(connection.getRemotingClientSessionId());
            }
         }              
      }
   }
   
   /*
    * The purpose of this class is to put it on the QueuedExecutor and wait for it to run
    * We can then ensure that all the Runnables in front of it on the queue have also executed
    * We cannot just call shutdownAfterProcessingCurrentlyQueuedTasks() since the
    * QueueExecutor might be share by other consumers and we don't want to wait for their
    * tasks to complete
    */
   private class Waiter implements Runnable
   {
      Future result;
      
      Waiter(Future result)
      {
         this.result = result;
      }
      
      public void run()
      {
         result.setResult(null);
      }
   }
   
   /**
    * 
    * The purpose of this class is to remove deliveries from the delivery list on commit
    * Each transaction has once instance of this per SCE
    *
    */
   private class DeliveryCallback implements TxCallback
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
      
      public synchronized void afterCommit(boolean onePhase) throws TransactionException
      {
         // Remove the deliveries from the delivery map.
         Iterator iter = delList.iterator();
         while (iter.hasNext())
         {
            Long messageID = (Long)iter.next();
            
            if (deliveries.remove(messageID) == null)
            {
               throw new TransactionException("Failed to remove delivery " + messageID);
            }
         }
      }
      
      public void afterRollback(boolean onePhase) throws TransactionException
      {                            
         //NOOP
      }
      
      synchronized void addMessageID(long messageID)
      {
         delList.add(new Long(messageID));
      }
   }
   
}
