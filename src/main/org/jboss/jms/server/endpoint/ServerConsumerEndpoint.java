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

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CHANGERATE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CLOSING;

import javax.jms.JMSException;

import org.jboss.jms.delegate.ConsumerEndpoint;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.messaging.core.Condition;
import org.jboss.messaging.core.Consumer;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.HandleStatus;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.impl.ConditionImpl;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.ChangeRateMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.Logger;

/**
 * Concrete implementation of a Consumer. 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Partially derived from JBM 1.x version by:
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt> $Id$
 */
public class ServerConsumerEndpoint implements Consumer, ConsumerEndpoint
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerEndpoint.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private String id;

   private Queue messageQueue;

   private String queueName;

   private ServerSessionEndpoint sessionEndpoint;

   private boolean noLocal;

   private Filter filter;

   private Destination destination;

   private Queue dlq;

   private Queue expiryQueue;

   private long redeliveryDelay;
   
   private int maxDeliveryAttempts;

   private boolean started;

   // This lock protects starting and stopping
   private Object startStopLock;

   // Must be volatile
   private volatile boolean clientAccepting;

   private boolean retainDeliveries;
   
   private long lastDeliveryID = -1;
   
   private volatile boolean dead;

   private int prefetchSize;
   
   private volatile int sendCount;
   
   private boolean firstTime = true;
   
   // Constructors ---------------------------------------------------------------------------------

   ServerConsumerEndpoint(MessagingServer sp, String id, Queue messageQueue, String queueName,
					           ServerSessionEndpoint sessionEndpoint, Filter filter,
					           boolean noLocal, Destination destination, Queue dlq,
					           Queue expiryQueue, long redeliveryDelay, int maxDeliveryAttempts,
					           int prefetchSize)
   {
      if (trace)
      {
         log.trace("constructing consumer endpoint " + id);
      }

      this.id = id;

      this.messageQueue = messageQueue;

      this.queueName = queueName;

      this.sessionEndpoint = sessionEndpoint;

      this.noLocal = noLocal;

      this.destination = destination;

      this.dlq = dlq;

      this.redeliveryDelay = redeliveryDelay;

      this.expiryQueue = expiryQueue;
      
      this.maxDeliveryAttempts = maxDeliveryAttempts;

      // Always start as false - wait for consumer to initiate.
      this.clientAccepting = false;
      
      this.startStopLock = new Object();

      this.prefetchSize = prefetchSize;
      
      this.filter = filter;
                
      //FIXME - we shouldn't have checks like this on the server side
      //It should be the jms client that decides whether to retain deliveries or not
      if (destination.getType() == DestinationType.TOPIC && !messageQueue.isDurable())
      {
         // This is a consumer of a non durable topic subscription. We don't need to store
         // deliveries since if the consumer is closed or dies the refs go too.
         this.retainDeliveries = false;
      }
      else
      {
         this.retainDeliveries = true;
      }
      
      this.started = this.sessionEndpoint.getConnectionEndpoint().isStarted();
      
      // adding the consumer to the queue
      messageQueue.addConsumer(this);
      
      messageQueue.deliver();

      log.trace(this + " constructed");
   }

   // Receiver implementation ----------------------------------------------------------------------

   public HandleStatus handle(MessageReference ref) throws Exception
   {
      if (trace)
      {
         log.trace(this + " receives " + ref + " for delivery");
      }
      
      // This is ok to have outside lock - is volatile
      if (!clientAccepting)
      {
         if (trace) { log.trace(this + " is NOT accepting messages!"); }

         return HandleStatus.BUSY;
      }

      if (ref.getMessage().isExpired())
      {         
         sessionEndpoint.expireDelivery(ref, expiryQueue);
         
         return HandleStatus.HANDLED;
      }
      
// TODO re-implement preserve ordering      
//      if (preserveOrdering && remote)
//      {
//      	//If the header exists it means the message has already been sucked once - so reject.
//      	
//      	if (ref.getMessage().getHeader(Message.CLUSTER_SUCKED) != null)
//      	{
//      		if (trace) { log.trace("Message has already been sucked once - not sucking again"); }
//      		
//      		return null;
//      	}      	    
//      }

      synchronized (startStopLock)
      {
         // If the consumer is stopped then we don't accept the message, it should go back into the
         // queue for delivery later.
         if (!started)
         {
            if (trace) { log.trace(this + " NOT started"); }

            return HandleStatus.BUSY;
         }
         
         if (trace) { log.trace(this + " has startStopLock lock, preparing the message for delivery"); }

         Message message = ref.getMessage();
         
         if (!accept(message))
         {
            return HandleStatus.NO_MATCH;
         }
         
         if (noLocal)
         {
            String conId = message.getConnectionID();

            if (trace) { log.trace("message connection id: " + conId + " current connection connection id: " + sessionEndpoint.getConnectionEndpoint().getConnectionID()); }

            if (sessionEndpoint.getConnectionEndpoint().getConnectionID().equals(conId))
            {
            	if (trace) { log.trace("Message from local connection so rejecting"); }
            	
            	PersistenceManager pm = sessionEndpoint.getConnectionEndpoint().getMessagingServer().getPersistenceManager();
            	            	            	
            	ref.acknowledge(pm);
            	
             	return HandleStatus.HANDLED;
            }            
         }
                  
         sendCount++;
         
         int num = prefetchSize;
         
         if (firstTime)
         {
            //We make sure we have a little extra buffer on the client side
            num = num + num / 3 ;
         }
         
         if (sendCount == num)
         {
            clientAccepting = false;
            
            firstTime = false;
         }          
                   
         try
         {
         	sessionEndpoint.handleDelivery(ref, this);
         }
         catch (Exception e)
         {
         	log.error("Failed to handle delivery", e);
         	
         	this.started = false; // DO NOT return null or the message might get delivered more than once
         }
                          
         return HandleStatus.HANDLED;
      }
   }
   
   // Filter implementation ------------------------------------------------------------------------

   public boolean accept(Message msg)
   {
      boolean accept = true;

      //FIXME - we shouldn't have checks like this - it should be the client side which decides whether
      //to have a filter on the consumer
      if (destination.getType() == DestinationType.QUEUE)
      {
         // For subscriptions message selection is handled in the Subscription itself we do not want
         // to do the check twice
         if (filter != null)
         {
            accept = filter.match(msg);

            if (trace) { log.trace("message filter " + (accept ? "accepts " : "DOES NOT accept ") + "the message"); }
         }
      }
      
      return accept;
   }

   // Closeable implementation ---------------------------------------------------------------------

   public long closing(long sequence) throws JMSException
   {
      try
      {
         if (trace) { log.trace(this + " closing");}

         stop();
         
         return lastDeliveryID;
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
         if (trace)
         {
            log.trace(this + " close");
         }

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
      if (trace)
      {
         log.trace(this + " changing rate to " + newRate);
      }

      try
      {
         if (newRate > 0)
         {
            sendCount = 0;
            
            clientAccepting = true;
         }
         else
         {
            clientAccepting = false;
         }

         if (clientAccepting)
         {
            promptDelivery();
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " changeRate");
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ConsumerEndpoint[" + id + "]";
   }

//   public Destination getDestination()
//   {
//      return destination;
//   }
//
//   public ServerSessionEndpoint getSessionEndpoint()
//   {
//      return sessionEndpoint;
//   }

   public PacketHandler newHandler()
   {
      return new ServerConsumerEndpointPacketHandler();
   }

   // Package protected ----------------------------------------------------------------------------
   
   String getID()
   {
   	return this.id;
   }

   boolean isRetainDeliveries()
   {
   	return this.retainDeliveries;
   }
   
   void setLastDeliveryID(long id)
   {
   	this.lastDeliveryID = id;
   }
   
   void setStarted(boolean started)
   {
      //No need to lock since caller already has the lock
      this.started = started;      
   }
   
   void setDead()
   {
      dead = true;
   }
   
   boolean isDead()
   {
      return dead;
   }
   
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
   
   int getMaxDeliveryAttempts()
   {
   	return maxDeliveryAttempts;
   }
   
   String getQueueName()
   {
   	return queueName;
   }

   void localClose() throws Exception
   {
      if (trace) { log.trace(this + " grabbed the main lock in close() " + this); }

      messageQueue.removeConsumer(this);
      
      sessionEndpoint.getConnectionEndpoint().getMessagingServer().getMinaService().getDispatcher().unregister(id);
            
      // If this is a consumer of a non durable subscription then we want to unbind the
      // subscription and delete all its data.

      //FIXME - We shouldn't have checks like this on the server side - it should the jms client
      //which decides whether to delete it or not
      if (destination.getType() == DestinationType.TOPIC)
      {
         PostOffice postOffice = sessionEndpoint.getConnectionEndpoint().getMessagingServer().getPostOffice();
                  
         MessagingServer sp = sessionEndpoint.getConnectionEndpoint().getMessagingServer();
         
         if (!messageQueue.isDurable())
         {
            Condition condition = new ConditionImpl(destination.getType(), destination.getName());
            
            postOffice.removeQueue(condition, messageQueue.getName(), false);

            //TODO message counters are handled elsewhere
            
//            if (!messageQueue.isTemporary())
//            {
//	            String counterName = ManagedDestination.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + queueName;
//	
//	            MessageCounter counter = sp.getMessageCounterManager().unregisterMessageCounter(counterName);
//	
//	            if (counter == null)
//	            {
//	               throw new IllegalStateException("Cannot find counter to remove " + counterName);
//	            }
//            }
         }
         else
         {
         	//Durable sub consumer
         	
            //TODO - how do we ensure this for JBM 2.0 ?
            
//         	if (queue.isClustered() && sp.getConfiguration().isClustered())
//            {
//            	//Clustered durable sub consumer created - we need to remove this info from the replicator
//            	
//            	Replicator rep = (Replicator)postOffice;
//            	
//            	rep.remove(queue.getName());
//            }
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

   void stop() throws Exception
   {
      synchronized (startStopLock)
      {
         if (!started)
         {
            return;
         }

         started = false;
         
         // Any message deliveries already transit to the consumer, will just be ignored by the
         // ClientConsumer since it will be closed.
         //
         // To clarify, the close protocol (from connection) is as follows:
         //
         // 1) ClientConsumer::close() - any messages in buffer are cancelled to the server
         // session, and any subsequent receive messages will be ignored.
         //
         // 2) ServerConsumerEndpoint::closing() causes stop() this flushes any deliveries yet to
         // deliver to the client callback handler.
         //
         // 3) ClientConsumer waits for all deliveries to arrive at client side
         //
         // 4) ServerConsumerEndpoint:close() - endpoint is deregistered.
         //
         // 5) Session.close() - acks or cancels any remaining deliveries in the SessionState as
         // appropriate.
         //
         // 6) ServerSessionEndpoint::close() - cancels any remaining deliveries and deregisters
         // session.
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
      sessionEndpoint.promptDelivery(messageQueue);
   }

   private PacketSender replier;

   private void setReplier(PacketSender replier)
   {
      this.replier = replier;
   }

   public void deliver(DeliverMessage message)
   {
      if (replier != null)
      {
         message.setTargetID(id);
         replier.send(message);
      } else
      {
         log.error("No replier to deliver message to consumer");
      }
   }

   // Inner classes --------------------------------------------------------------------------------
   
   private class ServerConsumerEndpointPacketHandler implements PacketHandler {

      public String getID()
      {
         return ServerConsumerEndpoint.this.id;
      }

      public void handle(AbstractPacket packet, PacketSender sender)
      {
         try
         {
            AbstractPacket response = null;

            PacketType type = packet.getType();
            if (type == MSG_CHANGERATE)
            {
               setReplier(sender);

               ChangeRateMessage message = (ChangeRateMessage) packet;
               changeRate(message.getRate());
            } else if (type == REQ_CLOSING)
            {
               ClosingRequest request = (ClosingRequest) packet;
               long id = closing(request.getSequence());
               
               response = new ClosingResponse(id);
            } else if (type == MSG_CLOSE)
            {
               close();
               setReplier(null);
               
               response = new NullPacket();
            } else
            {
               response = new JMSExceptionMessage(new MessagingJMSException(
                     "Unsupported packet for browser: " + packet));
            }

            // reply if necessary
            if (response != null)
            {
               response.normalize(packet);
               sender.send(response);
            }
         } catch (JMSException e)
         {
            JMSExceptionMessage message = new JMSExceptionMessage(e);
            message.normalize(packet);
            sender.send(message);
         }
      }

      @Override
      public String toString()
      {
         return "ServerConsumerEndpointPacketHandler[id=" + id + "]";
      }
   }
}
