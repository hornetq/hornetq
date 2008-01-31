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

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ADDTEMPORARYDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELETETEMPORARYDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UNSUBSCRIBE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEDESTINATION;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.server.container.SecurityAspect;
import org.jboss.jms.server.security.CheckType;
import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.Condition;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.ResourceManager;
import org.jboss.messaging.core.Transaction;
import org.jboss.messaging.core.impl.ConditionImpl;
import org.jboss.messaging.core.impl.DeliveryImpl;
import org.jboss.messaging.core.impl.TransactionImpl;
import org.jboss.messaging.core.impl.filter.FilterImpl;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.AddTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationResponse;
import org.jboss.messaging.core.remoting.wireformat.DeleteTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetInDoubtXidsResponse;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetTimeoutResponse;
import org.jboss.messaging.core.remoting.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResponse;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutResponse;
import org.jboss.messaging.core.remoting.wireformat.SessionXAStartMessage;
import org.jboss.messaging.core.remoting.wireformat.UnsubscribeMessage;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessageQueueNameHelper;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * Session implementation
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * Parts derived from JBM 1.x ServerSessionEndpoint by
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerSessionEndpoint
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerSessionEndpoint.class);

   static final String DUR_SUB_STATE_CONSUMERS = "C";

   static final String TEMP_QUEUE_MESSAGECOUNTER_PREFIX = "TempQueue.";

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private SecurityAspect security = new SecurityAspect();

   private boolean trace = log.isTraceEnabled();

   private String id;

   private volatile boolean closed;

   private ServerConnectionEndpoint connectionEndpoint;

   private MessagingServer sp;

   private Map consumers = new HashMap();
   private Map browsers = new HashMap();

   private PostOffice postOffice;
   private int defaultMaxDeliveryAttempts;
   private long defaultRedeliveryDelay;
   private Queue defaultDLQ;
   private Queue defaultExpiryQueue;

   private volatile LinkedList<Delivery> deliveries = new LinkedList<Delivery>();

   //private SynchronizedLong deliveryIdSequence;
   
   private long deliveryIDSequence = 0;

   //Temporary until we have our own NIO transport
   QueuedExecutor executor = new QueuedExecutor(new LinkedQueue());

   private Transaction tx;
   
   //private boolean transacted;
   
   private boolean xa;
   
   private PacketSender sender;
   
   //private boolean transactionalSends;
   
   private boolean autoCommitSends;
   
   private boolean autoCommitAcks;
   
   private ResourceManager resourceManager;

   // Constructors ---------------------------------------------------------------------------------

   ServerSessionEndpoint(String sessionID, ServerConnectionEndpoint connectionEndpoint,
                         boolean autoCommitSends, boolean autoCommitAcks, boolean xa,
                         PacketSender sender, ResourceManager resourceManager) throws Exception
   {
      this.id = sessionID;

      this.connectionEndpoint = connectionEndpoint;

      sp = connectionEndpoint.getMessagingServer();

      postOffice = sp.getPostOffice();

      defaultDLQ = sp.getDefaultDLQInstance();

      defaultExpiryQueue = sp.getDefaultExpiryQueueInstance();

      defaultMaxDeliveryAttempts = sp.getConfiguration().getDefaultMaxDeliveryAttempts();

      defaultRedeliveryDelay = sp.getConfiguration().getDefaultRedeliveryDelay();

      //this.transacted = transacted;
      
      this.xa = xa;
      
      if (!xa)
      {
         tx = new TransactionImpl();
      }
            
      this.sender = sender;
      
      //this.transactionalSends = transactionalSends;
      
      this.autoCommitSends = autoCommitSends;
      
      this.autoCommitAcks = autoCommitAcks;
      
      this.resourceManager = resourceManager;
   }
   
   
   // Public ---------------------------------------------------------------------------------------

   public ServerConnectionEndpoint getConnectionEndpoint()
   {
      return connectionEndpoint;
   }

   public String toString()
   {
      return "SessionEndpoint[" + id + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   void expireDelivery(MessageReference ref, Queue expiryQueue) throws Exception
   {
      if (trace) { log.trace(this + " detected expired message " + ref); }

//      if (expiryQueue != null)
//      {
//         if (trace) { log.trace(this + " sending expired message to expiry queue " + expiryQueue); }
//
//         Message copy = makeCopyForDLQOrExpiry(true, ref);
//
//         moveInTransaction(copy, ref, expiryQueue, true);
//      }
//      else
//      {
//         log.warn("No expiry queue has been configured so removing expired " + ref);
//
//         //TODO - tidy up these references - ugly
//         ref.acknowledge(this.getConnectionEndpoint().getMessagingServer().getPersistenceManager());
//      }
      
      //TODO
   }

   void removeBrowser(String browserId) throws Exception
   {
      synchronized (browsers)
      {
         if (browsers.remove(browserId) == null)
         {
            throw new IllegalStateException("Cannot find browser with id " + browserId + " to remove");
         }
      }
   }

   void removeConsumer(String consumerId) throws Exception
   {
      synchronized (consumers)
      {
         if (consumers.remove(consumerId) == null)
         {
            throw new IllegalStateException("Cannot find consumer with id " + consumerId + " to remove");
         }         
      }
   }

   void localClose() throws Exception
   {
      if (closed)
      {
         throw new IllegalStateException("Session is already closed");
      }

      if (trace) log.trace(this + " close()");

      //We clone to avoid deadlock http://jira.jboss.org/jira/browse/JBMESSAGING-836
      Map consumersClone;
      synchronized (consumers)
      {
         consumersClone = new HashMap(consumers);
      }

      for( Iterator i = consumersClone.values().iterator(); i.hasNext(); )
      {
         ((ServerConsumerEndpoint)i.next()).localClose();
      }

      consumers.clear();


      //We clone to avoid deadlock http://jira.jboss.org/jira/browse/JBMESSAGING-836
      Map browsersClone;
      synchronized (browsers)
      {
         browsersClone = new HashMap(browsers);
      }

      for( Iterator i = browsersClone.values().iterator(); i.hasNext(); )
      {
         ((ServerBrowserEndpoint)i.next()).localClose();
      }

      browsers.clear();

      rollback();
      
      //Close down the executor

      //Note we need to wait for ALL tasks to complete NOT just one otherwise we can end up with the following situation
      //prompter is queued and starts to execute
      //prompter almost finishes executing then a message is cancelled due to this session closing
      //this causes another prompter to be queued
      //shutdownAfterProcessingCurrentTask is then called
      //this means the second prompter never runs and the cancelled message doesn't get redelivered
      executor.shutdownAfterProcessingCurrentlyQueuedTasks();

      deliveries.clear();

      sp.removeSession(id);

      closed = true;
   }
      
   synchronized void handleDelivery(MessageReference ref, ServerConsumerEndpoint consumer,
                                    PacketSender sender) throws Exception
   { 
       //FIXME - we shouldn't have to pass in the packet Sender - this should be creatable
       //without the consumer having to call change rate first
       Delivery delivery = new DeliveryImpl(ref, consumer.getID(), deliveryIDSequence++, sender);
       
       deliveries.add(delivery);       
              
       delivery.deliver();
   }

   /**
    * Starts this session's Consumers
    */
   void setStarted(boolean s) throws Exception
   {
      //We clone to prevent deadlock http://jira.jboss.org/jira/browse/JBMESSAGING-836
      Map consumersClone;
      synchronized(consumers)
      {
         consumersClone = new HashMap(consumers);
      }

      for(Iterator i = consumersClone.values().iterator(); i.hasNext(); )
      {
         ServerConsumerEndpoint sce = (ServerConsumerEndpoint)i.next();
         if (s)
         {
            sce.start();
         }
         else
         {
            sce.stop();
         }
      }
   }

   void promptDelivery(final Queue queue)
   {
      if (trace) { log.trace("Prompting delivery on " + queue); }

      try
      {
         //TODO - do we really need to prompt on a different thread?
         this.executor.execute(new Runnable() { public void run() { queue.deliver();} } );

      }
      catch (Throwable t)
      {
         log.error("Failed to prompt delivery", t);
      }
   }

   private CreateConsumerResponse createServerConsumer(Destination destination,
                                                       String filterString,
                                                       boolean noLocal,
                                                       String subscriptionName,
                                                       boolean isCC) throws JMSException
   {

      checkSecurityCreateConsumerDelegate(destination, subscriptionName);

      try
      {
      	return createConsumerDelegateInternal(destination, filterString, noLocal, subscriptionName);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " createConsumerDelegate");
      }
   }

	private CreateBrowserResponse createServerBrowser(Destination destination,
                                                     String filterString)
      throws JMSException
	{
      security.check(destination, CheckType.READ, this.getConnectionEndpoint());

      try
      {
         return createBrowserDelegateInternal(destination, filterString);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " createBrowserDelegate");
      }
	}

   private JBossQueue createQueue(String name) throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Session is closed");
         }

         //FIXME - this method should not exist on the server

         Condition condition = new ConditionImpl(DestinationType.QUEUE, name);

         if (!postOffice.containsCondition(condition))
         {
            throw new JMSException("There is no administratively defined queue with name:" + name);
         }

         return new JBossQueue(name);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " createQueue");
      }
   }

   private JBossTopic createTopic(String name) throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Session is closed");
         }

         //FIXME - this method should not exist on the server

         Condition condition = new ConditionImpl(DestinationType.TOPIC, name);

         if (!postOffice.containsCondition(condition))
         {
            throw new JMSException("There is no administratively defined topic with name:" + name);
         }

         return new JBossTopic(name);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " createTopic");
      }
   }
   
   private void closing() throws JMSException
   {      
   }

   private void close() throws JMSException
   {
      try
      {
         localClose();

         connectionEndpoint.removeSession(id);

         connectionEndpoint.getMessagingServer().getRemotingService().getDispatcher().unregister(id);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      }
   }

   private void send(Message msg) throws JMSException
   {
      try
      {
         Destination dest = (Destination)msg.getHeader(org.jboss.messaging.core.Message.TEMP_DEST_HEADER_NAME);

         //Assign the message an internal id - this is used to key it in the store and also used to 
         //handle delivery
         
         msg.setMessageID(sp.getPersistenceManager().generateMessageID());
         
         // This allows the no-local consumers to filter out the messages that come from the same
         // connection.

         msg.setConnectionID(connectionEndpoint.getConnectionID());

         Condition condition = new ConditionImpl(dest.getType(), dest.getName());
         
         postOffice.route(condition, msg);
         
         //FIXME - this check belongs on the client side!!
         
         if (dest.getType() == DestinationType.QUEUE && msg.getReferences().isEmpty())
         {
            throw new InvalidDestinationException("Failed to route to queue " + dest.getName());
         }
         
         if (autoCommitSends)
         {
            if (msg.getNumDurableReferences() != 0)
            {
               sp.getPersistenceManager().addMessage(msg);
            }

            msg.send();
         }
         else
         {
            tx.addMessage(msg);
         }              	
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " send");
      }
   }

   private void addTemporaryDestination(Destination dest) throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Session is closed");
         }

         if (!dest.isTemporary())
         {
            throw new InvalidDestinationException("Destination:" + dest + " is not a temporary destination");
         }

         connectionEndpoint.addTemporaryDestination(dest);

         Condition condition = new ConditionImpl(dest.getType(), dest.getName());

         postOffice.addCondition(condition);

         //FIXME - comparisons like this do not belong on the server side
         //They should be computed on the client side
         if (dest.getType() == DestinationType.QUEUE)
         {

            postOffice.addQueue(condition, dest.getName(), null,
                                false, true, sp.getConfiguration().isClustered());

         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " addTemporaryDestination");
      }
   }

   private void deleteTemporaryDestination(Destination dest) throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Session is closed");
         }

         if (!dest.isTemporary())
         {
            throw new InvalidDestinationException("Destination:" + dest +
                                                  " is not a temporary destination");
         }

         Condition condition = new ConditionImpl(dest.getType(), dest.getName());

         //FIXME - comparisons like this should be done on the jms client not here
         if (dest.getType() == DestinationType.QUEUE)
         {
            List<Binding> bindings = postOffice.getBindingsForQueueName(dest.getName());

         	if (bindings.isEmpty())
         	{
         		throw new IllegalStateException("Cannot find binding for queue " + dest.getName());
         	}

         	Binding binding = bindings.get(0);

         	if (binding.getQueue().getConsumerCount() != 0)
         	{
         		throw new IllegalStateException("Cannot delete temporary queue if it has consumer(s)");
         	}

         	// temporary queues must be unbound on ALL nodes of the cluster

         	postOffice.removeQueue(condition, dest.getName(), sp.getConfiguration().isClustered());
         }
         else
         {
            //FIXME - this should be evaluated on the client side

            List<Binding> bindings = postOffice.getBindingsForCondition(new ConditionImpl(dest.getType(), dest.getName()));

            if (!bindings.isEmpty())
         	{
            	throw new IllegalStateException("Cannot delete temporary topic if it has consumer(s)");
         	}

            // There is no need to explicitly unbind the subscriptions for the temp topic, this is because we
            // will not get here unless there are no bindings.
            // Note that you cannot create surable subs on a temp topic
         }

         postOffice.removeCondition(condition);

         connectionEndpoint.removeTemporaryDestination(dest);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " deleteTemporaryDestination");
      }
   }

   private void unsubscribe(String subscriptionName) throws JMSException
   {
      log.trace(this + " unsubscribing " + subscriptionName);

      try
      {
         if (closed)
         {
            throw new IllegalStateException("Session is closed");
         }
         if (subscriptionName == null)
         {
            throw new InvalidDestinationException("Destination is null");
         }

         String clientID = connectionEndpoint.getClientID();

         if (clientID == null)
         {
            throw new JMSException("null clientID on connection");
         }

         //FIXME - this should be done on the client side

         String queueName = MessageQueueNameHelper.createSubscriptionName(clientID, subscriptionName);

         List<Binding> bindings = postOffice.getBindingsForQueueName(queueName);

         if (bindings.isEmpty())
         {
            throw new InvalidDestinationException("Cannot find durable subscription with name " +
                                                  subscriptionName + " to unsubscribe");
         }

         Queue sub = bindings.get(0).getQueue();

         //FIXME all this should be done on the jms client

         // Section 6.11. JMS 1.1.
         // "It is erroneous for a client to delete a durable subscription while it has an active
         // TopicSubscriber for it or while a message received by it is part of a current
         // transaction or has not been acknowledged in the session."

         if (sub.getConsumerCount() != 0)
         {
            throw new IllegalStateException("Cannot unsubscribe durable subscription " +
                                            subscriptionName + " since it has active subscribers");
         }

         //Also if it is clustered we must disallow unsubscribing if it has active consumers on other nodes

         //TODO - reimplement this for JBM2
//         if (sub.isClustered() && sp.getConfiguration().isClustered())
//         {
//         	Replicator rep = (Replicator)postOffice;
//
//         	Map map = rep.get(sub.getName());
//
//         	if (!map.isEmpty())
//         	{
//         		throw new IllegalStateException("Cannot unsubscribe durable subscription " +
//                     subscriptionName + " since it has active subscribers on other nodes");
//         	}
//         }

         //FIXME - again all this should be done on the client side jms client

         Condition condition = bindings.get(0).getCondition();

         postOffice.removeQueue(condition, sub.getName(), sub.isClustered() && sp.getConfiguration().isClustered());

         sp.getPersistenceManager().deleteAllReferences(sub);

         sub.removeAllReferences();

         //TODO - message counters should be handled automatically by the destination

//         String counterName = ManagedDestination.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + sub.getName();
//
//         MessageCounter counter = sp.getMessageCounterManager().unregisterMessageCounter(counterName);
//
//         if (counter == null)
//         {
//            throw new IllegalStateException("Cannot find counter to remove " + counterName);
//         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " unsubscribe");
      }
   }
   
   
   private synchronized void acknowledge(long deliveryID, boolean allUpTo) throws JMSException
   {
      //Note that we do not consider it an error if the deliveries cannot be found to be acked.
      //This can legitimately occur if a connection/session/consumer is closed from inside a MessageHandlers
      //onMessage method. In this situation the close will cancel any unacked deliveries, but the subsequent
      //call to delivered() will try and ack again and not find the last delivery on the server.
      try
      {
         if (allUpTo)
         {
            //Ack all deliveries up to and including the specified id
            
            for (Iterator<Delivery> iter = deliveries.iterator(); iter.hasNext();)
            {
               Delivery rec = iter.next();
               
               if (rec.getDeliveryID() <= deliveryID)
               {
                  iter.remove();
                  
                  MessageReference ref = rec.getReference();
                  
                  if (rec.getDeliveryID() > deliveryID)
                  {
                     //This catches the case where the delivery has been cancelled since it's expired
                     //And we don't want to end up acking all deliveries!
                     break;
                  }
                  
                  if (autoCommitAcks)
                  {
                     ref.acknowledge(sp.getPersistenceManager());
                  }
                  else
                  {
                     tx.addAcknowledgement(ref);
                  }
                  
                  if (rec.getDeliveryID() == deliveryID)
                  {
                     break;
                  }
               }
               else
               {
                  //Sanity check
                  throw new IllegalStateException("Failed to ack contiguently");
               }
            }
         }
         else
         {
            //Ack a specific delivery
            
            for (Iterator<Delivery> iter = deliveries.iterator(); iter.hasNext();)
            {
               Delivery rec = iter.next();
               
               if (rec.getDeliveryID() == deliveryID)
               {
                  iter.remove();
                  
                  MessageReference ref = rec.getReference();
                  
                  if (autoCommitAcks)
                  {
                     ref.acknowledge(sp.getPersistenceManager());
                  }
                  else
                  {
                     tx.addAcknowledgement(ref);
                  }
                   
                  break;
               }
            }            
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " acknowledge");
      }
   }
      
   private void rollback() throws JMSException
   {     
      try
      {                        
         if (tx == null)
         {
            //Might be null if XA
            
            tx = new TransactionImpl();
         }
         
         //Synchronize to prevent any new deliveries arriving during this recovery
         synchronized (this)
         {                     
            //Add any unacked deliveries into the tx
            //Doing this ensures all references are rolled back in the correct order
            //in a single contiguous block
            
            for (Delivery del: deliveries)
            {
               tx.addAcknowledgement(del.getReference());
            }
            
            deliveries.clear();
            
            deliveryIDSequence -= tx.getAcknowledgementsCount();
         }
            
         tx.rollback(sp.getPersistenceManager());          
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " commit");
      }
   }
   
   private void cancel(long deliveryID, boolean expired) throws JMSException
   {
      try
      {
         if (deliveryID == -1)
         {
            //Cancel all
            
            Transaction cancelTx;
            
            synchronized (this)
            {
               cancelTx = new TransactionImpl();
               
               for (Delivery del: deliveries)
               {
                  cancelTx.addAcknowledgement(del.getReference());
               }
               
               deliveries.clear();
            }
            
            cancelTx.rollback(sp.getPersistenceManager());
         }
         else
         {
            for (Iterator<Delivery> iter = deliveries.iterator(); iter.hasNext();)
            {
               Delivery delivery = iter.next();
               
               if (delivery.getDeliveryID() == deliveryID)
               {
                  //TODO - send to expiry queue
                  delivery.getReference().acknowledge(sp.getPersistenceManager());
               }
               
               iter.remove();
               
               break;                              
            }
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " commit");
      }
   }
   
   private void commit() throws JMSException
   {
      try
      {
         tx.commit(true, sp.getPersistenceManager());      
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " commit");
      }
   }
         
   private SessionXAResponse XACommit(boolean onePhase, Xid xid)
   {      
      try
      {
         if (tx != null)
         {
            final String msg = "Cannot commit, session is currently doing work in a transaction " + tx.getXid();
            
            return new SessionXAResponse(true, XAException.XAER_PROTO, msg);            
         }
         
         Transaction theTx = resourceManager.getTransaction(xid);
         
         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;
            
            return new SessionXAResponse(true, XAException.XAER_NOTA, msg);
         }
         
         if (theTx.isSuspended())
         { 
            return new SessionXAResponse(true, XAException.XAER_PROTO, "Cannot commit transaction, it is suspended " + xid);
         }
         
         theTx.commit(onePhase, sp.getPersistenceManager());      
         
         boolean removed = resourceManager.removeTransaction(xid);
         
         if (!removed)
         {
            final String msg = "Failed to remove transaction: " + xid;
            
            return new SessionXAResponse(true, XAException.XAER_PROTO, msg);
         }
         
         return new SessionXAResponse(false, XAResource.XA_OK, null);
      }
      catch (Exception e)
      {
         log.error("Failed to commit transaction branch", e);
         
         //Returning retry allows the tx manager to try again - otherwise heuristic action will
         //be needed
         return new SessionXAResponse(true, XAException.XA_RETRY, "Consult server logs for exception logging");
      }
   }
   
   private SessionXAResponse XAEnd(Xid xid, boolean failed)
   {  
      if (tx != null && tx.getXid().equals(xid))
      {
         if (tx.isSuspended())
         {
            final String msg = "Cannot end, transaction is suspended";
            
            return new SessionXAResponse(true, XAException.XAER_PROTO, msg);   
         }
         
         tx = null;
      }
      else
      {
         //It's also legal for the TM to call end for a Xid in the suspended state
         //See JTA 1.1 spec 3.4.4 - state diagram
         //Although in practice TMs rarely do this.
         Transaction theTx = resourceManager.getTransaction(xid);
         
         if (theTx == null)
         {
            final String msg = "Cannot find suspended transaction to end " + xid;
            
            return new SessionXAResponse(true, XAException.XAER_NOTA, msg);
         }
         
         if (!theTx.isSuspended())
         {
            final String msg = "Transaction is not suspended " + xid;
            
            return new SessionXAResponse(true, XAException.XAER_PROTO, msg);
         }
         
         theTx.resume();                  
      }

      return new SessionXAResponse(false, XAResource.XA_OK, null);
   }
   
   private SessionXAResponse XAForget(Xid xid)
   {      
      //Do nothing since we don't support heuristic commits / rollback from the resource manager
      
      return new SessionXAResponse(false, XAResource.XA_OK, null);
   }
   
   private SessionXAResponse XAJoin(Xid xid)
   {   
      try
      {
         Transaction theTx = resourceManager.getTransaction(xid);
         
         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;
            
            return new SessionXAResponse(true, XAException.XAER_NOTA, msg);
         }
         
         if (theTx.isSuspended())
         {
            return new SessionXAResponse(true, XAException.XAER_PROTO, "Cannot join tx, it is suspended " + xid);
         }
         
         tx = theTx;
         
         return new SessionXAResponse(false, XAResource.XA_OK, null);
      }
      catch (Exception e)
      {
         log.error("Failed to join transaction branch", e);

         return new SessionXAResponse(true, XAException.XAER_RMERR, "Consult server logs for exception logging");
      }          
   }
   
   private SessionXAResponse XAPrepare(Xid xid)
   {      
      try
      {
         if (tx != null)
         {
            final String msg = "Cannot commit, session is currently doing work in a transaction " + tx.getXid();
            
            return new SessionXAResponse(true, XAException.XAER_PROTO, msg);            
         }
         
         Transaction theTx = resourceManager.getTransaction(xid);
         
         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;
            
            return new SessionXAResponse(true, XAException.XAER_NOTA, msg);
         }
         
         if (theTx.isSuspended())
         { 
            return new SessionXAResponse(true, XAException.XAER_PROTO, "Cannot prepare transaction, it is suspended " + xid);
         }
         
         if (theTx.isEmpty())
         {
            //Nothing to do - remove it
            
            boolean removed = resourceManager.removeTransaction(xid);
            
            if (!removed)
            {
               final String msg = "Failed to remove transaction: " + xid;
               
               return new SessionXAResponse(true, XAException.XAER_PROTO, msg);
            }
            
            return new SessionXAResponse(false, XAResource.XA_RDONLY, null);
         }
         else
         {         
            theTx.prepare(sp.getPersistenceManager());
            
            return new SessionXAResponse(false, XAResource.XA_OK, null);
         }
      }
      catch (Exception e)
      {
         log.error("Failed to prepare transaction branch", e);
         
         return new SessionXAResponse(true, XAException.XAER_RMERR, "Consult server logs for exception logging");
      }
   }
   
   private SessionXAResponse XAResume(Xid xid)
   {            
      try
      {
         if (tx != null)
         {
            final String msg = "Cannot resume, session is currently doing work in a transaction " + tx.getXid();
            
            return new SessionXAResponse(true, XAException.XAER_PROTO, msg);            
         }
         
         Transaction theTx = resourceManager.getTransaction(xid);
         
         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;
            
            return new SessionXAResponse(true, XAException.XAER_NOTA, msg);
         }
         
         if (!theTx.isSuspended())
         { 
            return new SessionXAResponse(true, XAException.XAER_PROTO, "Cannot resume transaction, it is not suspended " + xid);
         }
         
         tx = theTx;
         
         tx.resume();
         
         return new SessionXAResponse(false, XAResource.XA_OK, null);
      }
      catch (Exception e)
      {
         log.error("Failed to join transaction branch", e);

         return new SessionXAResponse(true, XAException.XAER_RMERR, "Consult server logs for exception logging");
      }         
   }
    
   private SessionXAResponse XARollback(Xid xid)
   {      
      try
      {
         if (tx != null)
         {
            final String msg = "Cannot roll back, session is currently doing work in a transaction " + tx.getXid();
            
            return new SessionXAResponse(true, XAException.XAER_PROTO, msg);            
         }
         
         Transaction theTx = resourceManager.getTransaction(xid);
         
         if (theTx == null)
         {
            final String msg = "Cannot find xid in resource manager: " + xid;
            
            return new SessionXAResponse(true, XAException.XAER_NOTA, msg);
         }
         
         if (theTx.isSuspended())
         { 
            return new SessionXAResponse(true, XAException.XAER_PROTO, "Cannot rollback transaction, it is suspended " + xid);
         }
                  
         theTx.rollback(sp.getPersistenceManager());
         
         boolean removed = resourceManager.removeTransaction(xid);
         
         if (!removed)
         {
            final String msg = "Failed to remove transaction: " + xid;
            
            return new SessionXAResponse(true, XAException.XAER_PROTO, msg);
         }
         
         return new SessionXAResponse(false, XAResource.XA_OK, null);                  
      }
      catch (Exception e)
      {
         log.error("Failed to roll back transaction branch", e);

         return new SessionXAResponse(true, XAException.XAER_RMERR, "Consult server logs for exception logging");
      }         
   }
   
   private SessionXAResponse XAStart(Xid xid)
   {      
      if (tx != null)
      {
         final String msg = "Cannot start, session is already doing work in a transaction " + tx.getXid();
         
         return new SessionXAResponse(true, XAException.XAER_PROTO, msg);            
      }
      
      tx = new TransactionImpl(xid);
      
      boolean added = resourceManager.putTransaction(xid, tx);
      
      if (!added)
      {
         final String msg = "Cannot start, there is already a xid " + tx.getXid();
         
         return new SessionXAResponse(true, XAException.XAER_DUPID, msg);          
      }
      
      return new SessionXAResponse(false, XAResource.XA_OK, null);     
   }
   
   private SessionXAResponse XASuspend() throws JMSException
   {      
      if (tx == null)
      {
         final String msg = "Cannot suspend, session is not doing work in a transaction " + tx.getXid();
         
         return new SessionXAResponse(true, XAException.XAER_PROTO, msg);            
      }  
      
      if (tx.isSuspended())
      {
         final String msg = "Cannot suspend, transaction is already suspended " + tx.getXid();
         
         return new SessionXAResponse(true, XAException.XAER_PROTO, msg);   
      }
      
      tx.suspend();
      
      tx = null;
      
      return new SessionXAResponse(false, XAResource.XA_OK, null);   
   }
   
   private List<Xid> getInDoubtXids() throws JMSException
   {
      return null;
   }
   
   private int getXATimeout()
   {
      return resourceManager.getTimeoutSeconds();
   }
   
   private boolean setXATimeout(int timeoutSeconds)
   {
      return resourceManager.setTimeoutSeconds(timeoutSeconds);
   }
      
   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

//   private void cancelDeliveryInternal(Cancel cancel) throws Exception
//   {
//      DeliveryRecord rec = (DeliveryRecord)deliveries.remove(cancel.getDeliveryId());
//
//      if (rec == null)
//      {
//         //The delivery might not be found, if the session is not replicated (i.e. auto_ack or dups_ok)
//      	//and has failed over since recoverDeliveries won't have been called
//      	if (trace)
//      	{
//      		log.trace("Cannot find delivery to cancel, session probably failed over and is not replicated");
//      	}
//      	return;
//      }
//
//      MessageReference ref = rec.ref;
//
//      //Note we check the flag *and* evaluate again, this is because the server and client clocks may
//      //be out of synch and don't want to send back to the client a message it thought it has sent to
//      //the expiry queue
//      boolean expired = cancel.isExpired() || ref.getMessage().isExpired();
//
//      //Note we check the flag *and* evaluate again, this is because the server value of maxDeliveries
//      //might get changed after the client has sent the cancel - and we don't want to end up cancelling
//      //back to the original queue
//      boolean reachedMaxDeliveryAttempts =
//         cancel.isReachedMaxDeliveryAttempts() || cancel.getDeliveryCount() >= rec.maxDeliveryAttempts;
//
//      if (!expired && !reachedMaxDeliveryAttempts)
//      {
//         //Normal cancel back to the queue
//
//         ref.setDeliveryCount(cancel.getDeliveryCount());
//
//         //Do we need to set a redelivery delay?
//
//         if (rec.redeliveryDelay != 0)
//         {
//            ref.setScheduledDeliveryTime(System.currentTimeMillis() + rec.redeliveryDelay);
//         }
//
//         if (trace) { log.trace("Cancelling delivery " + cancel.getDeliveryId()); }
//
//         ref.cancel(sp.getPersistenceManager());
//
//      }
//      else
//      {
//         if (expired)
//         {
//            //Sent to expiry queue
//
//            Message copy = makeCopyForDLQOrExpiry(true, ref);
//
//            moveInTransaction(copy, ref, rec.expiryQueue, false);
//         }
//         else
//         {
//            //Send to DLQ
//
//            Message copy = makeCopyForDLQOrExpiry(false, ref);
//
//            moveInTransaction(copy, ref, rec.dlq, true);
//         }
//      }
//   }

   private Message makeCopyForDLQOrExpiry(boolean expiry, MessageReference ref) throws Exception
   {
      //We copy the message and send that to the dlq/expiry queue - this is because
      //otherwise we may end up with a ref with the same message id in the queue more than once
      //which would barf - this might happen if the same message had been expire from multiple
      //subscriptions of a topic for example
      //We set headers that hold the original message destination, expiry time and original message id

   	if (trace) { log.trace("Making copy of message for DLQ or expiry " + ref); }

      Message msg = ref.getMessage();

      Message copy = msg.copy();

      long newMessageId = sp.getPersistenceManager().generateMessageID();

      copy.setMessageID(newMessageId);

      //reset expiry
      copy.setExpiration(0);


      //TODO
// http://jira.jboss.org/jira/browse/JBMESSAGING-1202
//      String origMessageId = msg.getJMSMessageID();
//
//      String origDest = msg.getJMSDestination().toString();
//
//      copy.setStringProperty(JBossMessage.JBOSS_MESSAGING_ORIG_MESSAGE_ID, origMessageId);
//
//      copy.setStringProperty(JBossMessage.JBOSS_MESSAGING_ORIG_DESTINATION, origDest);
//
//      if (expiry)
//      {
//         long actualExpiryTime = System.currentTimeMillis();
//
//         copy.setLongProperty(JBossMessage.JBOSS_MESSAGING_ACTUAL_EXPIRY_TIME, actualExpiryTime);
//      }

      return copy;
   }

//   private void moveInTransaction(Message msg, MessageReference ref, Queue queue, boolean dlq) throws Exception
//   {
//      List<Message> msgs = new ArrayList<Message>();
//
//      msgs.add(msg);
//
//      List<MessageReference> refs = new ArrayList<MessageReference>();
//
//      refs.add(ref);
//
//      Transaction tx = new TransactionImpl(msgs, refs, msg.isDurable());
//
//      //FIXME - clear up these ugly refs to the pm
//      tx.commit(getConnectionEndpoint().getMessagingServer().getPersistenceManager());
//
////      MessageReference ref = msg.createReference();
////
////      try
////      {
////         if (queue != null)
////         {
////            queue.handle(null, ref, tx);
////            del.acknowledge(tx);
////         }
////         else
////         {
////            log.warn("No " + (dlq ? "DLQ" : "expiry queue") + " has been specified so the message will be removed");
////
////            del.acknowledge(tx);
////         }
////
////         tx.commit();
////      }
////      catch (Throwable t)
////      {
////         tx.rollback();
////         throw t;
////      }
//   }

   private CreateConsumerResponse createConsumerDelegateInternal(Destination destination,
                                                                 String filterString,
                                                                 boolean noLocal,
                                                                 String subscriptionName)
      throws Exception
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }

      if ("".equals(filterString))
      {
         filterString = null;
      }

      if (trace)
      {
         log.trace(this + " creating consumer for " + destination +
            (filterString == null ? "" : ", filter '" + filterString + "'") +
            (subscriptionName == null ? "" : ", subscription '" + subscriptionName + "'") +
            (noLocal ? ", noLocal" : ""));
      }

      Condition condition = new ConditionImpl(destination.getType(), destination.getName());

      if (!postOffice.containsCondition(condition))
      {
         throw new InvalidDestinationException("No such destination: " + destination.getName() + " has it been deployed?");
      }

      if (destination.isTemporary())
      {
         // Can only create a consumer for a temporary destination on the same connection
         // that created it
         if (!connectionEndpoint.hasTemporaryDestination(destination))
         {
            String msg = "Cannot create a message consumer on a different connection " +
                         "to that which created the temporary destination";
            throw new IllegalStateException(msg);
         }
      }

      String consumerID = UUID.randomUUID().toString();

      // Always validate the filter first
      Filter filter = null;

      if (filterString != null)
      {
         try
         {
            filter = new FilterImpl(filterString);
         }
         catch (Exception e)
         {
            throw new InvalidSelectorException("Invalid selector " + filterString);
         }
      }

      Queue queue;

      //FIXME - all this logic belongs on the jms client side

      if (destination.getType() == DestinationType.TOPIC)
      {
         if (subscriptionName == null)
         {
            // non-durable subscription
            if (log.isTraceEnabled()) { log.trace(this + " creating new non-durable subscription on " + destination); }

            queue = postOffice.addQueue(condition, UUID.randomUUID().toString(), filter, false, false, false);

            //TODO - message counters should be applied by the queue configurator factory

         }
         else
         {
            if (destination.isTemporary())
            {
               throw new InvalidDestinationException("Cannot create a durable subscription on a temporary topic");
            }

            // We have a durable subscription, look it up
            String clientID = connectionEndpoint.getClientID();
            if (clientID == null)
            {
               throw new JMSException("Cannot create durable subscriber without a valid client ID");
            }

            // See if there any bindings with the same client_id.subscription_name name

            String name = MessageQueueNameHelper.createSubscriptionName(clientID, subscriptionName);

            List<Binding> bindings = postOffice.getBindingsForQueueName(name);

            Binding binding = null;

            if (!bindings.isEmpty())
            {
               binding = bindings.get(0);
            }

            if (binding == null)
            {
               // Does not already exist

               if (trace) { log.trace(this + " creating new durable subscription on " + destination); }

               queue = postOffice.addQueue(condition, name, filter, true, false,
                                           sp.getConfiguration().isClustered());

               //TODO message counters handled by queue configurator

            }
            else
            {
               //Durable sub already exists

            	queue = binding.getQueue();

               if (trace) { log.trace(this + " subscription " + subscriptionName + " already exists"); }

            	//Check if it is already has a subscriber
            	//We can't have more than one subscriber at a time on the durable sub

               if (queue.getConsumerCount() > 0)
               {
               	throw new IllegalStateException("Cannot create a subscriber on the durable subscription since it already has subscriber(s)");
               }

               //TODO - apply message counters on the queue configurator

               // From javax.jms.Session Javadoc (and also JMS 1.1 6.11.1):
               // A client can change an existing durable subscription by creating a durable
               // TopicSubscriber with the same name and a new topic and/or message selector.
               // Changing a durable subscriber is equivalent to unsubscribing (deleting) the old
               // one and creating a new one.

               String oldFilterString = queue.getFilter() != null ? queue.getFilter().getFilterString() : null;

               boolean selectorChanged =
                  (filterString == null && oldFilterString != null) ||
                  (oldFilterString == null && filterString != null) ||
                  (oldFilterString != null && filterString != null &&
                           !oldFilterString.equals(filterString));

               if (trace) { log.trace("selector " + (selectorChanged ? "has" : "has NOT") + " changed"); }

               //FIXME - all this needs to be on the jms client

               String oldTopicName = binding.getCondition().getKey();

               boolean topicChanged = !oldTopicName.equals(destination.getName());

               if (log.isTraceEnabled()) { log.trace("topic " + (topicChanged ? "has" : "has NOT") + " changed"); }

               if (selectorChanged || topicChanged)
               {
                  if (trace) { log.trace("topic or selector changed so deleting old subscription"); }

                  // Unbind the durable subscription

                  // Durable subs must be unbound on ALL nodes of the cluster

                  postOffice.removeQueue(binding.getCondition(), queue.getName(), sp.getConfiguration().isClustered());

                  sp.getPersistenceManager().deleteAllReferences(queue);

                  queue.removeAllReferences();

                  queue = postOffice.addQueue(condition, name, filter, true, false, sp.getConfiguration().isClustered());

               }

               //TODO counter creation is handled in the queue configurator

            }
         }
      }
      else
      {
         // ClientConsumer on a jms queue

      	List<Binding> bindings = postOffice.getBindingsForQueueName(destination.getName());

         if (bindings.isEmpty())
         {
            throw new IllegalStateException("Cannot find queue: " + destination.getName());
         }

         queue = bindings.get(0).getQueue();
      }

      int prefetchSize = connectionEndpoint.getPrefetchSize();

      Queue dlqToUse = queue.getDLQ() == null ? defaultDLQ : queue.getDLQ();

      Queue expiryQueueToUse = queue.getExpiryQueue() == null ? defaultExpiryQueue : queue.getExpiryQueue();

      int maxDeliveryAttemptsToUse = queue.getMaxDeliveryAttempts() == -1 ? defaultMaxDeliveryAttempts : queue.getMaxDeliveryAttempts();

      long redeliveryDelayToUse = queue.getRedeliveryDelay() == -1 ? defaultRedeliveryDelay : queue.getRedeliveryDelay();

      ServerConsumerEndpoint ep =
         new ServerConsumerEndpoint(sp, consumerID, queue,
                                    queue.getName(), this, filter, noLocal,
                                    destination, dlqToUse, expiryQueueToUse, redeliveryDelayToUse,
                                    maxDeliveryAttemptsToUse, prefetchSize);

      //TODO implements this for JBM2

//      if (queue.isClustered() && sp.getConfiguration().isClustered() && jmsDestination.isTopic() && subscriptionName != null)
//      {
//      	//Clustered durable sub consumer created - we need to add this info in the replicator - it is needed by other nodes
//
//      	//This is also used to prevent a possible race condition where a clustered durable sub is bound on all nodes
//      	//but then unsubscribed before the bind is complete on all nodes, leaving it bound on some nodes and not on others
//      	//The bind all is synchronous so by the time we add the x to the replicator we know it is bound on all nodes
//      	//and same to unsubscribe
//
//      	Replicator rep = (Replicator)postOffice;
//
//      	rep.put(queue.getName(), DUR_SUB_STATE_CONSUMERS);
//      }
      connectionEndpoint.getMessagingServer().getRemotingService().getDispatcher().register(ep.newHandler());

      CreateConsumerResponse response = new CreateConsumerResponse(consumerID, prefetchSize,
                                                                   maxDeliveryAttemptsToUse, redeliveryDelayToUse );

      synchronized (consumers)
      {
         consumers.put(consumerID, ep);
      }

      log.trace(this + " created and registered " + ep);

      return response;
   }

   private CreateBrowserResponse createBrowserDelegateInternal(Destination destination,
                                                         String selector) throws Exception
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }

      if (destination == null)
      {
         throw new InvalidDestinationException("null destination");
      }

      //FIXME - this belongs in JMS client - not here

      if (destination.getType() == DestinationType.TOPIC)
      {
         throw new IllegalStateException("Cannot browse a topic");
      }

      Condition condition = new ConditionImpl(DestinationType.QUEUE, destination.getName());

      List<Binding> bindings = this.postOffice.getBindingsForCondition(condition);

      if (bindings.isEmpty())
      {
         throw new InvalidDestinationException("No such destination: " + destination);
      }

      log.trace(this + " creating browser for " + destination +
         (selector == null ? "" : ", selector '" + selector + "'"));

      Binding binding = bindings.get(0);

      String browserID = UUID.randomUUID().toString();

      ServerBrowserEndpoint ep = new ServerBrowserEndpoint(this, browserID, binding.getQueue(), selector);

      // still need to synchronized since close() can come in on a different thread
      synchronized (browsers)
      {
         browsers.put(browserID, ep);
      }

      connectionEndpoint.getMessagingServer().getRemotingService().getDispatcher().register(ep.newHandler());

      
      log.trace(this + " created and registered " + ep);

      return new CreateBrowserResponse(browserID);
   }
   
   private void checkSecurityCreateConsumerDelegate(Destination dest, String subscriptionName ) throws JMSException
   {
      security.check(dest, CheckType.READ, this.getConnectionEndpoint());

      // if creating a durable subscription then need create permission

      if (subscriptionName != null)
      {
         // durable
         security.check(dest, CheckType.CREATE, this.getConnectionEndpoint());
      }
   }
    

   public PacketHandler newHandler()
   {
      return new SessionAdvisedPacketHandler();
   }

   
   // Inner classes --------------------------------------------------------------------------------   

   private class SessionAdvisedPacketHandler implements PacketHandler
   {
      public SessionAdvisedPacketHandler()
      {
      }

      public String getID()
      {
         return ServerSessionEndpoint.this.id;
      }

      public void handle(Packet packet, PacketSender sender)
      {
         try
         {
            Packet response = null;

            PacketType type = packet.getType();
            
            //TODO use a switch for this
            if (type == MSG_SENDMESSAGE)
            {
               SessionSendMessage message = (SessionSendMessage) packet;
              
               send(message.getMessage());

               if (message.getMessage().isDurable())
               {
                  response = new NullPacket();
               }

            } else if (type == REQ_CREATECONSUMER)
            {
               CreateConsumerRequest request = (CreateConsumerRequest) packet;
               response = createServerConsumer(
                                request.getDestination(), request.getSelector(), request
                           .isNoLocal(), request.getSubscriptionName(), request
                           .isConnectionConsumer());
            } else if (type == REQ_CREATEDESTINATION)
            {
               CreateDestinationRequest request = (CreateDestinationRequest) packet;
               JBossDestination destination;
               if (request.isQueue())
               {
                  destination = createQueue(request.getName());
               } else
               {
                  destination = createTopic(request.getName());
               }

               response = new CreateDestinationResponse(destination);
            } else if (type == REQ_CREATEBROWSER)
            {
               CreateBrowserRequest request = (CreateBrowserRequest) packet;
               response = createServerBrowser(
                     request.getDestination(), request.getSelector());
            }
            else if (type == PacketType.MSG_CLOSING)
            {
               closing();
            } else if (type == MSG_CLOSE)
            {
               close();
            } else if (type == MSG_UNSUBSCRIBE)
            {
               UnsubscribeMessage message = (UnsubscribeMessage) packet;
               unsubscribe(message.getSubscriptionName());
            } else if (type == MSG_ADDTEMPORARYDESTINATION)
            {
               AddTemporaryDestinationMessage message = (AddTemporaryDestinationMessage) packet;
               addTemporaryDestination(message.getDestination());
            } else if (type == MSG_DELETETEMPORARYDESTINATION)
            {
               DeleteTemporaryDestinationMessage message = (DeleteTemporaryDestinationMessage) packet;
               deleteTemporaryDestination(message.getDestination());
            }            
            else if (type == PacketType.MSG_ACKNOWLEDGE)
            {
               SessionAcknowledgeMessage message = (SessionAcknowledgeMessage)packet;
               acknowledge(message.getDeliveryID(), message.isAllUpTo());
            }
            else if (type == PacketType.MSG_COMMIT)
            {
               commit();
            }
            else if (type == PacketType.MSG_ROLLBACK)
            {
               rollback();
            }
            else if (type == PacketType.MSG_CANCEL)
            {
               SessionCancelMessage message = (SessionCancelMessage)packet;
               cancel(message.getDeliveryID(), message.isExpired());
            }
            else if (type == PacketType.MSG_XA_COMMIT)
            {
               SessionXACommitMessage message = (SessionXACommitMessage)packet;
               
               response = XACommit(message.isOnePhase(), message.getXid());
            }
            else if (type == PacketType.MSG_XA_END)
            { 
               SessionXAEndMessage message = (SessionXAEndMessage)packet;
               
               response = XAEnd(message.getXid(), message.isFailed());
            }
            else if (type == PacketType.MSG_XA_FORGET)
            {
               SessionXAForgetMessage message = (SessionXAForgetMessage)packet;
               
               response = XAForget(message.getXid());
            }
            else if (type == PacketType.MSG_XA_JOIN)
            {
               SessionXAJoinMessage message = (SessionXAJoinMessage)packet;
               
               response = XAJoin(message.getXid());
            }
            else if (type == PacketType.MSG_XA_RESUME)
            {
               SessionXAResumeMessage message = (SessionXAResumeMessage)packet;
               
               response = XAResume(message.getXid());
            }
            else if (type == PacketType.MSG_XA_ROLLBACK)
            {
               SessionXARollbackMessage message = (SessionXARollbackMessage)packet;
               
               response = XARollback(message.getXid());
            }
            else if (type == PacketType.MSG_XA_START)
            {
               SessionXAStartMessage message = (SessionXAStartMessage)packet;
               
               response = XAStart(message.getXid());
            }
            else if (type == PacketType.MSG_XA_SUSPEND)
            {
               response = XASuspend();
            }   
            else if (type == PacketType.REQ_XA_PREPARE)
            {
               SessionXAPrepareMessage message = (SessionXAPrepareMessage)packet;
               
               response = XAPrepare(message.getXid());
            }
            else if (type == PacketType.REQ_XA_INDOUBT_XIDS)
            {
               List<Xid> xids = getInDoubtXids();
               
               response = new SessionXAGetInDoubtXidsResponse(xids);
            }
            else if (type == PacketType.MSG_XA_GET_TIMEOUT)
            {
               response = new SessionXAGetTimeoutResponse(getXATimeout());
            }
            else if (type == PacketType.MSG_XA_SET_TIMEOUT)
            {
               SessionXASetTimeoutMessage message = (SessionXASetTimeoutMessage)packet;
               
               response = new SessionXASetTimeoutResponse(setXATimeout(message.getTimeoutSeconds()));
            }
            else
            {
               response = new JMSExceptionMessage(new MessagingJMSException(
                     "Unsupported packet for browser: " + packet));
            }

            // reply if necessary
            if (response == null && packet.isOneWay() == false)
            {
               response = new NullPacket();               
            }
            
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
         return "SessionAdvisedPacketHandler[id=" + id + "]";
      }
   }
   
   

}
