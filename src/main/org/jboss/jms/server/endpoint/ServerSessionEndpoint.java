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

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ACKDELIVERIES;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ADDTEMPORARYDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCELDELIVERIES;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCELDELIVERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELETETEMPORARYDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UNSUBSCRIBE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_ACKDELIVERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CLOSING;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEDESTINATION;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.jboss.jms.client.api.ClientBrowser;
import org.jboss.jms.client.api.Consumer;
import org.jboss.jms.client.delegate.ClientBrowserDelegate;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.delegate.Ack;
import org.jboss.jms.delegate.Cancel;
import org.jboss.jms.delegate.DefaultAck;
import org.jboss.jms.delegate.DeliveryInfo;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.container.SecurityAspect;
import org.jboss.jms.server.security.CheckType;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.Condition;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.Transaction;
import org.jboss.messaging.core.TransactionSynchronization;
import org.jboss.messaging.core.impl.ConditionImpl;
import org.jboss.messaging.core.impl.TransactionImpl;
import org.jboss.messaging.core.impl.filter.FilterImpl;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveryRequest;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveryResponse;
import org.jboss.messaging.core.remoting.wireformat.AddTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.CancelDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.CancelDeliveryMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationResponse;
import org.jboss.messaging.core.remoting.wireformat.DeleteTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SendMessage;
import org.jboss.messaging.core.remoting.wireformat.UnsubscribeMessage;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.MessageQueueNameHelper;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedLong;

/**
 * Session implementation
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
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
public class ServerSessionEndpoint implements SessionEndpoint
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerSessionEndpoint.class);
   
   static final String DUR_SUB_STATE_CONSUMERS = "C";
   
   static final String TEMP_QUEUE_MESSAGECOUNTER_PREFIX = "TempQueue.";
   
   private static final long DELIVERY_WAIT_TIMEOUT = 5 * 1000;
   
   private static final long CLOSE_WAIT_TIMEOUT = 5 * 1000;
      
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private SecurityAspect security = new SecurityAspect();

   private boolean trace = log.isTraceEnabled();

   private String id;

   private volatile boolean closed;

   private ServerConnectionEndpoint connectionEndpoint;
   
   private MessagingServer sp;

   private Map consumers;
   private Map browsers;

   private DestinationManager dm;
   private PostOffice postOffice;
   private int nodeId;
   private int defaultMaxDeliveryAttempts;
   private long defaultRedeliveryDelay;
   private Queue defaultDLQ;
   private Queue defaultExpiryQueue;

   private Object deliveryLock = new Object();
      
   // Map <deliveryID, Delivery>
   private Map deliveries;
   
   private SynchronizedLong deliveryIdSequence;
   
   //Temporary until we have our own NIO transport   
   QueuedExecutor executor = new QueuedExecutor(new LinkedQueue());
   
   private LinkedQueue toDeliver = new LinkedQueue();
   
   private boolean waitingToClose = false;
   
   private Object waitLock = new Object();
   
   // Constructors ---------------------------------------------------------------------------------

   ServerSessionEndpoint(String sessionID, ServerConnectionEndpoint connectionEndpoint) throws Exception
   {
      this.id = sessionID;

      this.connectionEndpoint = connectionEndpoint;
       
      sp = connectionEndpoint.getMessagingServer();

      dm = sp.getDestinationManager();
      
      postOffice = sp.getPostOffice(); 
      
      nodeId = sp.getConfiguration().getMessagingServerID();
      
      consumers = new HashMap();
      
		browsers = new HashMap();
      
      defaultDLQ = sp.getDefaultDLQInstance();
      
      defaultExpiryQueue = sp.getDefaultExpiryQueueInstance();
      
      defaultMaxDeliveryAttempts = sp.getConfiguration().getDefaultMaxDeliveryAttempts();
      
      defaultRedeliveryDelay = sp.getConfiguration().getDefaultRedeliveryDelay();
      
      deliveries = new ConcurrentHashMap();
      
      deliveryIdSequence = new SynchronizedLong(0);
   }
   
   // SessionDelegate implementation ---------------------------------------------------------------


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

   public Consumer createConsumerDelegate(Destination destination,
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

	public ClientBrowser createBrowserDelegate(Destination destination,
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

   public JBossQueue createQueue(String name) throws JMSException
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

   public JBossTopic createTopic(String name) throws JMSException
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
   
   public void close() throws JMSException
   {
      try
      {
         localClose();
         
         connectionEndpoint.removeSession(id);
         
         connectionEndpoint.getMessagingServer().getMinaService().getDispatcher().unregister(id);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      }
   }
      
   public long closing(long sequence) throws JMSException
   {
      if (trace) log.trace(this + " closing");
      
      // Wait for last np message to arrive
      
      if (sequence != 0)
      {
      	synchronized (waitLock)
      	{      		
      		long wait = CLOSE_WAIT_TIMEOUT;
      		
	      	while (sequence != expectedSequence && wait > 0)
	      	{
	      		long start = System.currentTimeMillis(); 
	      		try
	      		{
	      			waitLock.wait();
	      		}
	      		catch (InterruptedException e)
	      		{	      			
	      		}
	      		wait -= (System.currentTimeMillis() - start);
	      	}
	      	
	      	if (wait <= 0)
	      	{
	      		log.warn("Timed out waiting for last message");
	      	}
      	}      	
      }      
      
      return -1;
   }
 
   private volatile long expectedSequence = 0;
   
   public void send(Message message) throws JMSException
   {
   	throw new IllegalStateException("Should not be handled on the server");
   }
   
   public void send(Message message, long thisSequence) throws JMSException
   {
      try
      {                
      	if (thisSequence != -1)
      	{
      		synchronized (waitLock)
      		{	      		      	        
   				connectionEndpoint.sendMessage(message); 
   				
      			expectedSequence++;
     			
	      		waitLock.notify();
      		}
      	}
      	else
      	{
      		connectionEndpoint.sendMessage(message);
      	}        
      	
      	if (message.isDurable())
      	{
      	   sp.getPersistenceManager().addMessage(message);
      	}
      	
      	message.send();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " send");
      }
   }
   
   public boolean acknowledgeDelivery(Ack ack) throws JMSException
   {
      try
      {
         return acknowledgeDeliveryInternal(ack);   
      }
      catch (Throwable t)
      {
         JMSException e = ExceptionUtil.handleJMSInvocation(t, this + " acknowledgeDelivery");
         
         throw e;
      }
   }     
         
   public void acknowledgeDeliveries(List acks) throws JMSException
   {    
      if (trace) {log.trace(this + " acknowledges deliveries " + acks); }
      
      try
      {
         Iterator iter = acks.iterator();
         
         while (iter.hasNext())
         {
            Ack ack = (Ack)iter.next();
            
            acknowledgeDeliveryInternal(ack);
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " acknowledgeDeliveries");
      }
   }
             
   public void cancelDelivery(Cancel cancel) throws JMSException
   {
      try
      {
         if (trace) {log.trace(this + " cancelDelivery " + cancel); }
         
         cancelDeliveryInternal(cancel);          
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " acknowledgeDeliveries");
      }
   }            

   public void cancelDeliveries(List cancels) throws JMSException
   {
      if (trace) {log.trace(this + " cancels deliveries " + cancels); }
        
      try
      {
         // deliveries must be cancelled in reverse order
                   
         for (int i = cancels.size() - 1; i >= 0; i--)
         {
            Cancel cancel = (Cancel)cancels.get(i);       
            
            if (trace) { log.trace(this + " cancelling delivery " + cancel.getDeliveryId()); }
                        
            cancelDeliveryInternal(cancel);
         }
                 
         if (trace) { log.trace("Cancelled deliveries"); }
         
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " cancelDeliveries");
      }
   }         
      
   public void addTemporaryDestination(Destination dest) throws JMSException
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
   
   public void deleteTemporaryDestination(Destination dest) throws JMSException
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
   
   public void unsubscribe(String subscriptionName) throws JMSException
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

   public int getDupsOKBatchSize()
   {
      throw new RuntimeException("information only available on client side");
   }

   public boolean isStrictTck()
   {
      throw new RuntimeException("information only available on client side");
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
      
      if (expiryQueue != null)
      {
         if (trace) { log.trace(this + " sending expired message to expiry queue " + expiryQueue); }
         
         Message copy = makeCopyForDLQOrExpiry(true, ref);
         
         moveInTransaction(copy, ref, expiryQueue, true);
      }
      else
      {
         log.warn("No expiry queue has been configured so removing expired " + ref);
         
         //TODO - tidy up these references - ugly
         ref.acknowledge(this.getConnectionEndpoint().getMessagingServer().getPersistenceManager());
      }
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
      
      
      //Now cancel any remaining deliveries in reverse delivery order
      //Note we don't maintain order using a LinkedHashMap since then we lose
      //concurrency since we would have to lock it exclusively
      
      List entries = new ArrayList(deliveries.entrySet());
      
      //Sort them in reverse delivery id order
      Collections.sort(entries,
                       new Comparator()
                       {
                           public int compare(Object obj1, Object obj2)
                           {
                              Map.Entry entry1 = (Map.Entry)obj1;
                              Map.Entry entry2 = (Map.Entry)obj2;
                              Long id1 = (Long)entry1.getKey();
                              Long id2 = (Long)entry2.getKey();
                              return id2.compareTo(id1);
                           } 
                       });

      Iterator iter = entries.iterator();
              
      if (trace) { log.trace(this + " cancelling " + entries.size() + " deliveries"); }
      
      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry)iter.next();
         
         if (trace) { log.trace(this + " cancelling delivery with delivery id: " + entry.getKey()); }
         
         DeliveryRecord rec = (DeliveryRecord)entry.getValue();

         rec.ref.cancel(this.sp.getPersistenceManager());         
      }
      
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
   
   void cancelDelivery(long deliveryId) throws Exception
   {
      DeliveryRecord rec = (DeliveryRecord)deliveries.remove(new Long(deliveryId));
      
      if (rec == null)
      {
         throw new IllegalStateException("Cannot find delivery to cancel " + deliveryId);
      }
      
      rec.ref.cancel(this.sp.getPersistenceManager());
   }         
      
   //TODO NOTE! This needs to be synchronized to prevent deliveries coming back
   //out of order! There maybe some better way of doing this 
   synchronized void handleDelivery(MessageReference ref, ServerConsumerEndpoint consumer) throws Exception
   {
   	 long deliveryId = -1;
   	 
   	 DeliveryRecord rec = null;
   	 
   	 deliveryId = deliveryIdSequence.increment();   	 
   	 
   	 if (trace) { log.trace("Delivery id is now " + deliveryId); }
   	 
   	 //TODO can't we combine flags isRetainDeliveries and isReplicating - surely they're mutually exclusive?
       if (consumer.isRetainDeliveries())
       {      	 
      	 // Add a delivery

      	 rec = new DeliveryRecord(ref, consumer, deliveryId);
          
          deliveries.put(new Long(deliveryId), rec);
          
          if (trace) { log.trace(this + " added delivery " + deliveryId + ": " + ref); }
       }
       else
       {
       	//Acknowledge it now
       	try
       	{
       		//This basically just releases the memory reference
       		
       		if (trace) { log.trace("Acknowledging delivery now"); }
       		     		
       		ref.acknowledge(connectionEndpoint.getMessagingServer().getPersistenceManager());
       	}
       	catch (Throwable t)
       	{
       		log.error("Failed to acknowledge delivery", t);
       	}
       }
       

       performDelivery(ref, deliveryId, consumer); 	                             
   }
   
   void performDelivery(MessageReference ref, long deliveryID, ServerConsumerEndpoint consumer)
   {
   	if (consumer == null)
   	{
   		if (trace) { log.trace(this + " consumer is null, cannot perform delivery"); }
   		
   		return;
   	}
      
      if (consumer.isDead())
      {
         //Ignore any responses that come back after consumer has died
         return;
      }
   	
   	if (trace) { log.trace(this + " performing delivery for " + ref); }
   	   	
      // We send the message to the client on the current thread. The message is written onto the
      // transport and then the thread returns immediately without waiting for a response.

   	DeliverMessage m = new DeliverMessage(ref.getMessage(), consumer.getID(), deliveryID, ref.getDeliveryCount());
   	m.setVersion(getConnectionEndpoint().getUsingVersion());
   	consumer.deliver(m);
   	
   	//TODO - what if we get an exception from MINA? 
   	//Surely we need to do exception logic too???

//      }
//      catch (Throwable t)
//      {
//         // it's an oneway callback, so exception could only have happened on the server, while
//         // trying to send the callback. This is a good reason to smack the whole connection.
//         // I trust remoting to have already done its own cleanup via a CallbackErrorHandler,
//         // I need to do my own cleanup at ConnectionManager level.
//
//         log.trace(this + " failed to handle callback", t);
//         
//         //We stop the consumer - some time later the lease will expire and the connection will be closed        
//         //which will remove the consumer
//         
//         consumer.setStarted(false);
//         
//         consumer.setDead();
//
//         //** IMPORTANT NOTE! We must return the delivery NOT null. **
//         //This is because if we return NULL then message will remain in the queue, but later
//         //the connection checker will cleanup and close this consumer which will cancel all the deliveries in it
//         //including this one, so the message will go back on the queue twice!         
//      }
   }
   
   List<MessageReference> acknowledgeTransactionally(List<Ack> acks, Transaction tx) throws Exception
   {
      if (trace) { log.trace(this + " acknowledging transactionally " + acks.size() + " messages for " + tx); }
             
      List<MessageReference> refs = new ArrayList<MessageReference>();
      
      for(Ack ack: acks)
      {
         long id = ack.getDeliveryID();
         
         //TODO - do this more elegantly
         if (ack instanceof DeliveryInfo)
         {
         	if (!((DeliveryInfo)ack).isShouldAck())
         	{
         		//If we are in VM then acks for non durable subs will still exist - this
         		//won't happen remoptely since they are not written to the wire
         		continue;
         	}
         }
         
         DeliveryRecord rec = (DeliveryRecord)deliveries.get(id);
          
         DeliveryCallback cb = new DeliveryCallback(id);
         
         tx.addSynchronization(cb);
         
         refs.add(rec.ref);
         
         if (rec.ref.getMessage().isDurable())
         {
            tx.setContainsPersistent(true);
         }
      }  
      
      return refs;
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
         //Prompting delivery must be asynchronous to avoid deadlock
         //but we cannot use one way invocations on cancelDelivery and
         //cancelDeliveries because remoting one way invocations can 
         //overtake each other in flight - this problem will
         //go away when we have our own transport and our dedicated connection
         this.executor.execute(new Runnable() { public void run() { queue.deliver();} } );
         
      }
      catch (Throwable t)
      {
         log.error("Failed to prompt delivery", t);
      }
   }
   
   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private void cancelDeliveryInternal(Cancel cancel) throws Exception
   {
      DeliveryRecord rec = (DeliveryRecord)deliveries.remove(cancel.getDeliveryId());
      
      if (rec == null)
      {
         //The delivery might not be found, if the session is not replicated (i.e. auto_ack or dups_ok)
      	//and has failed over since recoverDeliveries won't have been called
      	if (trace)
      	{
      		log.trace("Cannot find delivery to cancel, session probably failed over and is not replicated");
      	}
      	return;
      }
                 
      MessageReference ref = rec.ref;      
      
      //Note we check the flag *and* evaluate again, this is because the server and client clocks may
      //be out of synch and don't want to send back to the client a message it thought it has sent to
      //the expiry queue  
      boolean expired = cancel.isExpired() || ref.getMessage().isExpired();
      
      //Note we check the flag *and* evaluate again, this is because the server value of maxDeliveries
      //might get changed after the client has sent the cancel - and we don't want to end up cancelling
      //back to the original queue
      boolean reachedMaxDeliveryAttempts =
         cancel.isReachedMaxDeliveryAttempts() || cancel.getDeliveryCount() >= rec.maxDeliveryAttempts;
                    
      if (!expired && !reachedMaxDeliveryAttempts)
      {
         //Normal cancel back to the queue
         
         ref.setDeliveryCount(cancel.getDeliveryCount());
         
         //Do we need to set a redelivery delay?
         
         if (rec.redeliveryDelay != 0)
         {
            ref.setScheduledDeliveryTime(System.currentTimeMillis() + rec.redeliveryDelay);
         }
         
         if (trace) { log.trace("Cancelling delivery " + cancel.getDeliveryId()); }
         
         ref.cancel(sp.getPersistenceManager());
         
      }
      else
      {                  
         if (expired)
         {
            //Sent to expiry queue
            
            Message copy = makeCopyForDLQOrExpiry(true, ref);
            
            moveInTransaction(copy, ref, rec.expiryQueue, false);
         }
         else
         {
            //Send to DLQ
         	
            Message copy = makeCopyForDLQOrExpiry(false, ref);
            
            moveInTransaction(copy, ref, rec.dlq, true);
         }
      }      
   }      
   
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
   
   private void moveInTransaction(Message msg, MessageReference ref, Queue queue, boolean dlq) throws Exception
   {
      List<Message> msgs = new ArrayList<Message>();
      
      msgs.add(msg);
      
      List<MessageReference> refs = new ArrayList<MessageReference>();
      
      refs.add(ref);
                  
      Transaction tx = new TransactionImpl(msgs, refs, msg.isDurable());
      
      //FIXME - clear up these ugly refs to the pm
      tx.commit(getConnectionEndpoint().getMessagingServer().getPersistenceManager());
      
//      MessageReference ref = msg.createReference();
//                    
//      try
//      {               
//         if (queue != null)
//         {          
//            queue.handle(null, ref, tx);
//            del.acknowledge(tx);
//         }
//         else
//         {
//            log.warn("No " + (dlq ? "DLQ" : "expiry queue") + " has been specified so the message will be removed");
//            
//            del.acknowledge(tx);
//         }             
//         
//         tx.commit();         
//      }
//      catch (Throwable t)
//      {
//         tx.rollback();
//         throw t;
//      }       
   }
   
   private boolean acknowledgeDeliveryInternal(Ack ack) throws Exception
   {
      if (trace) { log.trace(this + " acknowledging delivery " + ack); }

      DeliveryRecord rec = (DeliveryRecord)deliveries.remove(ack.getDeliveryID());
      
      if (rec != null)
      {
         rec.ref.acknowledge(this.sp.getPersistenceManager());
         
         if (trace) { log.trace(this + " acknowledged delivery " + ack); }
         
         return true;
      }
      
      return false;      
   }
      
   private Consumer createConsumerDelegateInternal(Destination destination,
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
         // Consumer on a jms queue
         
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
      connectionEndpoint.getMessagingServer().getMinaService().getDispatcher().register(ep.newHandler());
      
      ClientConsumerDelegate stub =
         new ClientConsumerDelegate(consumerID, prefetchSize, maxDeliveryAttemptsToUse, redeliveryDelayToUse);
      
      synchronized (consumers)
      {
         consumers.put(consumerID, ep);
      }
         
      log.trace(this + " created and registered " + ep);
      
      return stub;
   }   

   private ClientBrowser createBrowserDelegateInternal(Destination destination,
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

      connectionEndpoint.getMessagingServer().getMinaService().getDispatcher().register(ep.newHandler());
      
      ClientBrowserDelegate stub = new ClientBrowserDelegate(browserID);

      log.trace(this + " created and registered " + ep);

      return stub;
   }

   // Inner classes --------------------------------------------------------------------------------
   
   /*
    * Holds a record of a delivery - we need to store the consumer id as well
    * hence this class
    * We can't rely on the cancel being driven from the ClientConsumer since
    * the deliveries may have got lost in transit (ignored) since the consumer might have closed
    * when they were in transit.
    * In such a case we might otherwise end up with the consumer closing but not all it's deliveries being
    * cancelled, which would mean they wouldn't be cancelled until the session is closed which is too late
    * 
    * We need to store various pieces of information, such as consumer id, dlq, expiry queue
    * since we need this at cancel time, but by then the actual consumer might have closed
    */
   private static class DeliveryRecord
   {
   	// We need to cache the attributes here  since the consumer may get gc'd BEFORE the delivery is acked   	
   	
      MessageReference ref;
        
      Queue dlq;
      
      Queue expiryQueue;
      
      long redeliveryDelay;
      
      int maxDeliveryAttempts;
      
      WeakReference consumerRef;
      
      String queueName;
      
      boolean replicating;
      
      volatile boolean waitingForResponse;
      
      long deliveryID;
      
      ServerConsumerEndpoint getConsumer()
      {
      	if (consumerRef != null)
      	{
      		return (ServerConsumerEndpoint)consumerRef.get();
      	}
      	else
      	{
      		return null;
      	}
      }
            
      private DeliveryRecord(MessageReference ref, Queue dlq, Queue expiryQueue, long redeliveryDelay, int maxDeliveryAttempts,
      		         String queueName, long deliveryID)
      {
      	this.ref = ref;
      	
      	this.dlq = dlq;
      	
      	this.expiryQueue = expiryQueue;
      	
      	this.redeliveryDelay = redeliveryDelay;
      	
      	this.maxDeliveryAttempts = maxDeliveryAttempts;
      	
      	this.queueName = queueName;
      	
      	this.deliveryID = deliveryID;
      }
      
      DeliveryRecord(MessageReference ref, ServerConsumerEndpoint consumer, long deliveryID)
      {
      	this (ref, consumer.getDLQ(), consumer.getExpiryQueue(), consumer.getRedliveryDelay(), consumer.getMaxDeliveryAttempts(),
      			consumer.getQueueName(), deliveryID);

      	// We need to cache the attributes here  since the consumer may get gc'd BEFORE the delivery is acked
         
      	
         //We hold a WeakReference to the consumer - this is only needed when replicating - where we store the delivery then wait
         //for the response to come back from the replicant before actually performing delivery
         //We need a weak ref since when the consumer closes deliveries may still and remain and we don't want that to prevent
         //the consumer being gc'd
      	
      	//FIXME - do we still need this??
         this.consumerRef = new WeakReference(consumer);
      }            
      
   	public String toString()
   	{
   		return "DeliveryRecord " + System.identityHashCode(this) + " ref: " + ref + " queueName: " + queueName;
   	}
   }
   
   /**
    * 
    * The purpose of this class is to remove deliveries from the delivery list on commit
    * Each transaction has once instance of this per SCE
    *
    */
   private class DeliveryCallback implements TransactionSynchronization
   { 
      private long deliveryId;
      
      DeliveryCallback(long deliveryId)
      {
         this.deliveryId = deliveryId;
      }
      
      public void afterCommit() throws Exception
      {
         deliveries.remove(deliveryId);
      }

      public void afterRollback() throws Exception
      {
       //One phase rollbacks never hit the server - they are dealt with locally only
         //so this would only ever be executed for a two phase rollback.

         //We don't do anything since cancellation is driven from the client.
      }

      public void beforeCommit() throws Exception
      {
      }

      public void beforeRollback() throws Exception
      {       
      }   
   }

   public PacketHandler newHandler()
   {
      return new SessionAdvisedPacketHandler();
   }


   // INNER CLASSES

   private class SessionAdvisedPacketHandler implements PacketHandler
      {


      public SessionAdvisedPacketHandler()
      {
      }

      public String getID()
      {
         return ServerSessionEndpoint.this.id;
      }

      public void handle(AbstractPacket packet, PacketSender sender)
      {
         try
         {
            AbstractPacket response = null;

            PacketType type = packet.getType();
            if (type == MSG_SENDMESSAGE)
            {
               SendMessage message = (SendMessage) packet;

               long sequence = message.getSequence();
               send(message.getMessage(), sequence);

               // a response is required only if seq == -1 -> reliable message or strict TCK
               if (sequence == -1)
               {
                  response = new NullPacket();
               }

            } else if (type == REQ_CREATECONSUMER)
            {
               CreateConsumerRequest request = (CreateConsumerRequest) packet;
               ClientConsumerDelegate consumer = (ClientConsumerDelegate) createConsumerDelegate(
                     request.getDestination(), request.getSelector(), request
                           .isNoLocal(), request.getSubscriptionName(), request
                           .isConnectionConsumer());

               response = new CreateConsumerResponse(consumer.getID(), consumer
                     .getBufferSize(), consumer.getMaxDeliveries(), consumer
                     .getRedeliveryDelay());
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
               ClientBrowserDelegate browser = (ClientBrowserDelegate) createBrowserDelegate(
                     request.getDestination(), request.getSelector());

               response = new CreateBrowserResponse(browser.getID());
            } else if (type == REQ_ACKDELIVERY)
            {
               AcknowledgeDeliveryRequest request = (AcknowledgeDeliveryRequest) packet;
               boolean acknowledged = acknowledgeDelivery(new DefaultAck(
                     request.getDeliveryID()));

               response = new AcknowledgeDeliveryResponse(acknowledged);
            } else if (type == MSG_ACKDELIVERIES)
            {
               AcknowledgeDeliveriesMessage message = (AcknowledgeDeliveriesMessage) packet;
               acknowledgeDeliveries(message.getAcks());

               response = new NullPacket();
            } else if (type == MSG_CANCELDELIVERY)
            {
               CancelDeliveryMessage message = (CancelDeliveryMessage) packet;
               cancelDelivery(message.getCancel());

               response = new NullPacket();
            } else if (type == MSG_CANCELDELIVERIES)
            {
               CancelDeliveriesMessage message = (CancelDeliveriesMessage) packet;
               cancelDeliveries(message.getCancels());

               response = new NullPacket();
            } else if (type == REQ_CLOSING)
            {
               ClosingRequest request = (ClosingRequest) packet;
               long id = closing(request.getSequence());

               response = new ClosingResponse(id);
            } else if (type == MSG_CLOSE)
            {
               close();

               response = new NullPacket();
            } else if (type == MSG_UNSUBSCRIBE)
            {
               UnsubscribeMessage message = (UnsubscribeMessage) packet;
               unsubscribe(message.getSubscriptionName());

               response = new NullPacket();
            } else if (type == MSG_ADDTEMPORARYDESTINATION)
            {
               AddTemporaryDestinationMessage message = (AddTemporaryDestinationMessage) packet;
               addTemporaryDestination(message.getDestination());

               response = new NullPacket();
            } else if (type == MSG_DELETETEMPORARYDESTINATION)
            {
               DeleteTemporaryDestinationMessage message = (DeleteTemporaryDestinationMessage) packet;
               deleteTemporaryDestination(message.getDestination());

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
         return "SessionAdvisedPacketHandler[id=" + id + "]";
      }
   }

}
