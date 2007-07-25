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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.jboss.aop.AspectManager;
import org.jboss.jms.client.delegate.ClientBrowserDelegate;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.delegate.Ack;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.Cancel;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.DeliveryInfo;
import org.jboss.jms.delegate.DeliveryRecovery;
import org.jboss.jms.delegate.SessionEndpoint;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.JMSCondition;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.destination.ManagedDestination;
import org.jboss.jms.server.destination.ManagedQueue;
import org.jboss.jms.server.destination.ManagedTopic;
import org.jboss.jms.server.destination.TopicService;
import org.jboss.jms.server.endpoint.advised.BrowserAdvised;
import org.jboss.jms.server.endpoint.advised.ConsumerAdvised;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.selector.Selector;
import org.jboss.jms.wireformat.ClientDelivery;
import org.jboss.jms.wireformat.Dispatcher;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.Channel;
import org.jboss.messaging.core.contract.Condition;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.DeliveryObserver;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.contract.Replicator;
import org.jboss.messaging.core.impl.IDManager;
import org.jboss.messaging.core.impl.MessagingQueue;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.messaging.core.impl.tx.TransactionException;
import org.jboss.messaging.core.impl.tx.TransactionRepository;
import org.jboss.messaging.core.impl.tx.TxCallback;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.GUIDGenerator;
import org.jboss.messaging.util.MessageQueueNameHelper;
import org.jboss.remoting.Client;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedLong;

/**
 * The server side representation of a JMS session.
 * 
 * A user must not invoke methods of a session concurrently on different threads, however there are
 * situations where multiple threads may access this object concurrently, for instance:
 * - A session can be closed when it's connection is closed by the user which might be called on a
 *   different thread.
 * - A session can be closed when the server determines the connection is dead.
 *
 * If the session represents a connection consumer's session then the connection consumer will farm
 * off messages to different sessions obtained from a pool, these may then cancel/ack etc on
 * different threads, but the acks/cancels/etc will end up back here on the connection consumer
 * session instance.
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
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
      
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private String id;

   private volatile boolean closed;

   private ServerConnectionEndpoint connectionEndpoint;
   private ServerInvokerCallbackHandler callbackHandler;
   
   private ServerPeer sp;

   private Map consumers;
   private Map browsers;

   private PersistenceManager pm;
   private MessageStore ms;

   private DestinationManager dm;
   private IDManager idm;
   private TransactionRepository tr;
   private PostOffice postOffice;
   private int nodeId;
   private int defaultMaxDeliveryAttempts;
   private long defaultRedeliveryDelay;
   private Queue defaultDLQ;
   private Queue defaultExpiryQueue;
   private boolean supportsFailover;
   
   private Object deliveryLock = new Object();
      
   // Map <deliveryID, Delivery>
   private Map deliveries;
   
   private SynchronizedLong deliveryIdSequence;
   
   //Temporary until we have our own NIO transport   
   QueuedExecutor executor = new QueuedExecutor(new LinkedQueue());
   
   private LinkedQueue toDeliver = new LinkedQueue();
   
   private boolean waitingToClose = false;
   
   // Constructors ---------------------------------------------------------------------------------

   ServerSessionEndpoint(String sessionID, ServerConnectionEndpoint connectionEndpoint) throws Exception
   {
      this.id = sessionID;

      this.connectionEndpoint = connectionEndpoint;
      
      callbackHandler = connectionEndpoint.getCallbackHandler();
      
      sp = connectionEndpoint.getServerPeer();

      pm = sp.getPersistenceManagerInstance();
      
      ms = sp.getMessageStore();
      
      dm = sp.getDestinationManager();
      
      postOffice = sp.getPostOfficeInstance(); 
      
      supportsFailover = connectionEndpoint.getConnectionFactoryEndpoint().isSupportsFailover() && postOffice.isClustered();
      
      idm = sp.getChannelIDManager();
      
      nodeId = sp.getServerPeerID();
      
      tr = sp.getTxRepository();
      
      consumers = new HashMap();
      
		browsers = new HashMap();
      
      defaultDLQ = sp.getDefaultDLQInstance();
      
      defaultExpiryQueue = sp.getDefaultExpiryQueueInstance();
      
      tr = sp.getTxRepository();
      
      defaultMaxDeliveryAttempts = sp.getDefaultMaxDeliveryAttempts();
      
      defaultRedeliveryDelay = sp.getDefaultRedeliveryDelay();
      
      deliveries = new ConcurrentHashMap();
      
      deliveryIdSequence = new SynchronizedLong(0);
   }
   
   // SessionDelegate implementation ---------------------------------------------------------------
       
   public ConsumerDelegate createConsumerDelegate(JBossDestination jmsDestination,
                                                  String selector,
                                                  boolean noLocal,
                                                  String subscriptionName,
                                                  boolean isCC,
                                                  boolean autoFlowControl) throws JMSException
   {
      try
      {
      	//TODO This is a temporary kludge to allow creation of consumers directly on core queues for 
      	//cluster connections
      	//This will disappear once we move all JMS knowledge to the client side
      	
      	if (jmsDestination.isDirect())
      	{
      		return createConsumerDelegateDirect(jmsDestination.getName(), selector);
      	}
      	else
      	{      	      	
	         return createConsumerDelegateInternal(jmsDestination, selector,
	                                               noLocal, subscriptionName);
      	}
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " createConsumerDelegate");
      }
   }
      
	public BrowserDelegate createBrowserDelegate(JBossDestination jmsDestination,
                                                String selector)
      throws JMSException
	{
      try
      {
         return createBrowserDelegateInternal(jmsDestination, selector);
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
         
         ManagedDestination dest = (ManagedDestination)dm.getDestination(name, true);
         
         if (dest == null)
         {
            throw new JMSException("There is no administratively defined queue with name:" + name);
         }        
   
         return new JBossQueue(dest.getName());
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
         
         ManagedDestination dest = (ManagedDestination)dm.getDestination(name, false);
                  
         if (dest == null)
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
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " close");
      }
   }
      
   public long closing() throws JMSException
   {
      // currently does nothing
      if (trace) log.trace(this + " closing (noop)");
      
      return -1;
   }
 
   public void send(JBossMessage message, boolean checkForDuplicates) throws JMSException
   {
      try
      {                
         connectionEndpoint.sendMessage(message, null, checkForDuplicates);         
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " send");
      }
   }
   
   public void acknowledgeDelivery(Ack ack) throws JMSException
   {
      try
      {
         acknowledgeDeliveryInternal(ack);   
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " acknowledgeDelivery");
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
      if (trace) {log.trace(this + " cancelDelivery " + cancel); }
      
      try
      {
         Delivery del = cancelDeliveryInternal(cancel);
         
         //Prompt delivery
         promptDelivery((Channel)del.getObserver());
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " cancelDelivery");
      }     
   }            

   public void cancelDeliveries(List cancels) throws JMSException
   {
      if (trace) {log.trace(this + " cancels deliveries " + cancels); }
        
      try
      {
         // deliveries must be cancelled in reverse order

         Set channels = new HashSet();
                          
         for (int i = cancels.size() - 1; i >= 0; i--)
         {
            Cancel cancel = (Cancel)cancels.get(i);       
            
            if (trace) { log.trace(this + " cancelling delivery " + cancel.getDeliveryId()); }
                        
            Delivery del = cancelDeliveryInternal(cancel);
            
            channels.add(del.getObserver());
         }
                 
         if (trace) { log.trace("Cancelled deliveries"); }
         
         // need to prompt delivery for all affected channels
         
         promptDelivery(channels);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " cancelDeliveries");
      }
   }         
   
   public void recoverDeliveries(List deliveryRecoveryInfos, String oldSessionID) throws JMSException
   {
      if (trace) { log.trace(this + "recovers deliveries " + deliveryRecoveryInfos); }

      try
      {
         if (!postOffice.isClustered())
         {
            throw new IllegalStateException("Recovering deliveries but post office is not clustered!");
         }
         
         long maxDeliveryId = 0;
                  
         //Sort into different list for each channel
         Map ackMap = new HashMap();
                  
         for (Iterator iter = deliveryRecoveryInfos.iterator(); iter.hasNext(); )
         {
            DeliveryRecovery deliveryInfo = (DeliveryRecovery)iter.next();
                
            String queueName = deliveryInfo.getQueueName();

            List acks = (List)ackMap.get(queueName);
            
            if (acks == null)
            {
               acks = new ArrayList();
               
               ackMap.put(queueName, acks);
            }
            
            acks.add(deliveryInfo);
         }  

         Iterator iter = ackMap.entrySet().iterator();
         
         while (iter.hasNext())
         {
            Map.Entry entry = (Map.Entry)iter.next();
            
            String queueName = (String)entry.getKey();
            
            //Look up the queue

            Binding binding = postOffice.getBindingForQueueName(queueName);
            
            Queue queue = binding.queue;
            
            if (queue == null)
            {
               throw new IllegalStateException("Cannot find queue with queue name: " + queueName);
            }
            
            List acks = (List)entry.getValue();
            
            List ids = new ArrayList(acks.size());
            
            for (Iterator iter2 = acks.iterator(); iter2.hasNext(); )
            {
               DeliveryRecovery info = (DeliveryRecovery)iter2.next();
               
               ids.add(new Long(info.getMessageID()));
            }
            
            JMSCondition cond = (JMSCondition)binding.condition;
            
            ManagedDestination dest =
               sp.getDestinationManager().getDestination(cond.getName(), cond.isQueue());
            
            if (dest == null)
            {
               throw new IllegalStateException("Cannot find managed destination with name " +
                  cond.getName() + " isQueue" + cond.isQueue());
            }
            
            Queue dlqToUse =
               dest.getDLQ() == null ? defaultDLQ : dest.getDLQ();
            
            Queue expiryQueueToUse =
               dest.getExpiryQueue() == null ? defaultExpiryQueue : dest.getExpiryQueue();
            
            int maxDeliveryAttemptsToUse =
               dest.getMaxDeliveryAttempts() == -1 ? defaultMaxDeliveryAttempts : dest.getMaxDeliveryAttempts();

            List dels = queue.recoverDeliveries(ids);

            Iterator iter2 = dels.iterator();
            
            Iterator iter3 = acks.iterator();
            
            while (iter2.hasNext())
            {
               Delivery del = (Delivery)iter2.next();
               
               DeliveryRecovery info = (DeliveryRecovery)iter3.next();
               
               long deliveryId = info.getDeliveryID();
               
               maxDeliveryId = Math.max(maxDeliveryId, deliveryId);
               
               if (trace) { log.trace(this + " Recovered delivery " + deliveryId + ", " + del); }
               
               deliveries.put(new Long(deliveryId),
                              new DeliveryRecord(del, dlqToUse, expiryQueueToUse, dest.getRedeliveryDelay(),
                              		maxDeliveryAttemptsToUse, queueName, supportsFailover, deliveryId));
               
               //We want to replicate the deliveries to the new backup, but we don't want a response since that would cause actual delivery
               //to occur, which we don't want since the client already has the deliveries
               
               if (supportsFailover)
               {
               	postOffice.sendReplicateDeliveryMessage(queueName, id, del.getReference().getMessage().getMessageID(), deliveryId, false, true);
               }
            }
         }
         
         iter = postOffice.getAllBindings().iterator();
         
         while (iter.hasNext())
         {
         	Binding binding = (Binding)iter.next();
         	
         	if (binding.queue.isClustered() && binding.queue.isRecoverable())
         	{
         		// Remove any stranded refs corresponding to refs that might have been in the client buffer but not consumed
         		binding.queue.removeStrandedReferences(oldSessionID);
         	}
         }
         
         this.deliveryIdSequence = new SynchronizedLong(maxDeliveryId + 1);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " recoverDeliveries");
      }
   }
   
   public void addTemporaryDestination(JBossDestination dest) throws JMSException
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

         connectionEndpoint.addTemporaryDestination(dest);
         
         // Register with the destination manager
         
         ManagedDestination mDest;
         
         int fullSize = connectionEndpoint.getDefaultTempQueueFullSize();
         int pageSize = connectionEndpoint.getDefaultTempQueuePageSize();
         int downCacheSize = connectionEndpoint.getDefaultTempQueueDownCacheSize();
         
         //Temporary destinations are clustered if the post office is clustered

         if (dest.isTopic())
         {
            mDest = new ManagedTopic(dest.getName(), fullSize, pageSize, downCacheSize, postOffice.isClustered());
         }
         else
         {
            mDest = new ManagedQueue(dest.getName(), fullSize, pageSize, downCacheSize, postOffice.isClustered());
         }
         
         mDest.setTemporary(true);
         
         dm.registerDestination(mDest);
         
         if (dest.isQueue())
         {            
            Queue coreQueue = new MessagingQueue(nodeId, dest.getName(),
            												 idm.getID(), ms, pm, false, -1, null,
										                   fullSize, pageSize, downCacheSize, postOffice.isClustered(),
										                   sp.getRecoverDeliveriesTimeout());

        
            Condition cond = new JMSCondition(true, dest.getName());
            
         	// make a binding for this temporary queue
            
            // temporary queues need to bound on ALL nodes of the cluster
            postOffice.addBinding(new Binding(cond, coreQueue, true), postOffice.isClustered());   
            
            coreQueue.activate();
         }         
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " addTemporaryDestination");
      }
   }
   
   public void deleteTemporaryDestination(JBossDestination dest) throws JMSException
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
         
         ManagedDestination mDest = dm.getDestination(dest.getName(), dest.isQueue());
         
         if (mDest == null)
         {
            throw new InvalidDestinationException("No such destination: " + dest);
         }
                  
         if (dest.isQueue())
         {
         	Binding binding = postOffice.getBindingForQueueName(dest.getName());
         	
         	if (binding == null)
         	{
         		throw new IllegalStateException("Cannot find binding for queue " + dest.getName());
         	}
         	
         	if (binding.queue.getLocalDistributor().getNumberOfReceivers() != 0)
         	{
         		throw new IllegalStateException("Cannot delete temporary queue if it has consumer(s)");
         	}
         	
         	// temporary queues must be unbound on ALL nodes of the cluster
         	
            postOffice.removeBinding(dest.getName(), postOffice.isClustered());         	        
         }
         else
         {
            //Topic            
            Collection queues = postOffice.getQueuesForCondition(new JMSCondition(false, dest.getName()), true);         	
                           
            if (!queues.isEmpty())
         	{
            	throw new IllegalStateException("Cannot delete temporary topic if it has consumer(s)");
         	}
                        
            // There is no need to explicitly unbind the subscriptions for the temp topic, this is because we
            // will not get here unless there are no bindings.
            // Note that you cannot create surable subs on a temp topic
         }
         
         connectionEndpoint.removeTemporaryDestination(dest);         
         
         dm.unregisterDestination(mDest);                             
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
         
         String queueName = MessageQueueNameHelper.createSubscriptionName(clientID, subscriptionName);
         
         Binding binding = postOffice.getBindingForQueueName(queueName);
         
         if (binding == null)
         {
            throw new InvalidDestinationException("Cannot find durable subscription with name " +
                                                  subscriptionName + " to unsubscribe");
         }
         
         Queue sub = binding.queue;         
         
         // Section 6.11. JMS 1.1.
         // "It is erroneous for a client to delete a durable subscription while it has an active
         // TopicSubscriber for it or while a message received by it is part of a current
         // transaction or has not been acknowledged in the session."
         
         if (sub.getLocalDistributor().getNumberOfReceivers() != 0)
         {
            throw new IllegalStateException("Cannot unsubscribe durable subscription " +
                                            subscriptionName + " since it has active subscribers");
         }
         
         //Also if it is clustered we must disallow unsubscribing if it has active consumers on other nodes
         
         if (sub.isClustered() && postOffice.isClustered())
         {
         	Replicator rep = (Replicator)postOffice;
         	
         	Map map = rep.get(sub.getName());
         	
         	if (!map.isEmpty())
         	{
         		throw new IllegalStateException("Cannot unsubscribe durable subscription " +
                     subscriptionName + " since it has active subscribers on other nodes");
         	}
         }
         
         postOffice.removeBinding(sub.getName(), sub.isClustered() && postOffice.isClustered());         
         
         String counterName = TopicService.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + sub.getName();
         
         MessageCounter counter = sp.getMessageCounterManager().unregisterMessageCounter(counterName);
         
         if (counter == null)
         {
            throw new IllegalStateException("Cannot find counter to remove " + counterName);
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " unsubscribe");
      }
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
   
   public void deliverAnyWaitingDeliveries(String queueName) throws Exception
   {
   	//	First deliver any waiting deliveries
   	
   	if (trace) { log.trace("Delivering any waiting deliveries: " + queueName); }
   	
   	List toAddBack = null;
   	
   	while (true)
   	{
   		DeliveryRecord dr = (DeliveryRecord)toDeliver.poll(0);
   		
   		if (dr == null)
   		{
   			break;
   		}
   		
   		if (trace) { log.trace("Considering " + dr); } 
   		
   		if (queueName == null || dr.queueName.equals(queueName))
   		{   		
   			//Need to synchronized to prevent the delivery being performed twice
   			synchronized (dr)
   			{   				
		   		performDelivery(dr.del.getReference(), dr.deliveryID, dr.getConsumer()); 
					
			   	dr.waitingForResponse = false;
   			}
   		}
   		else
   		{
   			if (toAddBack == null)
   			{
   				toAddBack = new ArrayList();
   			}
   			
   			toAddBack.add(dr);
   		}
   	}
   	
   	if (toAddBack != null)
   	{
   		Iterator iter = toAddBack.iterator();
   		
   		while (iter.hasNext())
   		{
   			toDeliver.put(iter.next());
   		}
   	}
   	
   	if (trace) { log.trace("Done delivering"); }
   }
   
   public boolean collectDeliveries(Map map, boolean firstNode, String queueName) throws Exception
   {
   	if (trace) { log.trace("Collecting deliveries"); }
   	
   	boolean gotSome = false;
   	   	
   	if (!firstNode)
   	{	   	
	   	if (trace) { log.trace("Now collecting"); }
	   	   	
	   	Iterator iter = deliveries.entrySet().iterator();
	   	
	   	while (iter.hasNext())
	   	{
	   		Map.Entry entry = (Map.Entry)iter.next();
	   		
	   		Long l = (Long)entry.getKey();
	   		
	   		long deliveryID = l.longValue();
	   		
	   		DeliveryRecord rec = (DeliveryRecord)entry.getValue();
	   		
	   		if (rec.replicating && (queueName == null || rec.queueName.equals(queueName)))
	   		{
	   			Map ids = (Map)map.get(rec.queueName);
	   			
	   			if (ids == null)
	   			{
	   				ids = new HashMap();
	   				
	   				map.put(rec.queueName, ids);
	   			}
	   			
	   			ids.put(new Long(rec.del.getReference().getMessage().getMessageID()), id);
	   			
	   			gotSome = true;
	   			
	   			boolean notify = false;
	   			
	   			//Need to synchronize to prevent delivery occurring more than once - e.g.
	   			//if replicateDeliveryResponseReceived occurs curently with this
	   			synchronized (rec)
	   			{
		   			if (rec.waitingForResponse)
		   			{
		   				//Do the delivery now
		   				
		   				performDelivery(rec.del.getReference(), deliveryID, rec.getConsumer());   	   	
		   		   	
		   		   	rec.waitingForResponse = false;
		   		   	
		   		   	notify = true;
		   			}
	   			}
	   		   	
	   			if (notify)
	   			{
	   		   	synchronized (deliveryLock)
	   		   	{
	   		   		if (waitingToClose)
	   		   		{
	   		   			deliveryLock.notifyAll();
	   		   		}
	   		   	}   					   			
	   			}
	   		}
	   	}
   	}
   	
   	if (trace) { log.trace("Collected " + map.size() + " deliveries"); }
   	
   	return gotSome;
   }
      
   private Object myLock = new Object();
   
   public void replicateDeliveryResponseReceived(long deliveryID) throws Exception
   {
   	//We look up the delivery in the list and actually perform the delivery
   	
   	if (trace) { log.trace(this + " replicate delivery response received for delivery " + deliveryID); }
   	
   	DeliveryRecord rec = (DeliveryRecord)deliveries.get(new Long(deliveryID));
   	
   	if (rec == null)
   	{
   		throw new java.lang.IllegalStateException("Cannot find delivery with id " + deliveryID);
   	}
   	   	
   	boolean delivered = false;
   	
   	//I have commented this out since we should be able guarantee responses come back in order if we use
   	//a QueuedExecutor on the other node to send the response
   	
//   	//Note there will only be contention on this if two or more responses come back at the same time - which is unlikely
//   	//TODO - This can occur since replicates are sent to the other node, and the responses are sent back using a pool which
//   	//means earlier responses can be received after later ones -hence we need to cope with this
//   	//However - if we used a queued executor on the other node to send back responses we could remove all this locking!!
//   	synchronized (myLock)
//   	{
//   		long toWait = DELIVERY_WAIT_TIMEOUT;
//   		
//   		while (toWait > 0)
//      	{
//      		DeliveryRecord dr = (DeliveryRecord)toDeliver.peek();
//      		      	      		
//      		if (dr == null)
//      		{
//      			if (trace) { log.trace("No more deliveries in list"); }
//      			
//      			break;
//      		}
//      		
//      		if (trace) { log.trace("Peeked delivery record: " + dr.deliveryID); }
//      		
//      		boolean wait = false;
//      		
//      		//Needs to be synchronized to prevent delivery occurring twice e.g. if this occurs at same time as collectDeliveries
//      		synchronized (dr)
//      		{	   		
//   	   		boolean performDelivery = false;
//   	   		
//   	   		if (dr.waitingForResponse)
//   	   		{
//   	   			if (dr == rec)
//   	   			{
//   	   				if (trace) { log.trace("Found our delivery"); }
//   	   				
//   	   				performDelivery = true;
//   	   			}
//   	   			else
//   	   			{
//   	   				if (!delivered)
//   	   				{
//	   	   				//We have to wait for another response to arrive first
//	   	   				
//	   	   				if (trace) { log.trace("Not ours - need to wait"); }
//	   	   				
//	   	   				wait = true;
//   	   				}
//   	   				else
//   	   				{
//   	   					//We have delivered ours and possibly any non replicated deliveries too   	   					   	   					
//   	   	   	   	
//   	   	   	   	myLock.notify();
//   	   	   	   	
//   	   					break;
//   	   				}
//   	   			}
//   	   		}
//   	   		else
//   	   		{
//   	   			//Non replicated delivery
//   	   			
//   	   			if (trace) { log.trace("Non replicated delivery"); }
//   	   			
//   	   			performDelivery = true;
//   	   		}
//   	   		
//   	   		if (performDelivery)
//   	   		{
//   	   			toDeliver.take();
//   	   			
//   	   			performDelivery(dr.del.getReference(), dr.deliveryID, dr.getConsumer()); 
//   	   			
//   	   			delivered = true;
//   	   	   	
//   	   	   	dr.waitingForResponse = false;
//   	   	   	
//   	   	   	delivered = true;
//   	   		}
//      		}
//      		
//      		if (wait)
//      		{
//   				long start = System.currentTimeMillis();
//   				
//      			try
//      			{
//      				if (trace) { log.trace("Waiting"); }
//      				
//      				//We need to wait since responses have come back out of order
//      				myLock.wait(toWait);
//      				
//      				if (trace) { log.trace("Woke up"); }
//      			}
//      			catch (InterruptedException e)
//      			{      				
//      			}
//      			toWait -= (System.currentTimeMillis() - start);
//      		}      		
//      	}
//   		if (toWait <= 0)
//   		{
//   			throw new IllegalStateException("Timed out waiting for previous response to arrive");
//   		}
//   	}   	   	
   	
		while (true)
   	{
   		DeliveryRecord dr = (DeliveryRecord)toDeliver.peek();
   		      	      		
   		if (dr == null)
   		{
   			if (trace) { log.trace("No more deliveries in list"); }
   			
   			break;
   		}
   		
   		if (trace) { log.trace("Peeked delivery record: " + dr.deliveryID); }
   		
   		//Needs to be synchronized to prevent delivery occurring twice e.g. if this occurs at same time as collectDeliveries
   		synchronized (dr)
   		{	   		
	   		boolean performDelivery = false;
	   		
	   		if (dr.waitingForResponse)
	   		{
	   			if (dr == rec)
	   			{
	   				if (trace) { log.trace("Found our delivery"); }
	   				
	   				performDelivery = true;
	   			}
	   			else
	   			{
	   				if (!delivered)
	   				{
   	   				//We have to wait for another response to arrive first
   	   				
   	   				throw new IllegalStateException("Reponses have come back our of order");
	   				}
	   				else
	   				{
	   					//We have delivered ours and possibly any non replicated deliveries too   	   					   	   					
	   	   	   	
	   					break;
	   				}
	   			}
	   		}
	   		else
	   		{
	   			//Non replicated delivery
	   			
	   			if (trace) { log.trace("Non replicated delivery"); }
	   			
	   			performDelivery = true;
	   		}
	   		
	   		if (performDelivery)
	   		{
	   			toDeliver.take();
	   			
	   			performDelivery(dr.del.getReference(), dr.deliveryID, dr.getConsumer()); 
	   			
	   			delivered = true;
	   	   	
	   	   	dr.waitingForResponse = false;
	   	   	
	   	   	delivered = true;
	   		}
   		}	
   	}   	  	      	
   	
   	if (delivered)
   	{
	   	synchronized (deliveryLock)
	   	{
	   		if (waitingToClose)
	   		{
	   			deliveryLock.notifyAll();
	   		}
	   	}
   	}   	   	   	   
   }

   // Package protected ----------------------------------------------------------------------------
   
   void expireDelivery(Delivery del, Queue expiryQueue) throws Throwable
   {
      if (trace) { log.trace(this + " detected expired message " + del.getReference()); }
      
      if (expiryQueue != null)
      {
         if (trace) { log.trace(this + " sending expired message to expiry queue " + expiryQueue); }
         
         JBossMessage copy = makeCopyForDLQOrExpiry(true, del);
         
         moveInTransaction(copy, del, expiryQueue);
      }
      else
      {
         log.warn("No expiry queue has been configured so removing expired " +  del.getReference());
         
         del.acknowledge(null);
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
    
   void localClose() throws Throwable
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
            
      Set channels = new HashSet();
      
      if (trace) { log.trace(this + " cancelling " + entries.size() + " deliveries"); }
      
      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry)iter.next();
         
         if (trace) { log.trace(this + " cancelling delivery with delivery id: " + entry.getKey()); }
         
         DeliveryRecord rec = (DeliveryRecord)entry.getValue();
         
         rec.del.cancel();
         
         channels.add(rec.del.getObserver());
      }
      
      promptDelivery(channels);
      
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
            
      Dispatcher.instance.unregisterTarget(id, this);
      
      closed = true;
   }            
   
   void cancelDelivery(long deliveryId) throws Throwable
   {
      DeliveryRecord rec = (DeliveryRecord)deliveries.remove(new Long(deliveryId));
      
      if (rec == null)
      {
         throw new IllegalStateException("Cannot find delivery to cancel " + deliveryId);
      }
      
      rec.del.cancel();
   }      
   
   /*
    * When a consumer closes there may be deliveries where the replication messages has gone out to the backup
    * but the response hasn't been received yet so the messages hasn't actually been delivered.
    * When closing we must wait for these to be delivered before closing, or the message will be "lost" until
    * the session is closed.
    */
   void waitForDeliveriesFromConsumer(String consumerID) throws Exception
   {   
		long toWait = DELIVERY_WAIT_TIMEOUT;
		
		boolean wait;
		
   	synchronized (deliveryLock)
   	{   		
   		do
   		{
   			wait = false;
   			
   			long start = System.currentTimeMillis();
   			
	   		Iterator iter = deliveries.values().iterator();
	   		
	   		while (iter.hasNext())
	   		{
	   			DeliveryRecord rec = (DeliveryRecord)iter.next();
	   			
	   			ServerConsumerEndpoint consumer = rec.getConsumer();
	   			
	   			if (consumer != null && consumer.getID().equals(consumerID) && rec.waitingForResponse)
	   			{
	   				wait = true;
	   				
	   				break;
	   			}
	   		}
	   		
	   		if (wait)
	   		{
	   			waitingToClose = true;
	   			try
	   			{
	   				deliveryLock.wait(toWait);
	   			}
	   			catch (InterruptedException e)
	   			{
	   				//Ignore
	   			}
	   			toWait -= (System.currentTimeMillis() - start);
	   		}
   		}
   		while (wait && toWait > 0);
   		
   		if (toWait <= 0)
   		{
   			//Clear toDeliver
   			while (toDeliver.poll(0) != null) {}
   			
   			log.warn("Timed out waiting for response to arrive");
   		}   		
   		waitingToClose = false;
   	}
   }
   
   void handleDelivery(Delivery delivery, ServerConsumerEndpoint consumer) throws Exception
   {
   	 long deliveryId = -1;
   	 
   	 if (trace) { log.trace(this + " handling delivery " + delivery); }
   	 
   	 DeliveryRecord rec = null;
   	 
   	 deliveryId = deliveryIdSequence.increment();   	 
   	 
   	 if (trace) { log.trace("Delivery id is now " + deliveryId); }
   	 
   	 //TODO can't we combine flags isRetainDeliveries and isReplicating - surely they're mutually exclusive?
       if (consumer.isRetainDeliveries())
       {      	 
      	 // Add a delivery

      	 rec = new DeliveryRecord(delivery, consumer, deliveryId);
          
          deliveries.put(new Long(deliveryId), rec);
          
          if (trace) { log.trace(this + " added delivery " + deliveryId + ": " + delivery); }
       }
       else
       {
       	//Acknowledge it now
       	try
       	{
       		//This basically just releases the memory reference
       		
       		if (trace) { log.trace("Acknowledging delivery now"); }
       		
       		delivery.acknowledge(null);
       	}
       	catch (Throwable t)
       	{
       		log.error("Failed to acknowledge delivery", t);
       	}
       }
       
       Message message = delivery.getReference().getMessage();
       
       if (!consumer.isReplicating())
       {
      	 if (trace) { log.trace(this + " doing the delivery straight away"); }
      	 
      	 //Actually do the delivery now
      	 performDelivery(delivery.getReference(), deliveryId, consumer); 	           
       }
       else if (!message.isReliable())
       {
      	 if (!toDeliver.isEmpty())
      	 {
      		 if (trace) { log.trace("Message is unreliable and there are refs in the toDeliver so adding to list"); }
      		 
      		 //We need to add to the list to prevent non persistent messages overtaking persistent messages from the same
      		 //producer in flight (since np don't need to be replicated)
      		 toDeliver.put(rec);
      		 
      		 //Race check (there's a small chance the message in the queue got removed between the empty check
      		 //and the put so we do another check:
      		 if (toDeliver.peek() == rec)
      		 {
      			 toDeliver.take();
      			 
      			 performDelivery(delivery.getReference(), deliveryId, consumer);
      		 }
      	 }
      	 else
      	 {
      		 if (trace) { log.trace("Message is unreliable, but no deliveries in list so performing delivery now"); }
      		 
      		 // Actually do the delivery now
         	 performDelivery(delivery.getReference(), deliveryId, consumer); 
      	 }
       }
       else
       {
      	 if (!postOffice.isFirstNode())
      	 {
         	 //We wait for the replication response to come back before actually performing delivery
         	 
         	 if (trace) { log.trace(this + " deferring delivery until we know it's been replicated"); }
         	 
         	 rec.waitingForResponse = true;      	 
         	 
         	 toDeliver.put(rec);
         	 
         	 postOffice.sendReplicateDeliveryMessage(consumer.getQueueName(), id,
                                                     delivery.getReference().getMessage().getMessageID(),
                                                     deliveryId, true, false);
      	 }
      	 else
      	 {
      		 //Only node in the cluster so deliver now
      	 
      		 rec.waitingForResponse = false;
      		 
      		 if (trace) { log.trace("First node so actually doing delivery now"); }
      		 
      		 // Actually do the delivery now - we are only node in the cluster
         	 performDelivery(delivery.getReference(), deliveryId, consumer); 	  
      	 }
       }

   }
   
   void performDelivery(MessageReference ref, long deliveryID, ServerConsumerEndpoint consumer)
   {
   	if (consumer == null)
   	{
   		if (trace) { log.trace(this + " consumer is null, cannot perform delivery"); }
   		
   		return;
   	}
   	
   	if (trace) { log.trace(this + " performing delivery for " + ref); }
   	   	
      // We send the message to the client on the current thread. The message is written onto the
      // transport and then the thread returns immediately without waiting for a response.

      Client callbackClient = callbackHandler.getCallbackClient();

      ClientDelivery del = new ClientDelivery(ref.getMessage(), consumer.getID(), deliveryID, ref.getDeliveryCount());

      Callback callback = new Callback(del);

      try
      {
         // FIXME - due a design (flaw??) in the socket based transports, they use a pool of TCP
         // connections, so subsequent invocations can end up using different underlying
         // connections meaning that later invocations can overtake earlier invocations, if there
         // are more than one user concurrently invoking on the same transport. We need someway
         // of pinning the client object to the underlying invocation. For now we just serialize
         // all access so that only the first connection in the pool is ever used - bit this is
         // far from ideal!!!
         // See http://jira.jboss.com/jira/browse/JBMESSAGING-789

         Object invoker = null;

         if (callbackClient != null)
         {
            invoker = callbackClient.getInvoker();                              
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
            if (trace) { log.trace(this + " submitting message " + ref.getMessage() + " to the remoting layer to be sent asynchronously"); }
            
            callbackHandler.handleCallbackOneway(callback);
                                    
            //We store the delivery id so we know to wait for any deliveries in transit on close
            consumer.setLastDeliveryID(deliveryID);
         }
      }
      catch (Throwable t)
      {
         // it's an oneway callback, so exception could only have happened on the server, while
         // trying to send the callback. This is a good reason to smack the whole connection.
         // I trust remoting to have already done its own cleanup via a CallbackErrorHandler,
         // I need to do my own cleanup at ConnectionManager level.

         log.trace(this + " failed to handle callback", t);
         
         //We stop the consumer - some time later the lease will expire and the connection will be closed        
         //which will remove the consumer
         
         consumer.setStarted(false);

         //** IMPORTANT NOTE! We must return the delivery NOT null. **
         //This is because if we return NULL then message will remain in the queue, but later
         //the connection checker will cleanup and close this consumer which will cancel all the deliveries in it
         //including this one, so the message will go back on the queue twice!
      }
   }
   
   void acknowledgeTransactionally(List acks, Transaction tx) throws Throwable
   {
      if (trace) { log.trace(this + " acknowledging transactionally " + acks.size() + " messages for " + tx); }
      
      DeliveryCallback deliveryCallback = (DeliveryCallback)tx.getCallback(this);
      
      if (deliveryCallback == null)
      {
         deliveryCallback = new DeliveryCallback();
         tx.addCallback(deliveryCallback, this);
      }
            
      for(Iterator i = acks.iterator(); i.hasNext(); )
      {
         Ack ack = (Ack)i.next();
         
         Long id = new Long(ack.getDeliveryID());
         
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
         
         if (rec == null)
         {
            log.warn("Cannot find delivery to acknowledge " + ack);
            continue;
         }
                           
         deliveryCallback.addDeliveryId(id);
         rec.del.acknowledge(tx);
      }      
   }
   
   /**
    * Starts this session's Consumers
    */
   void setStarted(boolean s) throws Throwable
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

   void promptDelivery(final Channel channel)
   {
   	if (trace) { log.trace("Prompting delivery on " + channel); }
   	
      try
      {
         //Prompting delivery must be asynchronous to avoid deadlock
         //but we cannot use one way invocations on cancelDelivery and
         //cancelDeliveries because remoting one way invocations can 
         //overtake each other in flight - this problem will
         //go away when we have our own transport and our dedicated connection
         this.executor.execute(new Runnable() { public void run() { channel.deliver();} } );
         
      }
      catch (Throwable t)
      {
         log.error("Failed to prompt delivery", t);
      }
   }
   

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private Delivery cancelDeliveryInternal(Cancel cancel) throws Throwable
   {
      DeliveryRecord rec = (DeliveryRecord)deliveries.remove(new Long(cancel.getDeliveryId()));
      
      if (rec == null)
      {
         throw new IllegalStateException("Cannot find delivery to cancel " + cancel.getDeliveryId());
      }
                 
      //Note we check the flag *and* evaluate again, this is because the server and client clocks may
      //be out of synch and don't want to send back to the client a message it thought it has sent to
      //the expiry queue  
      boolean expired = cancel.isExpired() || rec.del.getReference().getMessage().isExpired();
      
      //Note we check the flag *and* evaluate again, this is because the server value of maxDeliveries
      //might get changed after the client has sent the cancel - and we don't want to end up cancelling
      //back to the original queue
      boolean reachedMaxDeliveryAttempts =
         cancel.isReachedMaxDeliveryAttempts() || cancel.getDeliveryCount() >= rec.maxDeliveryAttempts;
                    
      Delivery del = rec.del;   
         
      if (!expired && !reachedMaxDeliveryAttempts)
      {
         //Normal cancel back to the queue
         
         del.getReference().setDeliveryCount(cancel.getDeliveryCount());
         
         //Do we need to set a redelivery delay?
         
         if (rec.redeliveryDelay != 0)
         {
            del.getReference().setScheduledDeliveryTime(System.currentTimeMillis() + rec.redeliveryDelay);
         }
         
         if (trace) { log.trace("Cancelling delivery " + cancel.getDeliveryId()); }
         del.cancel();
      }
      else
      {                  
         if (expired)
         {
            //Sent to expiry queue
            
            JBossMessage copy = makeCopyForDLQOrExpiry(true, del);
            
            moveInTransaction(copy, del, rec.expiryQueue);
         }
         else
         {
            //Send to DLQ
         	
            JBossMessage copy = makeCopyForDLQOrExpiry(false, del);
            
            moveInTransaction(copy, del, rec.dlq);
         }
      }      
      
      //Need to send a message to the replicant to remove the id
      postOffice.sendReplicateAckMessage(rec.queueName, del.getReference().getMessage().getMessageID());
      
      return rec.del;
   }      
   
   private JBossMessage makeCopyForDLQOrExpiry(boolean expiry, Delivery del) throws Exception
   {
      //We copy the message and send that to the dlq/expiry queue - this is because
      //otherwise we may end up with a ref with the same message id in the queue more than once
      //which would barf - this might happen if the same message had been expire from multiple
      //subscriptions of a topic for example
      //We set headers that hold the original message destination, expiry time and original message id
      
   	if (trace) { log.trace("Making copy of message for DLQ or expiry " + del); }
   	
      JBossMessage msg = ((JBossMessage)del.getReference().getMessage());
      
      JBossMessage copy = msg.doCopy();
      
      long newMessageId = sp.getMessageIDManager().getID();
      
      copy.setMessageId(newMessageId);
      
      //reset expiry
      copy.setExpiration(0);
      
      String origMessageId = msg.getJMSMessageID();
      
      String origDest = msg.getJMSDestination().toString();
            
      copy.setStringProperty(JBossMessage.JBOSS_MESSAGING_ORIG_MESSAGE_ID, origMessageId);
      
      copy.setStringProperty(JBossMessage.JBOSS_MESSAGING_ORIG_DESTINATION, origDest);
      
      if (expiry)
      {
         long actualExpiryTime = System.currentTimeMillis();
         
         copy.setLongProperty(JBossMessage.JBOSS_MESSAGING_ACTUAL_EXPIRY_TIME, actualExpiryTime);
      }
      
      return copy;
   }
   
   private void moveInTransaction(JBossMessage msg, Delivery del, Queue queue) throws Throwable
   {
      Transaction tx = tr.createTransaction();
      
      MessageReference ref = ms.reference(msg);      
                    
      try
      {               
         if (queue != null)
         {                                                       
            queue.handle(null, ref, tx);
            del.acknowledge(tx);
         }
         else
         {
            log.warn("Cannot move to destination since destination has not been deployed! " +
               "The message will be removed");
            
            del.acknowledge(tx);
         }             
         
         tx.commit();         
      }
      catch (Throwable t)
      {
         tx.rollback();
         throw t;
      } 
      finally
      {
         if (ref != null)
         {
            ref.releaseMemoryReference();
         }
      }
      
      //Need to prompt delivery on the dlq/expiry queue
      
      //TODO - are we sure this is the right place to prompt delivery?
      if (queue != null)
      {
         promptDelivery(queue);
      }
   }
   
   private void acknowledgeDeliveryInternal(Ack ack) throws Throwable
   {
      if (trace) { log.trace(this + " acknowledging delivery " + ack); }
      
      DeliveryRecord rec = (DeliveryRecord)deliveries.remove(new Long(ack.getDeliveryID()));
      
      if (rec == null)
      {
         log.warn("Cannot find " + ack + " to acknowledge, " +
            "maybe it was already acknowledged before failover!");
         return;
      }
      
      rec.del.acknowledge(null);  
      
      //Now replicate the ack
      
      if (rec.replicating)
      {
      	postOffice.sendReplicateAckMessage(rec.queueName, rec.del.getReference().getMessage().getMessageID());
      }
      
      if (trace) { log.trace(this + " acknowledged delivery " + ack); }
   }
   
   /* TODO We can combine this with createConsumerDelegateInternal once we move the distinction between queues and topics
    * to the client side, so the server side just deals with queues named by string - i.e MessagingQueue instances
    */
   private ConsumerDelegate createConsumerDelegateDirect(String queueName, String selectorString) throws Throwable
   {
   	if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      
      if ("".equals(selectorString))
      {
         selectorString = null;
      }
      
      if (trace)
      {
         log.trace(this + " creating direct consumer for " + queueName +
            (selectorString == null ? "" : ", selector '" + selectorString + "'"));
      }
      
      Binding binding =  postOffice.getBindingForQueueName(queueName);
      
      if (binding == null)
      {
      	throw new IllegalArgumentException("Cannot find queue with name " + queueName);
      }
      
      String consumerID = GUIDGenerator.generateGUID();
            
      int prefetchSize = connectionEndpoint.getPrefetchSize();
      
      JBossDestination dest = new JBossQueue(queueName);
      
      //We don't care about redelivery delays and number of attempts for a direct consumer
      
      ServerConsumerEndpoint ep =
         new ServerConsumerEndpoint(consumerID, binding.queue,
                                    binding.queue.getName(), this, selectorString, false,
                                    dest, null, null, 0, -1, true, false);
      
      ConsumerAdvised advised;
      
      // Need to synchronized to prevent a deadlock
      // See http://jira.jboss.com/jira/browse/JBMESSAGING-797
      synchronized (AspectManager.instance())
      {       
         advised = new ConsumerAdvised(ep);
      }
      
      Dispatcher.instance.registerTarget(consumerID, advised);
      
      ClientConsumerDelegate stub =
         new ClientConsumerDelegate(consumerID, prefetchSize, -1, 0);
      
      synchronized (consumers)
      {
         consumers.put(consumerID, ep);
      }
         
      log.trace(this + " created and registered " + ep);    
      
      return stub;
   }

   private ConsumerDelegate createConsumerDelegateInternal(JBossDestination jmsDestination,
                                                           String selectorString,
                                                           boolean noLocal,
                                                           String subscriptionName)
      throws Throwable
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      
      if ("".equals(selectorString))
      {
         selectorString = null;
      }
      
      if (trace)
      {
         log.trace(this + " creating consumer for " + jmsDestination +
            (selectorString == null ? "" : ", selector '" + selectorString + "'") +
            (subscriptionName == null ? "" : ", subscription '" + subscriptionName + "'") +
            (noLocal ? ", noLocal" : ""));
      }

      ManagedDestination mDest = dm.getDestination(jmsDestination.getName(), jmsDestination.isQueue());
      
      if (mDest == null)
      {
         throw new InvalidDestinationException("No such destination: " + jmsDestination + " has it been deployed?");
      }
      
      if (jmsDestination.isTemporary())
      {
         // Can only create a consumer for a temporary destination on the same connection
         // that created it
         if (!connectionEndpoint.hasTemporaryDestination(jmsDestination))
         {
            String msg = "Cannot create a message consumer on a different connection " +
                         "to that which created the temporary destination";
            throw new IllegalStateException(msg);
         }
      }
      
      String consumerID = GUIDGenerator.generateGUID();
      
      // Always validate the selector first
      Selector selector = null;
      
      if (selectorString != null)
      {
         selector = new Selector(selectorString);
      }
      
      Queue queue;
      
      if (jmsDestination.isTopic())
      {         
         if (subscriptionName == null)
         {
            // non-durable subscription
            if (log.isTraceEnabled()) { log.trace(this + " creating new non-durable subscription on " + jmsDestination); }
            
            // Create the non durable sub
            
            queue = new MessagingQueue(nodeId, GUIDGenerator.generateGUID(),
							                  idm.getID(), ms, pm, false,
							                  mDest.getMaxSize(), selector,
							                  mDest.getFullSize(),
							                  mDest.getPageSize(),
							                  mDest.getDownCacheSize(),
							                  mDest.isClustered(),
							                  sp.getRecoverDeliveriesTimeout());
            
            JMSCondition topicCond = new JMSCondition(false, jmsDestination.getName());
                        
            postOffice.addBinding(new Binding(topicCond, queue, false), false);   
            
            queue.activate();

            String counterName = TopicService.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + queue.getName();
  
            int dayLimitToUse = mDest.getMessageCounterHistoryDayLimit();
            if (dayLimitToUse == -1)
            {
               //Use override on server peer
               dayLimitToUse = sp.getDefaultMessageCounterHistoryDayLimit();
            }
            
            //We don't create message counters on temp topics
            if (!mDest.isTemporary())
            {
	            MessageCounter counter =  new MessageCounter(counterName, null, queue, true, false, dayLimitToUse);
	            
	            sp.getMessageCounterManager().registerMessageCounter(counterName, counter);
            }
         }
         else
         {
            if (jmsDestination.isTemporary())
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
            
            Binding binding = postOffice.getBindingForQueueName(name);
            
            if (binding == null)
            {
               // Does not already exist
               
               if (trace) { log.trace(this + " creating new durable subscription on " + jmsDestination); }
                              
               queue = new MessagingQueue(nodeId, name, idm.getID(),
                                          ms, pm, true,
                                          mDest.getMaxSize(), selector,
                                          mDest.getFullSize(),
                                          mDest.getPageSize(),
                                          mDest.getDownCacheSize(),
                                          mDest.isClustered(),
                                          sp.getRecoverDeliveriesTimeout());
               
               // Durable subs must be bound on ALL nodes of the cluster (if clustered)
               
               postOffice.addBinding(new Binding(new JMSCondition(false, jmsDestination.getName()), queue, true),
                                     postOffice.isClustered() && mDest.isClustered());
               
               queue.activate();
                  
               //We don't create message counters on temp topics
               if (!mDest.isTemporary())
               {	               
	               String counterName = TopicService.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + queue.getName();
	                       
	               MessageCounter counter =
	                  new MessageCounter(counterName, subscriptionName, queue, true, true,
	                                     mDest.getMessageCounterHistoryDayLimit());
	               
	               sp.getMessageCounterManager().registerMessageCounter(counterName, counter);
               }
            }
            else
            {
               //Durable sub already exists
            	
            	queue = binding.queue;
            	
               if (trace) { log.trace(this + " subscription " + subscriptionName + " already exists"); }
               
            	//Check if it is already has a subscriber
            	//We can't have more than one subscriber at a time on the durable sub
               
               if (queue.getLocalDistributor().getNumberOfReceivers() > 0)
               {
               	throw new IllegalStateException("Cannot create a subscriber on the durable subscription since it already has subscriber(s)");
               }
               
               // If the durable sub exists because it is clustered and was created on this node due to a bind on another node
               // then it will have no message counter
               
               String counterName = TopicService.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + queue.getName();    
               
               boolean createCounter = false;
               
               if (sp.getMessageCounterManager().getMessageCounter(counterName) == null)
               {
               	createCounter = true;
               }
                              
               // From javax.jms.Session Javadoc (and also JMS 1.1 6.11.1):
               // A client can change an existing durable subscription by creating a durable
               // TopicSubscriber with the same name and a new topic and/or message selector.
               // Changing a durable subscriber is equivalent to unsubscribing (deleting) the old
               // one and creating a new one.
               
               String filterString = queue.getFilter() != null ? queue.getFilter().getFilterString() : null;
               
               boolean selectorChanged =
                  (selectorString == null && filterString != null) ||
                  (filterString == null && selectorString != null) ||
                  (filterString != null && selectorString != null &&
                           !filterString.equals(selectorString));
               
               if (trace) { log.trace("selector " + (selectorChanged ? "has" : "has NOT") + " changed"); }
               
               String oldTopicName = ((JMSCondition)binding.condition).getName();
               
               boolean topicChanged = !oldTopicName.equals(jmsDestination.getName());
               
               if (log.isTraceEnabled()) { log.trace("topic " + (topicChanged ? "has" : "has NOT") + " changed"); }
               
               if (selectorChanged || topicChanged)
               {
                  if (trace) { log.trace("topic or selector changed so deleting old subscription"); }
                  
                  // Unbind the durable subscription
                  
                  // Durable subs must be unbound on ALL nodes of the cluster
                  
                  postOffice.removeBinding(queue.getName(), postOffice.isClustered() && mDest.isClustered());                  
                  
                  // create a fresh new subscription
                                    
                  queue = new MessagingQueue(nodeId, name, idm.getID(), ms, pm, true,
					                              mDest.getMaxSize(), selector,
					                              mDest.getFullSize(),
					                              mDest.getPageSize(),
					                              mDest.getDownCacheSize(),
					                              mDest.isClustered(),
					                              sp.getRecoverDeliveriesTimeout());
                  
                  // Durable subs must be bound on ALL nodes of the cluster
                  
                  postOffice.addBinding(new Binding(new JMSCondition(false, jmsDestination.getName()), queue, true),
                  		                postOffice.isClustered() && mDest.isClustered());
  
                  queue.activate();                  
                  
                  if (!mDest.isTemporary())
                  {
	                  createCounter = true;
                  }
               }
               
               if (createCounter)
               {
               	MessageCounter counter =
                     new MessageCounter(counterName, subscriptionName, queue, true, true,
                                        mDest.getMessageCounterHistoryDayLimit());
                  
                  sp.getMessageCounterManager().registerMessageCounter(counterName, counter);
               }
            }
         }
      }
      else
      {
         // Consumer on a jms queue
         
         // Let's find the queue
      	
      	queue = postOffice.getBindingForQueueName(jmsDestination.getName()).queue;
         
         if (queue == null)
         {
            throw new IllegalStateException("Cannot find queue: " + jmsDestination.getName());
         }
      }
      
      int prefetchSize = connectionEndpoint.getPrefetchSize();
      
      Queue dlqToUse = mDest.getDLQ() == null ? defaultDLQ : mDest.getDLQ();
      
      Queue expiryQueueToUse = mDest.getExpiryQueue() == null ? defaultExpiryQueue : mDest.getExpiryQueue();
      
      int maxDeliveryAttemptsToUse = mDest.getMaxDeliveryAttempts() == -1 ? defaultMaxDeliveryAttempts : mDest.getMaxDeliveryAttempts();
          
      long redeliveryDelayToUse = mDest.getRedeliveryDelay() == -1 ? defaultRedeliveryDelay : mDest.getRedeliveryDelay();
      
      //Is the consumer going to have its session state replicated onto a backup node?      
      
      boolean replicating = supportsFailover && queue.isClustered() && !(jmsDestination.isTopic() && !queue.isRecoverable());
      
      ServerConsumerEndpoint ep =
         new ServerConsumerEndpoint(consumerID, queue,
                                    queue.getName(), this, selectorString, noLocal,
                                    jmsDestination, dlqToUse, expiryQueueToUse, redeliveryDelayToUse,
                                    maxDeliveryAttemptsToUse, false, replicating);
      
      if (queue.isClustered() && postOffice.isClustered() && jmsDestination.isTopic() && subscriptionName != null)
      {
      	//Clustered durable sub consumer created - we need to add this info in the replicator - it is needed by other nodes
      	
      	//This is also used to prevent a possible race condition where a clustered durable sub is bound on all nodes
      	//but then unsubscribed before the bind is complete on all nodes, leaving it bound on some nodes and not on others
      	//The bind all is synchronous so by the time we add the x to the replicator we know it is bound on all nodes
      	//and same to unsubscribe
      	
      	Replicator rep = (Replicator)postOffice;
      	
      	rep.put(queue.getName(), DUR_SUB_STATE_CONSUMERS);
      }
      
      ConsumerAdvised advised;
      
      // Need to synchronized to prevent a deadlock
      // See http://jira.jboss.com/jira/browse/JBMESSAGING-797
      synchronized (AspectManager.instance())
      {       
         advised = new ConsumerAdvised(ep);
      }
      
      Dispatcher.instance.registerTarget(consumerID, advised);
      
      ClientConsumerDelegate stub =
         new ClientConsumerDelegate(consumerID, prefetchSize, maxDeliveryAttemptsToUse, redeliveryDelayToUse);
      
      synchronized (consumers)
      {
         consumers.put(consumerID, ep);
      }
         
      log.trace(this + " created and registered " + ep);
      
      return stub;
   }   

   private BrowserDelegate createBrowserDelegateInternal(JBossDestination jmsDestination,
                                                         String selector) throws Throwable
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }

      if (jmsDestination == null)
      {
         throw new InvalidDestinationException("null destination");
      }

      if (jmsDestination.isTopic())
      {
         throw new IllegalStateException("Cannot browse a topic");
      }

      if (dm.getDestination(jmsDestination.getName(), jmsDestination.isQueue()) == null)
      {
         throw new InvalidDestinationException("No such destination: " + jmsDestination);
      }

      log.trace(this + " creating browser for " + jmsDestination +
         (selector == null ? "" : ", selector '" + selector + "'"));

      Binding binding = postOffice.getBindingForQueueName(jmsDestination.getName());
      
      if (binding == null)
      {
      	throw new IllegalStateException("Cannot find queue with name " + jmsDestination.getName());
      }
      
      String browserID = GUIDGenerator.generateGUID();

      ServerBrowserEndpoint ep = new ServerBrowserEndpoint(this, browserID, binding.queue, selector);

      // still need to synchronized since close() can come in on a different thread
      synchronized (browsers)
      {
         browsers.put(browserID, ep);
      }

      BrowserAdvised advised;
      
      // Need to synchronized to prevent a deadlock
      // See http://jira.jboss.com/jira/browse/JBMESSAGING-797
      synchronized (AspectManager.instance())
      {       
         advised = new BrowserAdvised(ep);
      }
      
      Dispatcher.instance.registerTarget(browserID, advised);

      ClientBrowserDelegate stub = new ClientBrowserDelegate(browserID);

      log.trace(this + " created and registered " + ep);

      return stub;
   }

   private void promptDelivery(Set channels)
   {
      //Now prompt delivery on the channels
      Iterator iter = channels.iterator();
      
      while (iter.hasNext())
      {
         DeliveryObserver observer = (DeliveryObserver)iter.next();
         
         promptDelivery((Channel)observer);
      }
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
   	
      Delivery del;
        
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
            
      DeliveryRecord(Delivery del, Queue dlq, Queue expiryQueue, long redeliveryDelay, int maxDeliveryAttempts,
      		         String queueName, boolean replicating, long deliveryID)
      {
      	this.del = del;
      	
      	this.dlq = dlq;
      	
      	this.expiryQueue = expiryQueue;
      	
      	this.redeliveryDelay = redeliveryDelay;
      	
      	this.maxDeliveryAttempts = maxDeliveryAttempts;
      	
      	this.queueName = queueName;
      	
      	this.replicating = replicating;
      	
      	this.deliveryID = deliveryID;
      }
      
      DeliveryRecord(Delivery del, ServerConsumerEndpoint consumer, long deliveryID)
      {
      	this (del, consumer.getDLQ(), consumer.getExpiryQueue(), consumer.getRedliveryDelay(), consumer.getMaxDeliveryAttempts(),
      			consumer.getQueueName(), consumer.isReplicating(), deliveryID);

      	// We need to cache the attributes here  since the consumer may get gc'd BEFORE the delivery is acked
         
      	
         //We hold a WeakReference to the consumer - this is only needed when replicating - where we store the delivery then wait
         //for the response to come back from the replicant before actually performing delivery
         //We need a weak ref since when the consumer closes deliveries may still and remain and we don't want that to prevent
         //the consumer being gc'd
         this.consumerRef = new WeakReference(consumer);
      }            
      
   	public String toString()
   	{
   		return "DeliveryRecord " + System.identityHashCode(this) + " del: " + del + " queueName: " + queueName;
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
            Long deliveryId = (Long)iter.next();
            
            DeliveryRecord del = (DeliveryRecord)deliveries.remove(deliveryId);
            
            if (del != null && del.replicating)
            {
            	//TODO - we could batch this in one message
            	try
            	{
            		postOffice.sendReplicateAckMessage(del.queueName, del.del.getReference().getMessage().getMessageID());
            	}
            	catch (Exception e)
            	{            		
            		throw new TransactionException("Failed to handle send ack", e);
            	}
            }
         }
      }
      
      public void afterRollback(boolean onePhase) throws TransactionException
      {                            
         //One phase rollbacks never hit the server - they are dealt with locally only
         //so this would only ever be executed for a two phase rollback.

         //We don't do anything since cancellation is driven from the client.
      }
      
      synchronized void addDeliveryId(Long deliveryId)
      {
         delList.add(deliveryId);
      }
   }
}
