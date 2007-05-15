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
import org.jboss.jms.wireformat.Dispatcher;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.plugin.IDManager;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionException;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.core.tx.TxCallback;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.MessageQueueNameHelper;
import org.jboss.util.id.GUID;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
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
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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
   
   static final String TEMP_QUEUE_MESSAGECOUNTER_PREFIX = "TempQueue.";
   
   public static final String JBOSS_MESSAGING_ORIG_DESTINATION = "JBM_ORIG_DESTINATION";

   public static final String JBOSS_MESSAGING_ORIG_MESSAGE_ID = "JBM_ORIG_MESSAGE_ID";
   
   public static final String JBOSS_MESSAGING_ACTUAL_EXPIRY_TIME = "JBM_ACTUAL_EXPIRY";
   
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private int id;

   private volatile boolean closed;

   private ServerConnectionEndpoint connectionEndpoint;
   
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
   private int maxDeliveryAttempts;
   private Queue defaultDLQ;
   private Queue defaultExpiryQueue;
   
   // Map <deliveryID, Delivery>
   private Map deliveries;
   
   private SynchronizedLong deliveryIdSequence;
   
   //Temporary until we have our own NIO transport   
   QueuedExecutor executor = new QueuedExecutor();
   
   // Constructors ---------------------------------------------------------------------------------

   ServerSessionEndpoint(int sessionID, ServerConnectionEndpoint connectionEndpoint)
      throws Exception
   {
      this.id = sessionID;

      this.connectionEndpoint = connectionEndpoint;

      sp = connectionEndpoint.getServerPeer();

      pm = sp.getPersistenceManagerInstance();
      ms = sp.getMessageStore();
      dm = sp.getDestinationManager();
      postOffice = sp.getPostOfficeInstance();     
      idm = sp.getChannelIDManager();
      nodeId = sp.getServerPeerID();
      tr = sp.getTxRepository();

      consumers = new HashMap();
		browsers = new HashMap();
      
      defaultDLQ = sp.getDefaultDLQInstance();
      defaultExpiryQueue = sp.getDefaultExpiryQueueInstance();
      tr = sp.getTxRepository();
      maxDeliveryAttempts = sp.getDefaultMaxDeliveryAttempts();
      
      deliveries = new ConcurrentHashMap();
      
      deliveryIdSequence = new SynchronizedLong(0);
   }
   
   // SessionDelegate implementation ---------------------------------------------------------------
       
   public ConsumerDelegate createConsumerDelegate(JBossDestination jmsDestination,
                                                  String selector,
                                                  boolean noLocal,
                                                  String subscriptionName,
                                                  boolean isCC) throws JMSException
   {
      try
      {
         return createConsumerDelegateInternal(jmsDestination, selector,
                                               noLocal, subscriptionName);
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
                              
         // need to prompt delivery for all affected channels
         
         promptDelivery(channels);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " cancelDeliveries");
      }
   }         
   
   public void recoverDeliveries(List deliveryRecoveryInfos) throws JMSException
   {
      if (trace) { log.trace(this + "recovers deliveries " + deliveryRecoveryInfos); }

      try
      {
         if (postOffice.isLocal())
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
            
            //Look up channel
            Binding binding = postOffice.getBindingForQueueName(queueName);
            
            if (binding == null)
            {
               throw new IllegalStateException("Cannot find channel with queue name: " + queueName);
            }
            
            List acks = (List)entry.getValue();
            
            List ids = new ArrayList(acks.size());
            
            for (Iterator iter2 = acks.iterator(); iter2.hasNext(); )
            {
               DeliveryRecovery info = (DeliveryRecovery)iter2.next();
               
               ids.add(new Long(info.getMessageID()));
            }
            
            Queue queue = binding.getQueue();
            
            JMSCondition cond = (JMSCondition)binding.getCondition();                        
            
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
                              new DeliveryRecord(del, -1, dlqToUse,
                                                 expiryQueueToUse, dest.getRedeliveryDelay()));
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
         
         if (dest.isTopic())
         {
            mDest = new ManagedTopic(dest.getName(), fullSize, pageSize, downCacheSize);
         }
         else
         {
            mDest = new ManagedQueue(dest.getName(), fullSize, pageSize, downCacheSize);
         }
         
         dm.registerDestination(mDest);
         
         if (dest.isQueue())
         {            
            Queue coreQueue;

            coreQueue = new PagingFilteredQueue(dest.getName(),
                                                idm.getID(), ms, pm, true, false,
                                                -1, null, fullSize, pageSize, downCacheSize);

            String counterName = TEMP_QUEUE_MESSAGECOUNTER_PREFIX + dest.getName();
            
            MessageCounter counter =
               new MessageCounter(counterName, null, coreQueue, false, false,
                                  sp.getDefaultMessageCounterHistoryDayLimit());
            
            sp.getMessageCounterManager().registerMessageCounter(counterName, counter);
                                 
            // make a binding for this queue
            postOffice.bindQueue(new JMSCondition(true, dest.getName()), coreQueue);
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
         	
         	if (binding.getQueue().getNumberOfReceivers() != 0)
         	{
         		throw new IllegalStateException("Cannot delete temporary queue if it has consumer(s)");
         	}
         	
            //Unbind
            postOffice.unbindQueue(dest.getName());
            
            String counterName = TEMP_QUEUE_MESSAGECOUNTER_PREFIX + dest.getName();
            
            connectionEndpoint.removeTemporaryDestination(dest);
            
            MessageCounter counter =
               sp.getMessageCounterManager().unregisterMessageCounter(counterName);
            
            if (counter == null)
            {
               throw new IllegalStateException("Cannot find counter to unregister " + counterName);
            }
         }
         else
         {
            //Topic            
            Collection bindings =
               postOffice.getBindingsForCondition(new JMSCondition(false, dest.getName()));
            
            Iterator iter = bindings.iterator();
            
            while (iter.hasNext())
            {
            	Binding binding = (Binding)iter.next();
            	
            	if (binding.getQueue().getNumberOfReceivers() != 0)
            	{
            		throw new IllegalStateException("Cannot delete temporary destination if it has consumer(s)");
            	}
            }
         }
         
         dm.unregisterDestination(mDest);                             
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " deleteTemporaryDestination");
      }
   }
   
   public void unsubscribe(String subscriptionName) throws JMSException
   {
      log.debug(this + " unsubscribing " + subscriptionName);
      
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
         
         // Section 6.11. JMS 1.1.
         // "It is erroneous for a client to delete a durable subscription while it has an active
         // TopicSubscriber for it or while a message received by it is part of a current
         // transaction or has not been acknowledged in the session."
         
         Queue sub = binding.getQueue();
         
         if (sub.getNumberOfReceivers() != 0)
         {
            throw new IllegalStateException("Cannot unsubscribe durable subscription " +
                                            subscriptionName + " since it has active subscribers");
         }
         
         //Look up the topic
         
         JMSCondition topicCond = (JMSCondition)binding.getCondition();
         
         String topicName = topicCond.getName();
         
         ManagedDestination mDest = dm.getDestination(topicName, false);
         
         //Unbind it
    
         if (mDest.isClustered() && !postOffice.isLocal())
         {
            ClusteredPostOffice cpo = (ClusteredPostOffice)postOffice;
            
            cpo.unbindClusteredQueue(queueName);
         }
         else
         {         
            postOffice.unbindQueue(queueName);
         }
         
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
      }
   }
      
   void removeBrowser(int browserId) throws Exception
   {
      synchronized (browsers)
      {
         if (browsers.remove(new Integer(browserId)) == null)
         {
            throw new IllegalStateException("Cannot find browser with id " + browserId + " to remove");
         }
      }
   }
   
   void removeConsumer(int consumerId) throws Exception
   {
      synchronized (consumers)
      {
         if (consumers.remove(new Integer(consumerId)) == null)
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
      executor.shutdownAfterProcessingCurrentTask();

      deliveries.clear();
      
      sp.removeSession(new Integer(id));
            
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
   
   long addDelivery(Delivery del, int consumerId, Queue dlq, Queue expiryQueue, long redeliveryDelay)
   {
      long deliveryId = deliveryIdSequence.increment();
      
      deliveries.put(new Long(deliveryId), new DeliveryRecord(del, consumerId, dlq, expiryQueue, redeliveryDelay));
      
      if (trace) { log.trace(this + " added delivery " + deliveryId + ": " + del); }
      
      return deliveryId;      
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
      try
      {
         //Prompting delivery must be asynchronous to avoid deadlock
         //but we cannot use one way invocations on cancelDelivery and
         //cancelDeliveries because remoting one way invocations can 
         //overtake each other in flight - this problem will
         //go away when we have our own transport and our dedicated connection
         this.executor.execute(new Runnable() { public void run() { channel.deliver(); } } );
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
         cancel.isReachedMaxDeliveryAttempts() || cancel.getDeliveryCount() >= maxDeliveryAttempts;
                    
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
            
      copy.setStringProperty(JBOSS_MESSAGING_ORIG_MESSAGE_ID, origMessageId);
      
      copy.setStringProperty(JBOSS_MESSAGING_ORIG_DESTINATION, origDest);
      
      if (expiry)
      {
         long actualExpiryTime = System.currentTimeMillis();
         
         copy.setLongProperty(JBOSS_MESSAGING_ACTUAL_EXPIRY_TIME, actualExpiryTime);
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
      
      if (trace) { log.trace(this + " acknowledged delivery " + ack); }
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

      ManagedDestination mDest = dm.
         getDestination(jmsDestination.getName(), jmsDestination.isQueue());
      
      if (mDest == null)
      {
         throw new InvalidDestinationException("No such destination: " + jmsDestination);
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
      
      int consumerID = connectionEndpoint.getServerPeer().getNextObjectID();
      
      Binding binding = null;
      
      // Always validate the selector first
      Selector selector = null;
      if (selectorString != null)
      {
         selector = new Selector(selectorString);
      }
      
      if (jmsDestination.isTopic())
      {
         JMSCondition topicCond = new JMSCondition(false, jmsDestination.getName());
         
         if (subscriptionName == null)
         {
            // non-durable subscription
            if (log.isTraceEnabled()) { log.trace(this + " creating new non-durable subscription on " + jmsDestination); }
            
            // Create the non durable sub
                        
            PagingFilteredQueue q;
            
            if (postOffice.isLocal() || !mDest.isClustered())
            {
               q = new PagingFilteredQueue(new GUID().toString(), idm.getID(), ms, pm, true, false,
                        mDest.getMaxSize(), selector,
                        mDest.getFullSize(),
                        mDest.getPageSize(),
                        mDest.getDownCacheSize());
               
               binding = postOffice.bindQueue(topicCond, q);
            }
            else
            {  
               q = new LocalClusteredQueue((ClusteredPostOffice)postOffice, nodeId, new GUID().toString(),
                                           idm.getID(), ms, pm, true, false,
                                           mDest.getMaxSize(), selector, tr,
                                           mDest.getFullSize(),
                                           mDest.getPageSize(),
                                           mDest.getDownCacheSize());
               
               ClusteredPostOffice cpo = (ClusteredPostOffice)postOffice;

               binding = cpo.bindClusteredQueue(topicCond, (LocalClusteredQueue)q);
            }
            String counterName = TopicService.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + q.getName();
  
            int dayLimitToUse = mDest.getMessageCounterHistoryDayLimit();
            if (dayLimitToUse == -1)
            {
               //Use override on server peer
               dayLimitToUse = sp.getDefaultMessageCounterHistoryDayLimit();
            }
            
            MessageCounter counter =
               new MessageCounter(counterName, null, q, true, false,
                                  dayLimitToUse);
            
            sp.getMessageCounterManager().registerMessageCounter(counterName, counter);
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
            
            binding = postOffice.getBindingForQueueName(name);
            
            if (binding == null)
            {
               // Does not already exist
               
               if (trace) { log.trace(this + " creating new durable subscription on " + jmsDestination); }
                              
               PagingFilteredQueue q;

               if (postOffice.isLocal())
               {
                  q = new PagingFilteredQueue(name, idm.getID(), ms, pm, true, true,
                                              mDest.getMaxSize(), selector,
                                              mDest.getFullSize(),
                                              mDest.getPageSize(),
                                              mDest.getDownCacheSize());

                  binding = postOffice.bindQueue(topicCond, q);
               }
               else
               {
                  q = new LocalClusteredQueue((ClusteredPostOffice)postOffice, nodeId, name, idm.getID(),
                                              ms, pm, true, true,
                                              mDest.getMaxSize(), selector, tr,
                                              mDest.getFullSize(),
                                              mDest.getPageSize(),
                                              mDest.getDownCacheSize());
                  
                  ClusteredPostOffice cpo = (ClusteredPostOffice)postOffice;
                  
                  if (mDest.isClustered())
                  {
                     binding = cpo.bindClusteredQueue(topicCond, (LocalClusteredQueue)q);
                  }
                  else
                  {
                     binding = cpo.bindQueue(topicCond, q);
                  }
               }
               String counterName = TopicService.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + q.getName();
                       
               MessageCounter counter =
                  new MessageCounter(counterName, subscriptionName, q, true, true,
                                     mDest.getMessageCounterHistoryDayLimit());
               
               sp.getMessageCounterManager().registerMessageCounter(counterName, counter);
            }
            else
            {
               //Durable sub already exists
            	
               if (trace) { log.trace(this + " subscription " + subscriptionName + " already exists"); }
               
            	//Check if it is already has a subscriber
            	//We can't have more than one subscriber at a time on the durable sub
               
               if (binding.getQueue().getNumberOfReceivers() > 0)
               {
               	throw new IllegalStateException("Cannot create a subscriber on the durable subscription since it already has subscriber(s)");
               }
                              
               // From javax.jms.Session Javadoc (and also JMS 1.1 6.11.1):
               // A client can change an existing durable subscription by creating a durable
               // TopicSubscriber with the same name and a new topic and/or message selector.
               // Changing a durable subscriber is equivalent to unsubscribing (deleting) the old
               // one and creating a new one.
               
               String filterString =
                  binding.getQueue().getFilter() != null ?
                     binding.getQueue().getFilter().getFilterString() : null;
               
               boolean selectorChanged =
                  (selectorString == null && filterString != null) ||
                  (filterString == null && selectorString != null) ||
                  (filterString != null && selectorString != null &&
                           !filterString.equals(selectorString));
               
               if (trace) { log.trace("selector " + (selectorChanged ? "has" : "has NOT") + " changed"); }
               
               JMSCondition cond = (JMSCondition)binding.getCondition();
               
               boolean topicChanged = !cond.getName().equals(jmsDestination.getName());
               
               if (log.isTraceEnabled()) { log.trace("topic " + (topicChanged ? "has" : "has NOT") + " changed"); }
               
               if (selectorChanged || topicChanged)
               {
                  if (trace) { log.trace("topic or selector changed so deleting old subscription"); }
                  
                  // Unbind the durable subscription
                  
                  if (mDest.isClustered() && !postOffice.isLocal())
                  {
                     ClusteredPostOffice cpo = (ClusteredPostOffice)postOffice;
                     
                     cpo.unbindClusteredQueue(name);
                  }
                  else
                  {
                     postOffice.unbindQueue(name);
                  }
                  
                  // create a fresh new subscription
                                    
                  PagingFilteredQueue q;
                  
                  if (postOffice.isLocal())
                  {
                     q = new PagingFilteredQueue(name, idm.getID(), ms, pm, true, true,
                              mDest.getMaxSize(), selector,
                              mDest.getFullSize(),
                              mDest.getPageSize(),
                              mDest.getDownCacheSize());
                     binding = postOffice.bindQueue(topicCond, q);
                  }
                  else
                  {
                     q = new LocalClusteredQueue((ClusteredPostOffice)postOffice, nodeId, name, idm.getID(), ms, pm, true, true,
                              mDest.getMaxSize(), selector, tr,
                              mDest.getFullSize(),
                              mDest.getPageSize(),
                              mDest.getDownCacheSize());
                     
                     ClusteredPostOffice cpo = (ClusteredPostOffice)postOffice;
                     
                     if (mDest.isClustered())
                     {
                        binding = cpo.bindClusteredQueue(topicCond, (LocalClusteredQueue)q);
                     }
                     else
                     {
                        binding = cpo.bindQueue(topicCond, (LocalClusteredQueue)q);
                     }
                  }
                  String counterName = TopicService.SUBSCRIPTION_MESSAGECOUNTER_PREFIX + q.getName();
                  
                  MessageCounter counter =
                     new MessageCounter(counterName, subscriptionName, q, true, true,
                                        mDest.getMessageCounterHistoryDayLimit());
                  
                  sp.getMessageCounterManager().registerMessageCounter(counterName, counter);
               }
            }
         }
      }
      else
      {
         // Consumer on a jms queue
         
         // Let's find the binding
         binding = postOffice.getBindingForQueueName(jmsDestination.getName());
         
         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for jms queue: " + jmsDestination.getName());
         }
      }
      
      int prefetchSize = connectionEndpoint.getPrefetchSize();
      
      Queue dlqToUse = mDest.getDLQ() == null ? defaultDLQ : mDest.getDLQ();
      
      Queue expiryQueueToUse = mDest.getExpiryQueue() == null ? defaultExpiryQueue : mDest.getExpiryQueue();
      
      long redeliveryDelay = mDest.getRedeliveryDelay();
      
      if (redeliveryDelay == 0)
      {
         redeliveryDelay = sp.getDefaultRedeliveryDelay();
      }
      
      ServerConsumerEndpoint ep =
         new ServerConsumerEndpoint(consumerID, (PagingFilteredQueue)binding.getQueue(),
                  binding.getQueue().getName(), this, selectorString, noLocal,
                  jmsDestination, dlqToUse, expiryQueueToUse, redeliveryDelay);
      
      ConsumerAdvised advised;
      
      // Need to synchronized to prevent a deadlock
      // See http://jira.jboss.com/jira/browse/JBMESSAGING-797
      synchronized (AspectManager.instance())
      {       
         advised = new ConsumerAdvised(ep);
      }
      
      Dispatcher.instance.registerTarget(consumerID, advised);
      
      ClientConsumerDelegate stub =
         new ClientConsumerDelegate(consumerID,
                                    prefetchSize, maxDeliveryAttempts);
      
      synchronized (consumers)
      {
         consumers.put(new Integer(consumerID), ep);
      }
         
      log.debug(this + " created and registered " + ep);
      
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

      log.debug(this + " creating browser for " + jmsDestination +
         (selector == null ? "" : ", selector '" + selector + "'"));

      Binding binding = postOffice.getBindingForQueueName(jmsDestination.getName()); // TODO

      int browserID = connectionEndpoint.getServerPeer().getNextObjectID();

      ServerBrowserEndpoint ep =
         new ServerBrowserEndpoint(this, browserID,
                                   (PagingFilteredQueue)binding.getQueue(), selector);

      // still need to synchronized since close() can come in on a different thread
      synchronized (browsers)
      {
         browsers.put(new Integer(browserID), ep);
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

      log.debug(this + " created and registered " + ep);

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
    * We can't rely on the cancel being driven from the MessageCallbackHandler since
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
      Delivery del;
      
      int consumerId;
      
      Queue dlq;
      
      Queue expiryQueue;
      
      long redeliveryDelay;
      
      DeliveryRecord(Delivery del, int consumerId, Queue dlq, Queue expiryQueue, long redeliveryDelay)
      {
         this.del = del;
         
         this.consumerId = consumerId;
         
         this.dlq = dlq;
         
         this.expiryQueue = expiryQueue;
         
         this.redeliveryDelay = redeliveryDelay;
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
            
            deliveries.remove(deliveryId);
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
