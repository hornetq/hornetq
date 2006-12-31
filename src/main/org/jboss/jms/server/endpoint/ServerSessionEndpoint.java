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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.jboss.jms.client.delegate.ClientBrowserDelegate;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.selector.Selector;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.JMSCondition;
import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.destination.ManagedDestination;
import org.jboss.jms.server.destination.ManagedQueue;
import org.jboss.jms.server.destination.ManagedTopic;
import org.jboss.jms.server.endpoint.advised.BrowserAdvised;
import org.jboss.jms.server.endpoint.advised.ConsumerAdvised;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.util.MessageQueueNameHelper;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.plugin.IDManager;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.plugin.postoffice.cluster.RemoteQueueStub;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionException;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.core.tx.TxCallback;
import org.jboss.util.id.GUID;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedLong;

/**
 * The server side representation of a JMS session.
 * 
 * A user must not invoke methods of a session concurrently on different threads, however
 * there are situations where multiple threads may access this object concurrently, for instance:
 * 
 * A session can be closed when it's connection is closed by the user which might be called on a different thread
 * A session can be closed when the server determines the connection is dead.
 * If the session represents a connection consumer's session then the connection consumer will farm off
 * messages to different sessions obtained from a pool, these may then cancel/ack etc on different threads, but
 * the acks/cancels/etc will end up back here on the connection consumer session instance.
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
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerSessionEndpoint.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

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
   private QueuedExecutorPool pool;
   private TransactionRepository tr;
   private PostOffice postOffice;
   private int nodeId;
   private int maxDeliveryAttempts;
   private Queue defaultDLQ;
   private Queue defaultExpiryQueue;
   
   // Map < deliveryId, Delivery>
   private Map deliveries;
   
   private SynchronizedLong deliveryIdSequence;
   
   
   // Constructors --------------------------------------------------

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
      pool = sp.getQueuedExecutorPool();
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
   
   // SessionDelegate implementation --------------------------------
       
   public ConsumerDelegate createConsumerDelegate(JBossDestination jmsDestination,
                                                  String selectorString,
                                                  boolean noLocal,
                                                  String subscriptionName,
                                                  boolean isCC,
                                                  long failoverChannelId) throws JMSException
   {
      try
      {
         if (failoverChannelId == -1)
         {
            //Standard createConsumerDelegate
            
            return createConsumerDelegateInternal(jmsDestination, selectorString, noLocal, subscriptionName,
                                                  isCC);
         }
         else
         {
            //Failover of consumer
            
            return failoverConsumer(jmsDestination, selectorString, noLocal, subscriptionName,
                                    isCC, failoverChannelId);
         }         
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " createConsumerDelegate");
      }
   }
      
	public BrowserDelegate createBrowserDelegate(JBossDestination jmsDestination, String messageSelector)
	   throws JMSException
	{
      try
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
         
         Binding binding = postOffice.getBindingForQueueName(jmsDestination.getName()); // todo
         
   	   int browserID = connectionEndpoint.getServerPeer().getNextObjectID();
   	   
   	   ServerBrowserEndpoint ep =
   	      new ServerBrowserEndpoint(this, browserID, (PagingFilteredQueue)binding.getQueue(), messageSelector);
   	   
         //Still need to synchronized since close() can come in on a different thread
         synchronized (browsers)
         {
            browsers.put(new Integer(browserID), ep);
         }
   	   
         JMSDispatcher.instance.registerTarget(new Integer(browserID), new BrowserAdvised(ep));
   	   
   	   ClientBrowserDelegate stub = new ClientBrowserDelegate(browserID);
   	   
         log.debug(this + " created and registered " + ep);
   
   	   return stub;
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
      
   public void closing() throws JMSException
   {
      // currently does nothing
      if (trace) log.trace(this + " closing (noop)");
   }
 
   public void send(JBossMessage message) throws JMSException
   {
      try
      {       
         connectionEndpoint.sendMessage(message, null);
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
         throw ExceptionUtil.handleJMSInvocation(t, this + " acknowledge");
      }
   }     
         
   public void acknowledgeDeliveries(List acks) throws JMSException
   {    
      if (trace) {log.trace(this + " acknowledgeDeliveries " + acks); }
      
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
         throw ExceptionUtil.handleJMSInvocation(t, this + " acknowledgeBatch");
      }
   }
             
   public void cancelDelivery(Cancel cancel) throws JMSException
   {
      if (trace) {log.trace(this + " cancelDelivery " + cancel); }
      
      try
      {
         Delivery del = cancelDeliveryInternal(cancel);
         
         //Prompt delivery
         ((Channel)del.getObserver()).deliver(false);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " cancelDelivery");
      }     
   }            

   public void cancelDeliveries(List cancels) throws JMSException
   {
      if (trace) {log.trace(this + " cancelDeliveries " + cancels); }
        
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
      if (trace) { log.trace(this + "recovering deliveries " + deliveryRecoveryInfos); }
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
                
            Long channelId = new Long(deliveryInfo.getChannelId());
            
            List acks = (List)ackMap.get(channelId);
            
            if (acks == null)
            {
               acks = new ArrayList();
               
               ackMap.put(channelId, acks);
            }
            
            acks.add(deliveryInfo);
         }  
         
         Iterator iter = ackMap.entrySet().iterator();
         
         while (iter.hasNext())
         {
            Map.Entry entry = (Map.Entry)iter.next();
            
            Long channelId = (Long)entry.getKey();
            
            //Look up channel
            Binding binding = postOffice.getBindingforChannelId(channelId.longValue());
            
            if (binding == null)
            {
               throw new IllegalStateException("Cannot find channel with id: " + channelId);
            }
            
            List acks = (List)entry.getValue();
            
            List ids = new ArrayList(acks.size());
            
            for (Iterator iter2 = acks.iterator(); iter2.hasNext(); )
            {
               DeliveryRecovery info = (DeliveryRecovery)iter2.next();
               
               ids.add(new Long(info.getMessageId()));
            }
            
            Queue queue = binding.getQueue();
            
            List dels = queue.recoverDeliveries(ids);
            
            Iterator iter2 = dels.iterator();
            
            Iterator iter3 = acks.iterator();
            
            while (iter2.hasNext())
            {
               Delivery del = (Delivery)iter2.next();
               
               DeliveryRecovery info = (DeliveryRecovery)iter3.next();
               
               long deliveryId = info.getDeliveryId();
               
               maxDeliveryId = Math.max(maxDeliveryId, deliveryId);
               
               if (trace) { log.trace(this + " Recovered delivery " + deliveryId + ", " + del); }
               
               deliveries.put(new Long(deliveryId), new DeliveryRecord(del, -1));
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
            throw new InvalidDestinationException("Destination:" + dest + " is not a temporary destination");
         }
         connectionEndpoint.addTemporaryDestination(dest);
         
         //Register with the destination manager
         
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
            QueuedExecutor executor = (QueuedExecutor)pool.get();
            
            PagingFilteredQueue q = 
               new PagingFilteredQueue(dest.getName(), idm.getID(), ms, pm, true, false,
                                       executor, null, fullSize, pageSize, downCacheSize);                        
            
            //Make a binding for this queue
            postOffice.bindQueue(new JMSCondition(true, dest.getName()), q);
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
            throw new InvalidDestinationException("Destination:" + dest + " is not a temporary destination");
         }
         
         ManagedDestination mDest = dm.getDestination(dest.getName(), dest.isQueue());
         
         if (mDest == null)
         {
            throw new InvalidDestinationException("No such destination: " + dest);
         }
                  
         if (dest.isQueue())
         {
            //Unbind
            postOffice.unbindQueue(dest.getName());
         }
         else
         {
            //Topic            
            Collection bindings = postOffice.listBindingsForCondition(new JMSCondition(false, dest.getName()));
            
            if (!bindings.isEmpty())
            {
               throw new IllegalStateException("Cannot delete temporary destination, since it has active consumer(s)");
            }
         }
         
         dm.unregisterDestination(mDest);         
            
         connectionEndpoint.removeTemporaryDestination(dest);
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
         
         if (sub.numberOfReceivers() != 0)
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
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " unsubscribe");
      }
   }
   
   public boolean isClosed() throws JMSException
   {
      throw new IllegalStateException("isClosed should never be handled on the server side");
   }
    
   // Public --------------------------------------------------------
   
   public ServerConnectionEndpoint getConnectionEndpoint()
   {
      return connectionEndpoint;
   }

   public String toString()
   {
      return "SessionEndpoint[" + id + "]";
   }

   // Package protected ---------------------------------------------
   
   void expireDelivery(Delivery del, Queue expiryQueue) throws Throwable
   {
      if (trace) { log.trace("Reference has expired: " + del.getReference()); }
      
      if (expiryQueue != null)
      {
         if (trace) { log.trace("Sending to expiry queue"); }
         
         moveInTransaction(del, expiryQueue);
      }
      else
      {
         log.warn("No expiry queue has been configured so removing the reference");
      }
   }
      
   void cancelDeliveriesForConsumerAfterDeliveryId(int consumerId, long lastDeliveryId) throws Throwable
   {
      //Need to cancel in reverse
      
      LinkedList toCancel = new LinkedList();
      
      Iterator iter = deliveries.entrySet().iterator();
            
      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry)iter.next();
         
         Long deliveryId = (Long)entry.getKey();
         
         DeliveryRecord record = (DeliveryRecord)entry.getValue();
         
         if (record.consumerId == consumerId && deliveryId.longValue() > lastDeliveryId)
         {
            iter.remove();
            
            toCancel.addFirst(record);
         }
      }
      
      iter = toCancel.iterator();
      
      while (iter.hasNext())
      {
         DeliveryRecord record = (DeliveryRecord)iter.next();
         
         record.del.cancel();
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
            
      synchronized (consumers)
      {            
         for( Iterator i = consumers.values().iterator(); i.hasNext(); )
         {
            ((ServerConsumerEndpoint)i.next()).localClose();
         }  
         
         consumers.clear();
      }
      
      synchronized (browsers)
      {            
         for( Iterator i = browsers.values().iterator(); i.hasNext(); )
         {
            ((ServerBrowserEndpoint)i.next()).localClose();
         }  
         
         browsers.clear();
      }
      
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
      
      deliveries.clear();
      
      sp.removeSession(new Integer(id));
            
      JMSDispatcher.instance.unregisterTarget(new Integer(id));
      
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
   
   long addDelivery(Delivery del, int consumerId)
   {
      long deliveryId = deliveryIdSequence.increment();
      
      deliveries.put(new Long(deliveryId), new DeliveryRecord(del, consumerId));
      
      if (trace) { log.trace(this + " added delivery " + deliveryId + ": " + del); }
      
      return deliveryId;      
   }      
   
   void acknowledgeTransactionally(List acks, Transaction tx) throws Throwable
   {
      if (trace) { log.trace(this + " acknowledging transactionally " + acks.size() + " for tx: " + tx); }
      
      DeliveryCallback deliveryCallback = (DeliveryCallback)tx.getCallback(this);
      
      if (deliveryCallback == null)
      {
         deliveryCallback = new DeliveryCallback();
         tx.addCallback(deliveryCallback, this);
      }
            
      Iterator iter = acks.iterator();
      
      while (iter.hasNext())
      {
         Ack ack = (Ack)iter.next();
         
         Long id = new Long(ack.getDeliveryId());
           
         DeliveryRecord rec = (DeliveryRecord)deliveries.get(id);
         
         if (rec == null)
         {
            throw new IllegalStateException("Cannot find delivery to acknowledge " + ack);
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
      synchronized(consumers)
      {
         for(Iterator i = consumers.values().iterator(); i.hasNext(); )
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
   } 

   // Protected -----------------------------------------------------        

   // Private -------------------------------------------------------
   
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
      boolean expired = cancel.isExpired() || rec.del.getReference().isExpired();
      
      //Note we check the flag *and* evaluate again, this is because the server value of maxDeliveries
      //might get changed after the client has sent the cancel - and we don't want to end up cancelling
      //back to the original queue
      boolean reachedMaxDeliveryAttempts =
         cancel.isReachedMaxDeliveryAttempts() || cancel.getDeliveryCount() >= maxDeliveryAttempts;
         
      if (!expired && !reachedMaxDeliveryAttempts)
      {
         //Normal cancel back to the queue
         
         rec.del.getReference().setDeliveryCount(cancel.getDeliveryCount());
         
         rec.del.cancel();
      }
      else
      {
         ServerConsumerEndpoint consumer = null;
         
         synchronized (consumers)
         {
            consumer = (ServerConsumerEndpoint)consumers.get(new Integer(rec.consumerId));
         }
         
         if (consumer == null)
         {
            throw new IllegalStateException("Cannot find consumer with id " + rec.consumerId);
         }
         
         if (expired)
         {
            //Sent to expiry queue
            
            this.moveInTransaction(rec.del, consumer.getExpiryQueue());
         }
         else
         {
            //Send to DLQ
            
            this.moveInTransaction(rec.del, consumer.getDLQ());
         }
      }      
      
      return rec.del;
   }      
   
   private void moveInTransaction(Delivery del, Queue queue) throws Throwable
   {
      Transaction tx = tr.createTransaction();
      
      try
      {               
         if (queue != null)
         {                               
            //Need to reset expiration and delivery account
            del.getReference().setExpiration(0);
            del.getReference().getMessage().setExpiration(0);
            del.getReference().setDeliveryCount(0);
            
            queue.handle(null, del.getReference(), tx);
            
            del.acknowledge(tx);           
         }
         else
         {
            log.warn("Cannot move to destination since destination has not been deployed! The message will be removed");
            
            del.acknowledge(tx);
         }             
         
         tx.commit();
         
         if (queue != null)
         {
            queue.deliver(false);
         }
      }
      catch (Throwable t)
      {
         tx.rollback();
         
         throw t;
      } 
   }
   
   
   private void acknowledgeDeliveryInternal(Ack ack) throws Throwable
   {
      if (trace) { log.trace(this + " acknowledging delivery " + ack.getDeliveryId()); }
      
      DeliveryRecord rec = (DeliveryRecord)deliveries.remove(new Long(ack.getDeliveryId()));
      
      if (rec == null)
      {
         throw new IllegalStateException("Cannot find delivery to acknowledge: " + ack.getDeliveryId());
      }
      
      rec.del.acknowledge(null);    
   } 
   
   

   private ConsumerDelegate failoverConsumer(JBossDestination jmsDestination,
                                             String selectorString,
                                             boolean noLocal,  String subscriptionName,
                                             boolean connectionConsumer,
                                             long oldChannelID) throws Exception
   {
      // fail over channel
      if (postOffice.isLocal())
      {
         throw new IllegalStateException("Cannot failover on a non clustered post office!");
      }

      log.debug(this + " failing over consumer");
      
      // this is a Clustered operation... so postOffice here must be Clustered
      Binding binding = ((ClusteredPostOffice)postOffice).getBindingforChannelId(oldChannelID);
      if (binding == null)
      {
         throw new IllegalStateException("Can't find failed over channel " + oldChannelID);
      }

      if (trace)
      {
         long newChannelID =
            binding.getQueue() instanceof RemoteQueueStub ?
               ((RemoteQueueStub)binding.getQueue()).getChannelID() :
               ((PagingFilteredQueue)binding.getQueue()).getChannelID();

         log.trace(this + "failing over from channel " + oldChannelID + " to channel " + newChannelID);
      }

      int consumerID = connectionEndpoint.getServerPeer().getNextObjectID();
      int prefetchSize = connectionEndpoint.getPrefetchSize();
      
      ManagedDestination dest = 
         sp.getDestinationManager().getDestination(jmsDestination.getName(), jmsDestination.isQueue());
      
      if (dest == null)
      {
         throw new IllegalStateException("Cannot find managed destination for dest: " + jmsDestination);
      }
      
      Queue dlqToUse = dest.getDLQ() == null ? defaultDLQ : dest.getDLQ();
      
      Queue expiryQueueToUse = dest.getExpiryQueue() == null ? defaultExpiryQueue : dest.getExpiryQueue();
            
      ServerConsumerEndpoint ep =
         new ServerConsumerEndpoint(consumerID, binding.getQueue(),
                                    binding.getQueue().getName(), this, selectorString, noLocal,
                                    jmsDestination, dlqToUse, expiryQueueToUse);
      
      JMSDispatcher.instance.registerTarget(new Integer(consumerID), new ConsumerAdvised(ep));

      ClientConsumerDelegate stub =
         new ClientConsumerDelegate(consumerID, binding.getQueue().getChannelID(),
                                    prefetchSize, maxDeliveryAttempts);
            
      synchronized (consumers)
      {      
         consumers.put(new Integer(consumerID), ep);
      }
      
      return stub;
   }
   
   private ConsumerDelegate createConsumerDelegateInternal(JBossDestination jmsDestination,
                                                           String selectorString,
                                                           boolean noLocal,
                                                           String subscriptionName,
                                                           boolean isCC) throws Throwable
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      
      if ("".equals(selectorString))
      {
         selectorString = null;
      }
      
      log.debug(this + " creating consumer for " + jmsDestination +
         (selectorString == null ? "" : ", selector '" + selectorString + "'") +
         (subscriptionName == null ? "" : ", subscription '" + subscriptionName + "'") +
         (noLocal ? ", noLocal" : ""));

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
            QueuedExecutor executor = (QueuedExecutor)pool.get();
            
            PagingFilteredQueue q;
            
            if (postOffice.isLocal())
            {
               q = new PagingFilteredQueue(new GUID().toString(), idm.getID(), ms, pm, true, false,
                        executor, selector,
                        mDest.getFullSize(),
                        mDest.getPageSize(),
                        mDest.getDownCacheSize());
               
               binding = postOffice.bindQueue(topicCond, q);
            }
            else
            {
               q = new LocalClusteredQueue(postOffice, nodeId, new GUID().toString(), idm.getID(), ms, pm, true, false,
                        executor, selector, tr,
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
               
               QueuedExecutor executor = (QueuedExecutor)pool.get();
               PagingFilteredQueue q;

               if (postOffice.isLocal())
               {
                  q = new PagingFilteredQueue(name, idm.getID(), ms, pm, true, true,
                                              executor, selector,
                                              mDest.getFullSize(),
                                              mDest.getPageSize(),
                                              mDest.getDownCacheSize());

                  binding = postOffice.bindQueue(topicCond, q);
               }
               else
               {
                  q = new LocalClusteredQueue(postOffice, nodeId, name, idm.getID(),
                                              ms, pm, true, true,
                                              executor, selector, tr,
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
            }
            else
            {
               //Durable sub already exists
               
               if (trace) { log.trace(this + " subscription " + subscriptionName + " already exists"); }
               
               // From javax.jms.Session Javadoc (and also JMS 1.1 6.11.1):
               // A client can change an existing durable subscription by creating a durable
               // TopicSubscriber with the same name and a new topic and/or message selector.
               // Changing a durable subscriber is equivalent to unsubscribing (deleting) the old
               // one and creating a new one.
               
               String filterString = binding.getQueue().getFilter() != null ? binding.getQueue().getFilter().getFilterString() : null;
               
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
                  
                  QueuedExecutor executor = (QueuedExecutor)pool.get();
                  PagingFilteredQueue q;
                  
                  if (postOffice.isLocal())
                  {
                     q = new PagingFilteredQueue(name, idm.getID(), ms, pm, true, true,
                              executor, selector,
                              mDest.getFullSize(),
                              mDest.getPageSize(),
                              mDest.getDownCacheSize());
                     binding = postOffice.bindQueue(topicCond, q);
                  }
                  else
                  {
                     q = new LocalClusteredQueue(postOffice, nodeId, name, idm.getID(), ms, pm, true, true,
                              executor, selector, tr,
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
      
      ServerConsumerEndpoint ep =
         new ServerConsumerEndpoint(consumerID, (PagingFilteredQueue)binding.getQueue(),
                  binding.getQueue().getName(), this, selectorString, noLocal,
                  jmsDestination, dlqToUse, expiryQueueToUse);
      
      JMSDispatcher.instance.registerTarget(new Integer(consumerID), new ConsumerAdvised(ep));
      
      ClientConsumerDelegate stub =
         new ClientConsumerDelegate(consumerID, binding.getQueue().getChannelID(),
                                    prefetchSize, maxDeliveryAttempts);
      
      synchronized (consumers)
      {
         consumers.put(new Integer(consumerID), ep);
      }
         
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
         
         ((Channel)observer).deliver(false);
      }
   }
   
   // Inner classes -------------------------------------------------
   
   /*
    * Holds a record of a delivery - we need to store the consumer id as well
    * hence this class
    * The only reason we need to store the consumer id is that on consumer close, we need to 
    * cancel any deliveries corresponding to that consumer.
    * We can't rely on the cancel being driven from the MessageCallbackHandler since
    * the deliveries may have got lost in transit (ignored) since the consumer might have closed
    * when they were in transit.
    * In such a case we might otherwise end up with the consumer closing but not all it's deliveries being
    * cancelled, which would mean they wouldn't be cancelled until the session is closed which is too late
    */
   private static class DeliveryRecord
   {
      Delivery del;
      
      int consumerId;
      
      DeliveryRecord(Delivery del, int consumerId)
      {
         this.del = del;
         
         this.consumerId = consumerId;
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
