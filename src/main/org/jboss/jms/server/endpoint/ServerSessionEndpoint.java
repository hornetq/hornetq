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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.destination.ManagedDestination;
import org.jboss.jms.server.destination.ManagedQueue;
import org.jboss.jms.server.destination.ManagedTopic;
import org.jboss.jms.server.endpoint.advised.BrowserAdvised;
import org.jboss.jms.server.endpoint.advised.ConsumerAdvised;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.jms.util.MessageQueueNameHelper;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.cluster.LocalClusteredQueue;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.util.id.GUID;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * Concrete implementation of SessionEndpoint.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   private int sessionID;
   
   private boolean closed;

   private ServerConnectionEndpoint connectionEndpoint;

   private Map consumers;
   private Map browsers;

   private PersistenceManager pm;
   private MessageStore ms;
   private DestinationManager dm;
   private IdManager idm;
   private QueuedExecutorPool pool;
   private TransactionRepository tr;
   private PostOffice topicPostOffice;
   private PostOffice queuePostOffice;
   private int nodeId;
   
   
   // Constructors --------------------------------------------------

   protected ServerSessionEndpoint(int sessionID, ServerConnectionEndpoint connectionEndpoint)
      throws Exception
   {
      this.sessionID = sessionID;
      
      this.connectionEndpoint = connectionEndpoint;

      ServerPeer sp = connectionEndpoint.getServerPeer();

      pm = sp.getPersistenceManagerInstance();
      ms = sp.getMessageStore();
      dm = sp.getDestinationManager();
      topicPostOffice = sp.getTopicPostOfficeInstance();
      queuePostOffice = sp.getQueuePostOfficeInstance();
      idm = sp.getChannelIdManager();
      pool = sp.getQueuedExecutorPool();
      nodeId = sp.getServerPeerID();
      tr = sp.getTxRepository();

      consumers = new HashMap();
		browsers = new HashMap();  
   }
   
   // SessionDelegate implementation --------------------------------
   
	public ConsumerDelegate createConsumerDelegate(JBossDestination jmsDestination,
                                                  String selectorString,
                                                  boolean noLocal,
                                                  String subscriptionName,
                                                  boolean isCC) throws JMSException
   {
      try
      {
         if (closed)
         {
            throw new IllegalStateException("Session is closed");
         }
         
         if ("".equals(selectorString))
         {
            selectorString = null;
         }
         
         log.debug("creating consumer for " + jmsDestination + ", selector " + selectorString + ", " + (noLocal ? "noLocal, " : "") + "subscription " + subscriptionName);
   
         ManagedDestination mDest = dm.getDestination(jmsDestination.getName(), jmsDestination.isQueue());
         
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
            if (subscriptionName == null)
            {
               // non-durable subscription
               if (log.isTraceEnabled()) { log.trace("creating new non-durable subscription on " + jmsDestination); }
                     
               //Create the non durable sub
               QueuedExecutor executor = (QueuedExecutor)pool.get();
               
               PagingFilteredQueue q;
               
               if (topicPostOffice.isLocal())
               {
                  q = new PagingFilteredQueue(new GUID().toString(), idm.getId(), ms, pm, true, false,                              
                                              executor, selector,
                                              mDest.getFullSize(),
                                              mDest.getPageSize(),
                                              mDest.getDownCacheSize());
                  
                  binding = topicPostOffice.bindQueue(jmsDestination.getName(), q);      
               }
               else
               {
                  q = new LocalClusteredQueue(topicPostOffice, nodeId, new GUID().toString(), idm.getId(), ms, pm, true, false,                              
                                              executor, selector, tr,
                                              mDest.getFullSize(),
                                              mDest.getPageSize(),
                                              mDest.getDownCacheSize());
                  
                  ClusteredPostOffice cpo = (ClusteredPostOffice)topicPostOffice;
                  
                  if (mDest.isClustered())
                  {                  
                     binding = cpo.bindClusteredQueue(jmsDestination.getName(), (LocalClusteredQueue)q);  
                  }
                  else
                  {
                     binding = cpo.bindQueue(jmsDestination.getName(), q); 
                  }
               }       
            }
            else
            {
               if (jmsDestination.isTemporary())
               {
                  throw new InvalidDestinationException("Cannot create a durable subscription on a temporary topic");
               }
               
               // we have a durable subscription, look it up
               String clientID = connectionEndpoint.getClientID();
               if (clientID == null)
               {
                  throw new JMSException("Cannot create durable subscriber without a valid client ID");
               }
               
               // See if there any bindings with the same client_id.subscription_name name
               
               String name = MessageQueueNameHelper.createSubscriptionName(clientID, subscriptionName);
               
               binding = topicPostOffice.getBindingForQueueName(name);
                  
               if (binding == null)
               {
                  //Does not already exist
                  
                  if (trace) { log.trace("creating new durable subscription on " + jmsDestination); }
                  
                  QueuedExecutor executor = (QueuedExecutor)pool.get();
                  PagingFilteredQueue q;
                  
                  if (topicPostOffice.isLocal())
                  {
                     q = new PagingFilteredQueue(name, idm.getId(), ms, pm, true, true,                              
                                                 executor, selector,
                                                 mDest.getFullSize(),
                                                 mDest.getPageSize(),
                                                 mDest.getDownCacheSize());
                     
                     binding = topicPostOffice.bindQueue(jmsDestination.getName(), q);      
                  }
                  else
                  {
                     q = new LocalClusteredQueue(topicPostOffice, nodeId, name, idm.getId(), ms, pm, true, true,                              
                                                 executor, selector, tr,
                                                 mDest.getFullSize(),
                                                 mDest.getPageSize(),
                                                 mDest.getDownCacheSize());
                     
                     ClusteredPostOffice cpo = (ClusteredPostOffice)topicPostOffice;
                     
                     if (mDest.isClustered())
                     {                     
                        binding = cpo.bindClusteredQueue(jmsDestination.getName(), (LocalClusteredQueue)q);
                     }
                     else
                     {
                        binding = cpo.bindQueue(jmsDestination.getName(), q);
                     }
                  }                 
               }
               else
               {
                  //Durable sub already exists
                  
                  if (trace) { log.trace("subscription " + subscriptionName + " already exists"); }
                  
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
   
                  boolean topicChanged = !binding.getCondition().equals(jmsDestination.getName());
                  
                  if (log.isTraceEnabled()) { log.trace("topic " + (topicChanged ? "has" : "has NOT") + " changed"); }
                  
                  if (selectorChanged || topicChanged)
                  {    
                     if (trace) { log.trace("topic or selector changed so deleting old subscription"); }
   
                     // Unbind the durable subscription
                     
                     if (mDest.isClustered() && !topicPostOffice.isLocal())
                     {
                        ClusteredPostOffice cpo = (ClusteredPostOffice)topicPostOffice;
                        
                        cpo.unbindClusteredQueue(name);
                     }
                     else
                     {         
                        topicPostOffice.unbindQueue(name);
                     }
                        
                     // create a fresh new subscription
                     
                     QueuedExecutor executor = (QueuedExecutor)pool.get();
                     PagingFilteredQueue q;
                     
                     if (topicPostOffice.isLocal())
                     {
                        q = new PagingFilteredQueue(name, idm.getId(), ms, pm, true, true,                              
                                                    executor, selector,
                                                    mDest.getFullSize(),
                                                    mDest.getPageSize(),
                                                    mDest.getDownCacheSize());
                        binding = topicPostOffice.bindQueue(jmsDestination.getName(), q);      
                     }
                     else
                     {
                        q = new LocalClusteredQueue(topicPostOffice, nodeId, name, idm.getId(), ms, pm, true, true,                              
                                                    executor, selector, tr,
                                                    mDest.getFullSize(),
                                                    mDest.getPageSize(),
                                                    mDest.getDownCacheSize());
                        
                        ClusteredPostOffice cpo = (ClusteredPostOffice)topicPostOffice;
                        
                        if (mDest.isClustered())
                        {                        
                           binding = cpo.bindClusteredQueue(jmsDestination.getName(), (LocalClusteredQueue)q);    
                        }
                        else
                        {
                           binding = cpo.bindQueue(jmsDestination.getName(), (LocalClusteredQueue)q);
                        }
                     }    
                  }               
               }
            }
         }
         else
         {
            //Consumer on a jms queue
            
            //Let's find the binding
            binding = queuePostOffice.getBindingForQueueName(jmsDestination.getName());
            
            if (binding == null)
            {
               throw new IllegalStateException("Cannot find binding for jms queue: " + jmsDestination.getName());
            }
         }
         
         int prefetchSize = connectionEndpoint.getPrefetchSize();
         
         ServerConsumerEndpoint ep =
            new ServerConsumerEndpoint(consumerID, (PagingFilteredQueue)binding.getQueue(), binding.getQueue().getName(),
                                       this, selectorString, noLocal, jmsDestination, prefetchSize);
          
         JMSDispatcher.instance.registerTarget(new Integer(consumerID), new ConsumerAdvised(ep));
                     
         ClientConsumerDelegate stub = new ClientConsumerDelegate(consumerID, prefetchSize);
                       
         putConsumerEndpoint(consumerID, ep); // caching consumer locally
         
         connectionEndpoint.getServerPeer().putConsumerEndpoint(consumerID, ep); // cachin consumer in server peer
         
         log.debug("created and registered " + ep);
   
         return stub;
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
         
         Binding binding = queuePostOffice.getBindingForQueueName(jmsDestination.getName());
         
   	   int browserID = connectionEndpoint.getServerPeer().getNextObjectID();
   	   
   	   ServerBrowserEndpoint ep =
   	      new ServerBrowserEndpoint(this, browserID, (PagingFilteredQueue)binding.getQueue(), messageSelector);
   	   
   	   putBrowserDelegate(browserID, ep);
   	   
         JMSDispatcher.instance.registerTarget(new Integer(browserID), new BrowserAdvised(ep));
   	   
   	   ClientBrowserDelegate stub = new ClientBrowserDelegate(browserID);
   	   
         log.debug("created and registered " + ep);
   
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
         if (closed)
         {
            throw new IllegalStateException("Session is already closed");
         }
         
         if (trace) log.trace(this + " close()");
               
         // clone to avoid ConcurrentModificationException
         HashSet consumerSet = new HashSet(consumers.values());
         
         for(Iterator i = consumerSet.iterator(); i.hasNext(); )
         {
            ((ServerConsumerEndpoint)i.next()).remove();
         }           
         
         connectionEndpoint.removeSessionDelegate(sessionID);
         
         JMSDispatcher.instance.unregisterTarget(new Integer(sessionID));
         
         closed = true;
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

   public boolean isClosed()
   {
      return closed;
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
   
   public void acknowledgeBatch(List ackInfos) throws JMSException
   {      
      try
      {
         Iterator iter = ackInfos.iterator();
         
         while (iter.hasNext())
         {
            AckInfo ackInfo = (AckInfo)iter.next();
            
            acknowledgeInternal(ackInfo);
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " acknowledgeBatch");
      }
   }
   
   public void acknowledge(AckInfo ackInfo) throws JMSException
   {
      try
      {
         acknowledgeInternal(ackInfo);      
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " acknowledge");
      }
   }      
         
   public void cancelDeliveries(List ackInfos) throws JMSException
   {
      try
      {
         // deliveries must be cancelled in reverse order
          
         Set consumers = new HashSet();
         
         for (int i = ackInfos.size() - 1; i >= 0; i--)
         {
            AckInfo ack = (AckInfo)ackInfos.get(i);
            
            // We look in the global map since the message might have come from connection consumer
            ServerConsumerEndpoint consumer =
               this.connectionEndpoint.getConsumerEndpoint(ack.getConsumerID());
   
            if (consumer == null)
            {
               throw new IllegalArgumentException("Cannot find consumer id: " + ack.getConsumerID());
            }
            
            consumer.cancelDelivery(new Long(ack.getMessageID()));
            consumers.add(consumer);
         }
         
         // need to prompt delivery for all consumers
         
         for(Iterator i = consumers.iterator(); i.hasNext(); )
         {
            ServerConsumerEndpoint consumer = (ServerConsumerEndpoint)i.next();
            consumer.promptDelivery();
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " cancelDeliveries");
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
               new PagingFilteredQueue(dest.getName(), idm.getId(), ms, pm, true, false,
                                       executor, null, fullSize, pageSize, downCacheSize);
            
            
            
            //Make a binding for this queue
            queuePostOffice.bindQueue(dest.getName(), q);  
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
            queuePostOffice.unbindQueue(dest.getName());
         }
         else
         {
            //Topic            
            Collection bindings = topicPostOffice.listBindingsForCondition(dest.getName());
            
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
         
         Binding binding = topicPostOffice.getBindingForQueueName(queueName);
         
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
         ManagedDestination mDest = dm.getDestination(binding.getCondition(), false);
         
         //Unbind it
    
         if (mDest.isClustered() && !topicPostOffice.isLocal())
         {
            ClusteredPostOffice cpo = (ClusteredPostOffice)topicPostOffice;
            
            cpo.unbindClusteredQueue(queueName);
         }
         else
         {         
            topicPostOffice.unbindQueue(queueName);
         }
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " unsubscribe");
      }
   }
    
   // Public --------------------------------------------------------
   
   public ServerConnectionEndpoint getConnectionEndpoint()
   {
      return connectionEndpoint;
   }

   public String toString()
   {
      return "SessionEndpoint[" + sessionID + "]";
   }

   // Package protected ---------------------------------------------

   /**
    * @return a Set<Integer>
    */
   Set getConsumerEndpointIDs()
   {
      return consumers.keySet();
   }
   
   // Protected -----------------------------------------------------
   
   protected void acknowledgeInternal(AckInfo ackInfo) throws Throwable
   {
      //If the message was delivered via a connection consumer then the message needs to be acked
      //via the original consumer that was used to feed the connection consumer - which
      //won't be one of the consumers of this session
      //Therefore we always look in the global map of consumers held in the server peer
      ServerConsumerEndpoint consumer = this.connectionEndpoint.getConsumerEndpoint(ackInfo.getConsumerID());

      if (consumer == null)
      {
         throw new IllegalArgumentException("Cannot find consumer id: " + ackInfo.getConsumerID());
      }
      
      consumer.acknowledge(ackInfo.getMessageID());
   }
   
   protected ServerConsumerEndpoint putConsumerEndpoint(int consumerID, ServerConsumerEndpoint d)
   {
      if (trace) { log.trace(this + " caching consumer " + consumerID); }
      return (ServerConsumerEndpoint)consumers.put(new Integer(consumerID), d);
   }

   protected ServerConsumerEndpoint getConsumerEndpoint(int consumerID)
   {
      return (ServerConsumerEndpoint)consumers.get(new Integer(consumerID));
   }
   
   protected ServerConsumerEndpoint removeConsumerEndpoint(int consumerID)
   {
      if (trace) { log.trace(this + " removing consumer " + consumerID + " from cache"); }
      return (ServerConsumerEndpoint)consumers.remove(new Integer(consumerID));
   }
   
   protected ServerBrowserEndpoint putBrowserDelegate(int browserID, ServerBrowserEndpoint sbd)
   {
      return (ServerBrowserEndpoint)browsers.put(new Integer(browserID), sbd);
   }
   
   protected ServerBrowserEndpoint getBrowserDelegate(int browserID)
   {
      return (ServerBrowserEndpoint)browsers.get(new Integer(browserID));
   }
   
   protected ServerBrowserEndpoint removeBrowserDelegate(int browserID)
   {
      return (ServerBrowserEndpoint)browsers.remove(new Integer(browserID));
   }

   /**
    * Starts this session's Consumers
    */
   protected void setStarted(boolean s) throws Throwable
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

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
