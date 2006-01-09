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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.jboss.aop.Dispatcher;
import org.jboss.jms.client.delegate.ClientBrowserDelegate;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.client.delegate.ClientProducerDelegate;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.server.DestinationManagerImpl;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.StateManager;
import org.jboss.jms.server.endpoint.advised.BrowserAdvised;
import org.jboss.jms.server.endpoint.advised.ConsumerAdvised;
import org.jboss.jms.server.endpoint.advised.ProducerAdvised;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.local.DurableSubscription;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.local.Subscription;
import org.jboss.messaging.core.local.Topic;
import org.jboss.messaging.util.Util;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.util.id.GUID;

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

   protected String sessionID;
   
   protected ServerConnectionEndpoint connectionEndpoint;
      
   protected Map producers;
   
   protected Map consumers;
   
	protected Map browsers;
	
	protected int acknowledgmentMode;

   protected ServerPeer serverPeer;
   
   private InvokerCallbackHandler callbackHandler;
   
   private boolean closed;
   
   // Constructors --------------------------------------------------

   ServerSessionEndpoint(String sessionID, ServerConnectionEndpoint connectionEndpoint,
								 int acknowledgmentMode)
   {
      this.sessionID = sessionID;
      
		this.acknowledgmentMode = acknowledgmentMode;
      
      this.connectionEndpoint = connectionEndpoint;  
      
      producers = new HashMap();
      
      consumers = new HashMap();
      
		browsers = new HashMap();
      
      serverPeer = connectionEndpoint.getServerPeer();
   }

   // SessionDelegate implementation --------------------------------

   public ProducerDelegate createProducerDelegate(Destination jmsDestination) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
            
      // look-up destination
      DestinationManagerImpl dm = serverPeer.getDestinationManager();
      if (jmsDestination != null)
      {
         if (dm.getCoreDestination(jmsDestination) == null)
         {
            throw new InvalidDestinationException("No such destination: " + jmsDestination);
         }
      }
     
      String producerID = new GUID().toString();
      
      // create the corresponding server-side producer endpoint and register it with this
      // session endpoint instance
      ServerProducerEndpoint ep = new ServerProducerEndpoint(producerID, jmsDestination, this);
      
      putProducerDelegate(producerID, ep);
      ProducerAdvised producerAdvised = new ProducerAdvised(ep);
      Dispatcher.singleton.registerTarget(producerID, producerAdvised);
         
      ClientProducerDelegate d = new ClientProducerDelegate(producerID);
      
      log.debug("created and registered " + ep);

      return d;
   }

	public ConsumerDelegate createConsumerDelegate(Destination jmsDestination,
                                                  String selector,
                                                  boolean noLocal,
                                                  String subscriptionName,
                                                  boolean isCC) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      
      if ("".equals(selector))
      {
         selector = null;
      }
      
      JBossDestination d = (JBossDestination)jmsDestination;
      
      if (log.isTraceEnabled()) { log.trace("creating ServerConsumerDelegate for dest:" + d.getName() +
            " selector:" + selector + " noLocal:" + noLocal + " subName:" + subscriptionName); }
            
      if (d.isTemporary())
      {
         //Can only create a consumer for a temporary destination on the same connection
         //that created it
         if (!this.connectionEndpoint.temporaryDestinations.contains(d))
         {
            throw new IllegalStateException("Cannot create a message consumer " +
                  "on a different connection to that which created the temporary destination");
         }
      }
      
      // look-up destination
      DestinationManagerImpl dm = serverPeer.getDestinationManager();
    
      Distributor destination = dm.getCoreDestination(jmsDestination);
      if (destination == null)
      {
         throw new InvalidDestinationException("No such destination: " + jmsDestination);
      }
          
      String consumerID = new GUID().toString();
     
      if (callbackHandler == null)
      {
         throw new JMSException("null callback handler");
      }
      
      Subscription subscription = null;
      if (d.isTopic())
      {
         MessageStore ms = connectionEndpoint.getServerPeer().getMessageStore();
         Topic topic = (Topic)destination;

         if (subscriptionName != null)
         {
            //Look-up the durable subscription
         
            String clientID = connectionEndpoint.getClientID();
            if (clientID == null)
            {
               throw new JMSException("Cannot create durable subscriber without having set client ID");
            }
                     
            //It's a durable subscription - have we already got one with that name?
            StateManager stateManager = serverPeer.getStateManager();
                       
            subscription = stateManager.getDurableSubscription(clientID, subscriptionName);                        
         
            if (subscription != null)
            {
               
               if (log.isTraceEnabled()) { log.trace("Subscription with name " + subscriptionName +
                                          " already exists"); }                        
                                       
               /* From javax.jms.Session Javadoc:
                * 
                * A client can change an existing durable subscription by creating a durable
                * TopicSubscriber with the same name and a new topic and/or message selector.
                * Changing a durable subscriber is equivalent to unsubscribing (deleting)
                * the old one and creating a new one.
                */

               //Has the selector changed?
               boolean selectorChanged = (selector == null && subscription.getSelector() != null) ||
                         (subscription.getSelector() == null && selector != null) ||
                         (subscription.getSelector() != null && selector != null && 
                          !subscription.getSelector().equals(selector));
            
               if (log.isTraceEnabled()) { log.trace("Selector has changed? " + selectorChanged); }
               
               //Has the Topic changed?               
               boolean topicChanged = subscription.getTopic() != destination;
               
               if (log.isTraceEnabled()) { log.trace("Topic has changed? " + topicChanged); }
              
               if (selectorChanged || topicChanged)
               {
                  if (log.isTraceEnabled()) { log.trace("Changed so deleting old subscription"); }
                  
                  boolean removed = this.serverPeer.getStateManager().
                     removeDurableSubscription(this.connectionEndpoint.clientID, subscriptionName);
   
                  if (!removed)
                  {
                     throw new InvalidDestinationException("Cannot find durable subscription with name " +
                        subscriptionName + " to unsubscribe");
                  }
   
                  subscription.unsubscribe();
                  subscription = null;
               }
            }
         
         
            if (subscription == null)
            {
               //Create a new durable subscription
                        
               if (log.isTraceEnabled()) { log.trace("Creating new durable subscription on topic " + destination); }
             
               subscription = stateManager.createDurableSubscription(d.getName(), clientID, subscriptionName, selector);
            }
         } //durable sub
         else
         {
            //Non durable subscription
            
            if (log.isTraceEnabled()) { log.trace("Creating new non-durable subscription on topic " + destination); }
            
            subscription = new Subscription(topic, selector, ms);    
         }
      } 
      
      ServerConsumerEndpoint ep =
         new ServerConsumerEndpoint(consumerID,
                                    subscription == null ? (Channel)destination : subscription,
                                    callbackHandler, this, selector, noLocal);
       
      Dispatcher.singleton.registerTarget(consumerID, new ConsumerAdvised(ep));
         
      ClientConsumerDelegate stub = new ClientConsumerDelegate(consumerID);
      
      if (subscription != null)
      {
         subscription.subscribe();
      }
            
      putConsumerDelegate(consumerID, ep);
      
      connectionEndpoint.consumers.put(consumerID, ep);

      log.debug("created and registered " + ep);

      return stub;
   }
	
	public BrowserDelegate createBrowserDelegate(Destination jmsDestination, String messageSelector)
	   throws JMSException
	{
	   if (closed)
	   {
	      throw new IllegalStateException("Session is closed");
	   }
	   
	   if (jmsDestination == null)
	   {
	      throw new InvalidDestinationException("null destination");
	   }
	   
	   // look-up destination
	   DestinationManagerImpl dm = serverPeer.getDestinationManager();
	   
	   Distributor destination = dm.getCoreDestination(jmsDestination);
	   
	   if (destination == null)
	   {
	      throw new InvalidDestinationException("No such destination: " + jmsDestination);
	   }
	   
	   if (!(destination instanceof Queue))
	   {
	      throw new IllegalStateException("Cannot browse a topic");
	   }
	   
	   String browserID = new GUID().toString();
	   
	   ServerBrowserEndpoint ep =
	      new ServerBrowserEndpoint(this, browserID, (Channel)destination, messageSelector);
	   
	   putBrowserDelegate(browserID, ep);
	   
	   Dispatcher.singleton.registerTarget(browserID, new BrowserAdvised(ep));
	   
	   ClientBrowserDelegate stub = new ClientBrowserDelegate(browserID);
	   
      log.debug("created and registered " + ep);

	   return stub;
	}

   public javax.jms.Queue createQueue(String name) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      
      DestinationManagerImpl dm = serverPeer.getDestinationManager();
      
      Distributor coreDestination = dm.getCoreDestination(true, name);

      if (coreDestination == null)
      {
         throw new JMSException("There is no administratively defined queue with name:" + name);
      }

      if (coreDestination instanceof Topic)
      {
         throw new JMSException("A topic with the same name exists already");
      }

      return new JBossQueue(name);
   }

   public javax.jms.Topic createTopic(String name) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      
      DestinationManagerImpl dm = serverPeer.getDestinationManager();
      Distributor coreDestination = dm.getCoreDestination(false, name);

      if (coreDestination == null)
      {
         throw new JMSException("There is no administratively defined topic with name:" + name);
      }

      if (coreDestination instanceof Queue)
      {
         throw new JMSException("A queue with the same name exists already");
      }

      return new JBossTopic(name);
   }

   public void close() throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is already closed");
      }
      
      if (log.isTraceEnabled()) log.trace("close()");
            
      //Clone to avoid ConcurrentModificationException
      HashSet consumerSet = new HashSet(consumers.values());
      
      for(Iterator i = consumerSet.iterator(); i.hasNext(); )
      {
         ((ServerConsumerEndpoint)i.next()).remove();
      }
      
      HashSet producerSet = new HashSet(producers.values());
      
      for(Iterator i = producerSet.iterator(); i.hasNext(); )
      {
         ((ServerProducerEndpoint)i.next()).close();
      }
      
      this.connectionEndpoint.sessions.remove(this.sessionID);
      
      Dispatcher.singleton.unregisterTarget(this.sessionID);
      
      closed = true;
   }
   
   public void closing() throws JMSException
   {
      if (log.isTraceEnabled()) log.trace("closing (noop)");

      //Currently does nothing
   }
   
   /**
    * Cancel all the deliveries in the session
    */
	public void cancelDeliveries() throws JMSException
	{
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      
      if (log.isTraceEnabled()) { log.trace("Cancelling messages"); }
            
		for(Iterator i = this.consumers.values().iterator(); i.hasNext(); )
		{
			ServerConsumerEndpoint scd = (ServerConsumerEndpoint)i.next();
         scd.cancelAllDeliveries();
		}     
	}
	
   public void acknowledge() throws JMSException
   {

      Iterator iter = consumers.values().iterator();
      while (iter.hasNext())
      {
         ServerConsumerEndpoint consumer = (ServerConsumerEndpoint)iter.next();
         consumer.acknowledgeAll();
      }
   }

   public void addTemporaryDestination(Destination dest) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      JBossDestination d = (JBossDestination)dest;
      if (!d.isTemporary())
      {
         throw new InvalidDestinationException("Destination:" + dest + " is not a temporary destination");
      }
      this.connectionEndpoint.temporaryDestinations.add(dest);
      serverPeer.getDestinationManager().addTemporaryDestination(dest);
   }
   
   public void deleteTemporaryDestination(Destination dest) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      JBossDestination d = (JBossDestination)dest;
      
      if (!d.isTemporary())
      {
         throw new InvalidDestinationException("Destination:" + dest + " is not a temporary destination");
      }
      
      //It is illegal to delete a temporary destination if there any active consumers on it
      Distributor destination = serverPeer.getDestinationManager().getCoreDestination(dest);
      
      if (destination == null)
      {
         throw new InvalidDestinationException("Destination:" + dest + " does not exist");         
      }
      
   
      if (destination instanceof Queue)
      {
         if (destination.iterator().hasNext())
         {
            throw new IllegalStateException("Cannot delete temporary destination, since it has active consumer(s)");
         }
      }
      else if (destination instanceof Topic)
      {
         Iterator iter = destination.iterator();
         while (iter.hasNext())
         {
            Subscription sub = (Subscription)iter.next();
            if (sub.iterator().hasNext())
            {
               throw new IllegalStateException("Cannot delete temporary destination, since it has active consumer(s)");
            }
         }
      }
      
      serverPeer.getDestinationManager().removeTemporaryDestination(dest);
      this.connectionEndpoint.temporaryDestinations.remove(dest);
   }
   
   public void unsubscribe(String subscriptionName) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      if (subscriptionName == null)
      {
         throw new InvalidDestinationException("Destination is null");
      }
      DurableSubscription subscription = this.serverPeer.getStateManager().getDurableSubscription(this.connectionEndpoint.clientID, subscriptionName);

      if (subscription == null)
      {
         throw new InvalidDestinationException("Cannot find durable subscription with name " +
                                               subscriptionName + " to unsubscribe");
      }
      
      boolean removed = 
         this.serverPeer.getStateManager().removeDurableSubscription(this.connectionEndpoint.clientID, subscriptionName);
      
      if (!removed)
      {
         throw new JMSException("Failed to remove durable subscription");
      }

      
      subscription.unsubscribe();
      
      try
      {
         this.serverPeer.getPersistenceManager().removeAllMessageData(subscription.getChannelID());
      }
      catch (Exception e)
      {
         log.error("Failed to remove message data", e);
         throw new IllegalStateException("Failed to remove message data");
      }
      
   }     
   
   // Public --------------------------------------------------------

   protected ServerProducerEndpoint putProducerDelegate(String producerID, ServerProducerEndpoint d)
   {
      return (ServerProducerEndpoint)producers.put(producerID, d);
   }

   public ServerProducerEndpoint getProducerDelegate(String producerID)
   {
      return (ServerProducerEndpoint)producers.get(producerID);
   }

   protected ServerConsumerEndpoint putConsumerDelegate(String consumerID, ServerConsumerEndpoint d)
   {
      return (ServerConsumerEndpoint)consumers.put(consumerID, d);
   }

   public ServerConsumerEndpoint getConsumerDelegate(String consumerID)
   {
      return (ServerConsumerEndpoint)consumers.get(consumerID);
   }
	
	protected ServerBrowserEndpoint putBrowserDelegate(String browserID, ServerBrowserEndpoint sbd)
   {
      return (ServerBrowserEndpoint)browsers.put(browserID, sbd);
   }
	
	public ServerBrowserEndpoint getBrowserDelegate(String browserID)
   {
      return (ServerBrowserEndpoint)browsers.get(browserID);
   }

   public ServerConnectionEndpoint getConnectionEndpoint()
   {
      return connectionEndpoint;
   }

   /**
    * IoC
    */
   public void setCallbackHandler(InvokerCallbackHandler callbackHandler)
   {
      this.callbackHandler = callbackHandler;
   }

   public String toString()
   {
      return "SessionEndpoint[" + Util.guidToString(sessionID) + "]";
   }

   // Package protected ---------------------------------------------
   
   /**
    * Starts this session's Consumers
    */
   void setStarted(boolean s)
   {
      synchronized(consumers)
      {
         for(Iterator i = consumers.values().iterator(); i.hasNext(); )
         {
            ((ServerConsumerEndpoint)i.next()).setStarted(s);

         }
      }
   }   

   // Protected -----------------------------------------------------


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
