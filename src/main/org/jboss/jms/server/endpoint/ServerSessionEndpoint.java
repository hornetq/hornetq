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
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.advised.BrowserAdvised;
import org.jboss.jms.server.endpoint.advised.ConsumerAdvised;
import org.jboss.jms.server.plugin.contract.ChannelMapper;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.CoreDestination;
import org.jboss.messaging.core.local.CoreDurableSubscription;
import org.jboss.messaging.core.local.CoreSubscription;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;

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

   private Map consumers;
   
   private Map browsers;

   private ServerConnectionEndpoint connectionEndpoint;

   private ChannelMapper cm;
   
   private PersistenceManager pm;
   
   private MessageStore ms;


   // Constructors --------------------------------------------------

   protected ServerSessionEndpoint(int sessionID, ServerConnectionEndpoint connectionEndpoint)
   {
      this.sessionID = sessionID;
      
      this.connectionEndpoint = connectionEndpoint;

      ServerPeer sp = connectionEndpoint.getServerPeer();

      cm = sp.getChannelMapperDelegate();
      pm = sp.getPersistenceManagerDelegate();
      ms = sp.getMessageStoreDelegate();

      consumers = new HashMap();
		browsers = new HashMap();
   }

   // SessionDelegate implementation --------------------------------

	public ConsumerDelegate createConsumerDelegate(JBossDestination jmsDestination,
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
      
      log.debug("creating consumer for " + jmsDestination + ", selector " + selector + ", " + (noLocal ? "noLocal, " : "") + "subscription " + subscriptionName);

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
      
      CoreDestination coreDestination = cm.getCoreDestination(jmsDestination);
      if (coreDestination == null)
      {
         throw new InvalidDestinationException("No such destination: " + jmsDestination);
      }
          
      int consumerID = connectionEndpoint.getServerPeer().getNextObjectID();
     
      CoreSubscription subscription = null;

      if (jmsDestination.isTopic())
      {
         if (subscriptionName == null)
         {
            // non-durable subscription
            if (log.isTraceEnabled()) { log.trace("creating new non-durable subscription on " + coreDestination); }
            subscription = cm.createSubscription(jmsDestination.getName(), selector, noLocal, ms, pm);
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

            subscription = cm.getDurableSubscription(clientID, subscriptionName, ms, pm);

            if (subscription == null)
            {
               if (trace) { log.trace("creating new durable subscription on " + coreDestination); }
               subscription = cm.createDurableSubscription(jmsDestination.getName(),
                                                           clientID,
                                                           subscriptionName,
                                                           selector,
                                                           noLocal,
                                                           ms,
                                                           pm);
            }
            else
            {
               if (trace) { log.trace("subscription " + subscriptionName + " already exists"); }

               // From javax.jms.Session Javadoc (and also JMS 1.1 6.11.1):
               // A client can change an existing durable subscription by creating a durable
               // TopicSubscriber with the same name and a new topic and/or message selector.
               // Changing a durable subscriber is equivalent to unsubscribing (deleting) the old
               // one and creating a new one.

               boolean selectorChanged =
                  (selector == null && subscription.getSelector() != null) ||
                  (subscription.getSelector() == null && selector != null) ||
                  (subscription.getSelector() != null && selector != null &&
                  !subscription.getSelector().equals(selector));
               
               if (trace) { log.trace("selector " + (selectorChanged ? "has" : "has NOT") + " changed"); }

               boolean topicChanged =  subscription.getTopic().getId() != coreDestination.getId();
               
               if (log.isTraceEnabled()) { log.trace("topic " + (topicChanged ? "has" : "has NOT") + " changed"); }
               
               boolean noLocalChanged = noLocal != subscription.isNoLocal();

               if (selectorChanged || topicChanged || noLocalChanged)
               {
                  if (trace) { log.trace("topic or selector or noLocal changed so deleting old subscription"); }

                  boolean removed =
                     cm.removeDurableSubscription(connectionEndpoint.getClientID(), subscriptionName);

                  if (!removed)
                  {
                     throw new InvalidDestinationException("Cannot find durable subscription " +
                                                           subscriptionName + " to unsubscribe");
                  }

                  subscription.unsubscribe();

                  // create a fresh new subscription
                  subscription = cm.createDurableSubscription(jmsDestination.getName(),
                                                              clientID,
                                                              subscriptionName,
                                                              selector,
                                                              noLocal,
                                                              ms,
                                                              pm);
               }
            }
         }
      }
      
      ServerConsumerEndpoint ep =
         new ServerConsumerEndpoint(consumerID,
                                    subscription == null ? (Channel)coreDestination : subscription,
                                    this, selector, noLocal);
       
      JMSDispatcher.instance.registerTarget(new Integer(consumerID), new ConsumerAdvised(ep));
         
      ClientConsumerDelegate stub = new ClientConsumerDelegate(consumerID);
      
      if (subscription != null)
      {
         subscription.subscribe();
      }
            
      putConsumerDelegate(consumerID, ep);
      
      connectionEndpoint.putConsumerDelegate(consumerID, ep);
      
      log.debug("created and registered " + ep);

      return stub;
   }
	
	public BrowserDelegate createBrowserDelegate(JBossDestination jmsDestination, String messageSelector)
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
	   
	   CoreDestination destination = cm.getCoreDestination(jmsDestination);
	   
	   if (destination == null)
	   {
	      throw new InvalidDestinationException("No such destination: " + jmsDestination);
	   }
	   
	   if (!(destination instanceof Queue))
	   {
	      throw new IllegalStateException("Cannot browse a topic");
	   }
	   
	   int browserID = connectionEndpoint.getServerPeer().getNextObjectID();
	   
	   ServerBrowserEndpoint ep =
	      new ServerBrowserEndpoint(this, browserID, (Channel)destination, messageSelector);
	   
	   putBrowserDelegate(browserID, ep);
	   
      JMSDispatcher.instance.registerTarget(new Integer(browserID), new BrowserAdvised(ep));
	   
	   ClientBrowserDelegate stub = new ClientBrowserDelegate(browserID);
	   
      log.debug("created and registered " + ep);

	   return stub;
	}

   public JBossQueue createQueue(String name) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      
      CoreDestination coreDestination = cm.getCoreDestination(new JBossQueue(name));

      if (coreDestination == null)
      {
         throw new JMSException("There is no administratively defined queue with name:" + name);
      }

      return new JBossQueue(name);
   }

   public JBossTopic createTopic(String name) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }
      
      CoreDestination coreDestination = cm.getCoreDestination(new JBossTopic(name));

      if (coreDestination == null)
      {
         throw new JMSException("There is no administratively defined topic with name:" + name);
      }

      return new JBossTopic(name);
   }

   public void close() throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is already closed");
      }
      
      if (trace) log.trace("close()");
            
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
   
   public void closing() throws JMSException
   {
      //Currently does nothing
      if (trace) log.trace("closing (noop)");
   }
   
   public void send(JBossMessage message) throws JMSException
   {
      connectionEndpoint.sendMessage(message, null);
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
      
      if (trace) { log.trace("Cancelling messages"); }
            
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

   public void addTemporaryDestination(JBossDestination dest) throws JMSException
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
      
      //FIXME - Params should not be hardcoded
      cm.deployCoreDestination(dest.isQueue(), dest.getName(), ms, pm, 50000, 1000, 1000);
   }
   
   public void deleteTemporaryDestination(JBossDestination dest) throws JMSException
   {
      if (closed)
      {
         throw new IllegalStateException("Session is closed");
      }

      if (!dest.isTemporary())
      {
         throw new InvalidDestinationException("Destination:" + dest + " is not a temporary destination");
      }
      
      //It is illegal to delete a temporary destination if there any active consumers on it
      CoreDestination destination = cm.getCoreDestination(dest);
      
      if (destination == null)
      {
         throw new InvalidDestinationException("Destination:" + dest + " does not exist");         
      }
      
      if (dest instanceof JBossQueue)
      {
         if (destination.iterator().hasNext())
         {
            throw new IllegalStateException("Cannot delete temporary destination, since it has active consumer(s)");
         }
      }
      else if (dest instanceof JBossTopic)
      {
         Iterator iter = destination.iterator();
         while (iter.hasNext())
         {
            CoreSubscription sub = (CoreSubscription)iter.next();
            if (sub.iterator().hasNext())
            {
               throw new IllegalStateException("Cannot delete temporary destination, since it has active consumer(s)");
            }
         }
      }
      
      cm.undeployCoreDestination(dest.isQueue(), dest.getName());
      connectionEndpoint.removeTemporaryDestination(dest);
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

      String clientID = connectionEndpoint.getClientID();

      if (clientID == null)
      {
         throw new JMSException("null clientID on connection");
      }

      CoreDurableSubscription subscription =
         cm.getDurableSubscription(clientID, subscriptionName, ms, pm);

      if (subscription == null)
      {
         throw new InvalidDestinationException("Cannot find durable subscription with name " +
                                               subscriptionName + " to unsubscribe");
      }
      
      boolean removed =
         cm.removeDurableSubscription(connectionEndpoint.getClientID(), subscriptionName);
      
      if (!removed)
      {
         throw new JMSException("Failed to remove durable subscription");
      }

      
      subscription.unsubscribe();
      
      try
      {
         pm.removeAllChannelData(subscription.getChannelID());
      }
      catch (Exception e)
      {
         log.error("Failed to remove message data", e);
         throw new IllegalStateException("Failed to remove message data");
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
   
   // Protected -----------------------------------------------------
   
   protected ServerConsumerEndpoint putConsumerDelegate(int consumerID, ServerConsumerEndpoint d)
   {
      return (ServerConsumerEndpoint)consumers.put(new Integer(consumerID), d);
   }

   protected ServerConsumerEndpoint getConsumerDelegate(int consumerID)
   {
      return (ServerConsumerEndpoint)consumers.get(new Integer(consumerID));
   }
   
   protected ServerConsumerEndpoint removeConsumerDelegate(int consumerID)
   {
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
   protected void setStarted(boolean s)
   {
      synchronized(consumers)
      {
         for(Iterator i = consumers.values().iterator(); i.hasNext(); )
         {
            ((ServerConsumerEndpoint)i.next()).setStarted(s);
         }
      }
   }   

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
