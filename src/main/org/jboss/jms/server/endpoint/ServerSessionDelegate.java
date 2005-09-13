/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;


import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.client.container.InvokerInterceptor;
import org.jboss.jms.client.container.JMSConsumerInvocationHandler;
import org.jboss.jms.client.container.JMSInvocationHandler;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.server.ClientManager;
import org.jboss.jms.server.DestinationManagerImpl;
import org.jboss.jms.server.DurableSubscriptionHolder;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.local.Topic;
import org.jboss.messaging.core.util.Lockable;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;
import org.jboss.util.id.GUID;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerSessionDelegate extends Lockable implements SessionDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerSessionDelegate.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String sessionID;
   protected ServerConnectionDelegate connectionEndpoint;
      
   protected int producerIDCounter;
	protected int browserIDCounter;

   protected Map producers;
   protected Map consumers;
	protected Map browsers;
	
	protected int acknowledgmentMode;

   protected ServerPeer serverPeer;

   private InvokerCallbackHandler callbackHandler;


   // Constructors --------------------------------------------------

   public ServerSessionDelegate(String sessionID, ServerConnectionDelegate connectionEndpoint,
										  int acknowledgmentMode)
   {
      this.sessionID = sessionID;
		this.acknowledgmentMode = acknowledgmentMode;
      this.connectionEndpoint = connectionEndpoint;      
      producers = new HashMap();
      consumers = new HashMap();
		browsers = new HashMap();
      producerIDCounter = 0;
      serverPeer = connectionEndpoint.getServerPeer();
   }

   // SessionDelegate implementation --------------------------------

   public ProducerDelegate createProducerDelegate(Destination jmsDestination)
         throws JMSException
   {

      // look-up destination
      DestinationManagerImpl dm = serverPeer.getDestinationManager();
      Channel destination = null;
     
      if (jmsDestination != null)
      {
         destination = dm.getCoreDestination(jmsDestination);
      }
     
      log.debug("got producer's destination: " + destination);

      // create the dynamic proxy that implements ProducerDelegate

      Serializable oid = serverPeer.getProducerAdvisor().getName();
      String stackName = "ProducerStack";
      AdviceStack stack = AspectManager.instance().getAdviceStack(stackName);

      // TODO why do I need to the advisor to create the interceptor stack?
      Interceptor[] interceptors = stack.createInterceptors(serverPeer.getProducerAdvisor(), null);

      // TODO: The ConnectionFactoryDelegate and ConnectionDelegate share the same locator (TCP/IP connection?). Performance?
      JMSInvocationHandler h = new JMSInvocationHandler(interceptors);

      String producerID = generateProducerID();

      SimpleMetaData metadata = new SimpleMetaData();
      // TODO: The ConnectionFactoryDelegate and ConnectionDelegate share the same locator (TCP/IP connection?). Performance?
      metadata.addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, oid, PayloadKey.AS_IS);
      metadata.addMetaData(InvokerInterceptor.REMOTING,
                           InvokerInterceptor.INVOKER_LOCATOR,
                           serverPeer.getLocator(),
                           PayloadKey.AS_IS);
      metadata.addMetaData(InvokerInterceptor.REMOTING,
                           InvokerInterceptor.SUBSYSTEM,
                           "JMS",
                           PayloadKey.AS_IS);
      // TODO: Is this really necessary? Can't I just use the producerID?
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CONNECTION_ID, connectionEndpoint.getConnectionID(), PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID, sessionID, PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.PRODUCER_ID, producerID, PayloadKey.AS_IS);
      
      h.getMetaData().mergeIn(metadata);

      // TODO
      ClassLoader loader = getClass().getClassLoader();
      Class[] interfaces = new Class[] { ProducerDelegate.class };
      ProducerDelegate delegate = (ProducerDelegate)Proxy.newProxyInstance(loader, interfaces, h);

      // create the corresponding "server-side" ProducerDelegate and register it with this
      // SessionDelegate instance
      ServerProducerDelegate spd =
            new ServerProducerDelegate(producerID, jmsDestination, this);
      putProducerDelegate(producerID, spd);

      log.debug("created producer delegate (producerID=" + producerID + ")");

      return delegate;
   }

	public ConsumerDelegate createConsumerDelegate(Destination jmsDestination,
                                                  String selector,
                                                  boolean noLocal,
                                                  String subscriptionName)
		throws JMSException
   {
      if ("".equals(selector))
      {
         selector = null;
      }
      
      JBossDestination d = (JBossDestination)jmsDestination;
      
      if (log.isTraceEnabled()) { log.trace("Creating ServerConsumerDelegate for dest:" + d.getName() + 
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
    
      Channel destination = dm.getCoreDestination(jmsDestination);
     
      // create the MessageConsumer dynamic proxy

      String stackName = "ConsumerStack";
      AdviceStack stack = AspectManager.instance().getAdviceStack(stackName);

      // TODO why do I need to the advisor to create the interceptor stack?
      Interceptor[] interceptors = stack.createInterceptors(serverPeer.getSessionAdvisor(), null);
      JMSConsumerInvocationHandler h = new JMSConsumerInvocationHandler(interceptors);
      String consumerID = generateConsumerID();
      SimpleMetaData metadata = new SimpleMetaData();

      metadata.addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, serverPeer.getConsumerAdvisor().getName(), PayloadKey.AS_IS);
      metadata.addMetaData(InvokerInterceptor.REMOTING,
                           InvokerInterceptor.INVOKER_LOCATOR,
                           serverPeer.getLocator(),
                           PayloadKey.AS_IS);
      metadata.addMetaData(InvokerInterceptor.REMOTING,
                           InvokerInterceptor.SUBSYSTEM,
                           "JMS",
                           PayloadKey.AS_IS);
      // TODO: Is this really necessary? Can't I just use the consumerID?
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CONNECTION_ID, connectionEndpoint.getConnectionID(), PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID, sessionID, PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CONSUMER_ID, consumerID, PayloadKey.AS_IS);

      h.getMetaData().mergeIn(metadata);

      // TODO
      ClassLoader loader = getClass().getClassLoader();
      Class[] interfaces = new Class[] { ConsumerDelegate.class };
      ConsumerDelegate delegate = (ConsumerDelegate)Proxy.newProxyInstance(loader, interfaces, h);

      if (callbackHandler == null)
      {
         throw new JMSException("null callback handler");
      }
               
      DurableSubscriptionHolder subscription = null;
      if (subscriptionName != null)
      {
         String clientID = connectionEndpoint.getClientID();
         if (clientID == null)
         {
            throw new JMSException("Cannot create durable subscriber without having set client ID");
         }
                  
         //It's a durable subscription - have we already got one with that name?
         ClientManager clientManager = serverPeer.getClientManager();
                    
         subscription = clientManager.getDurableSubscription(clientID, subscriptionName);                        
         
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
               
               DurableSubscriptionHolder removed = this.serverPeer.getClientManager().
                  removeDurableSubscription(this.connectionEndpoint.clientID, subscriptionName);

               if (removed == null)
               {
                  throw new InvalidDestinationException("Cannot find durable subscription with name " +
                     subscriptionName + " to unsubscribe");
               }

               subscription.getTopic().remove(subscription.getQueue());
               subscription = null;
            }
         }
         
         if (subscription == null)
         {
            if (!(destination instanceof org.jboss.messaging.core.local.Topic))
            {
               throw new JMSException("Can only create a durable subscription on a topic");
            }
            
            if (log.isTraceEnabled()) { log.trace("Creating new durable subscription on topic " + destination); }
            
            Topic topic = (Topic)destination;

            MessageStore ms = connectionEndpoint.getServerPeer().getMessageStore();
            TransactionManager tm = connectionEndpoint.getServerPeer().getTransactionManager();
            subscription = new DurableSubscriptionHolder(subscriptionName, topic,
                                                         new Queue(consumerID, ms, tm),
                                                         selector);
            clientManager.addDurableSubscription(clientID, subscriptionName, subscription);
            //start it
            destination.add(subscription.getQueue());
         }
      }
      
      ServerConsumerDelegate scd =
         new ServerConsumerDelegate(consumerID,
                                    subscriptionName == null ? destination : subscription.getQueue(),
                                    callbackHandler, this, selector, noLocal, subscription);
      
      //The connection may have already been started - so the consumer must be started
      if (this.connectionEndpoint.started)
      {
         scd.setStarted(true);
      }
      
      putConsumerDelegate(consumerID, scd);
      connectionEndpoint.receivers.put(consumerID, scd);

      if (log.isTraceEnabled()) log.trace("created consumer endpoint (destination=" + jmsDestination + ")");

      return delegate;
   }

   public Message createMessage() throws JBossJMSException
   {
      throw new JBossJMSException("We don't create messages on the server");
   }
   
   public BytesMessage createBytesMessage() throws JMSException
   {
      throw new JBossJMSException("We don't create messages on the server");
   }
   
   public MapMessage createMapMessage() throws JMSException
   {
      throw new JBossJMSException("We don't create messages on the server");
   }

   public ObjectMessage createObjectMessage() throws JMSException
   {
      throw new JBossJMSException("We don't create messages on the server");
   }

   public ObjectMessage createObjectMessage(Serializable object) throws JMSException
   {
      throw new JBossJMSException("We don't create messages on the server");
   }

   public StreamMessage createStreamMessage() throws JMSException
   {
      throw new JBossJMSException("We don't create messages on the server");
   }

   public TextMessage createTextMessage() throws JMSException
   {
      throw new JBossJMSException("We don't create messages on the server");
   }
   
   public TextMessage createTextMessage(String text) throws JMSException
   {
      throw new JBossJMSException("We don't create messages on the server");
   }

   public javax.jms.Queue createQueue(String name) throws JMSException
   {
      DestinationManagerImpl dm = serverPeer.getDestinationManager();
      Channel coreDestination = dm.getCoreDestination(true, name);

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
      DestinationManagerImpl dm = serverPeer.getDestinationManager();
      Channel coreDestination = dm.getCoreDestination(false, name);

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

   public void close() throws JBossJMSException
   {
      if (log.isTraceEnabled()) log.trace("ServerSessionDelegate.close()");
    
      //The traversal of the children is done in the ClosedInterceptor
   }
   
   public void closing() throws JMSException
   {
      if (log.isTraceEnabled()) log.trace("ServerSessionDelegate.closing()");

      //Currently does nothing
   }
   
   public void commit() throws JMSException
   {
      throw new JMSException("commit is not handled on the server");
   }
   
   public void rollback() throws JMSException
   {
      throw new JMSException("rollback is not handled on the server");
   }
   
   public void recover() throws JMSException
   {
      throw new JMSException("recover is not handled on the server");
   }
   
   
	public BrowserDelegate createBrowserDelegate(Destination jmsDestination, String messageSelector)
   	throws JMSException
	{

      if (jmsDestination == null)
      {
         throw new InvalidDestinationException("null destination");
      }

	   // look-up destination
	   DestinationManagerImpl dm = serverPeer.getDestinationManager();
	  
      Channel destination = dm.getCoreDestination(jmsDestination);
	
	   BrowserDelegate bd = null;
	   Serializable oid = serverPeer.getBrowserAdvisor().getName();
	   String stackName = "BrowserStack";
	   AdviceStack stack = AspectManager.instance().getAdviceStack(stackName);
	
	   Interceptor[] interceptors = stack.createInterceptors(serverPeer.getBrowserAdvisor(), null);
	   
	   JMSInvocationHandler h = new JMSInvocationHandler(interceptors);
	
	   String browserID = generateBrowserID();
	
	   SimpleMetaData metadata = new SimpleMetaData();      
	   metadata.addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, oid, PayloadKey.AS_IS);
	   metadata.addMetaData(InvokerInterceptor.REMOTING,
	                        InvokerInterceptor.INVOKER_LOCATOR,
	                        serverPeer.getLocator(),
	                        PayloadKey.AS_IS);
	   metadata.addMetaData(InvokerInterceptor.REMOTING,
	                        InvokerInterceptor.SUBSYSTEM,
	                        "JMS",
	                        PayloadKey.AS_IS);
	  
	   metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CONNECTION_ID, connectionEndpoint.getConnectionID(), PayloadKey.AS_IS);
	   metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID, sessionID, PayloadKey.AS_IS);
	   metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.BROWSER_ID, browserID, PayloadKey.AS_IS);
	   
	   h.getMetaData().mergeIn(metadata);
		   
	   ClassLoader loader = getClass().getClassLoader();
	   Class[] interfaces = new Class[] { BrowserDelegate.class };
	   bd = (BrowserDelegate)Proxy.newProxyInstance(loader, interfaces, h);
	
	   // create the corresponding "server-side" BrowserDelegate and register it with this
	   // BrowserDelegate instance
	   ServerBrowserDelegate sbd =
	         new ServerBrowserDelegate(browserID, destination, messageSelector);
	   putBrowserDelegate(browserID, sbd);
	
	   return bd;
	}

	
	public void preDeliver(String messageID, String receiverID)
	{
		log.warn("predeliver is not handled on the server");
	}
	
   public void postDeliver(String messageID, String receiverID)
   {
      log.warn("postdeliver is not handled on the server");
   }	
	
	public void acknowledgeSession() throws JMSException
	{
		throw new JMSException("acknowledgeSession is not handled on the server");
	}
	
	/**
	 * Redeliver all unacked messages for the session
	 */
	public void redeliver() throws JMSException
	{
      if (log.isTraceEnabled()) { log.trace("redeliver"); }
		Iterator iter = this.consumers.values().iterator();
		while (iter.hasNext())
		{
          // TODO I need to do this atomically, otherwise only some of the messages may be redelivered
			ServerConsumerDelegate scd = (ServerConsumerDelegate)iter.next();
         scd.redeliver();
		}
	}
	
	public void acknowledge(String messageID, String receiverID)
		throws JMSException
	{
		this.connectionEndpoint.acknowledge(messageID, receiverID);
	}

   public Object getMetaData(Object attr)
   {
      // TODO - See "Delegate Implementation" thread
      // TODO   http://www.jboss.org/index.html?module=bb&op=viewtopic&t=64747

      // NOOP
      log.warn("getMetaData(): NOT handled on the server-side");
      return null;
   }

   public void addMetaData(Object attr, Object metaDataValue)
   {
      // TODO - See "Delegate Implementation" thread
      // TODO   http://www.jboss.org/index.html?module=bb&op=viewtopic&t=64747

      // NOOP
      log.warn("addMetaData(): NOT handled on the server-side");
   }

   public Object removeMetaData(Object attr)
   {
      // TODO - See "Delegate Implementation" thread
      // TODO   http://www.jboss.org/index.html?module=bb&op=viewtopic&t=64747

      // NOOP
      log.warn("removeMetaData(): NOT handled on the server-side");
      return null;
   }
   
   public void setTransacted(boolean transacted)
   {
      log.warn("setTransacted(): NOT handled on the server-side");
   }
   
   public void setAcknowledgeMode(int ackMode)
   {
      log.warn("setAcknowledgeMode(): NOT handled on the server-side");
   }
   
   public boolean getXA()
   {
      log.warn("getXA(): NOT handled on the server-side");
      return false;
   }
   
   public void setXA(boolean xa)
   {
      log.warn("setXA(): NOT handled on the server-side");
   }
   
   public void setXAResource(XAResource resource)
   {
      log.warn("setXAResource(): NOT handled on the server-side");
   }
   
   public void setResourceManager(ResourceManager rm)
   {
      log.warn("setResourceManager(): NOT handled on the server-side");
   }
   
   public ResourceManager getResourceManager()
   {
      log.warn("getResourceManager(): NOT handled on the server-side");
      return null;
   }
   
   public void addTemporaryDestination(Destination dest) throws JMSException
   {
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
      JBossDestination d = (JBossDestination)dest;
      
      if (!d.isTemporary())
      {
         throw new InvalidDestinationException("Destination:" + dest + " is not a temporary destination");
      }
      
      //It is illegal to delete a temporary destination if there any active consumers on it
      Channel c = serverPeer.getDestinationManager().getCoreDestination(dest);
      
      if (c == null)
      {
         throw new InvalidDestinationException("Destination:" + dest + " does not exist");         
      }
      
      Iterator iter = this.connectionEndpoint.receivers.values().iterator();
      while (iter.hasNext())
      {
         ServerConsumerDelegate scd = (ServerConsumerDelegate)iter.next();
         if (c.contains(scd))
         {
            throw new IllegalStateException("Cannot delete temporary destination, since it has active consumer(s)");
         }
      }
      
      serverPeer.getDestinationManager().removeTemporaryDestination(dest);
      this.connectionEndpoint.temporaryDestinations.remove(dest);
   }
   
   public void unsubscribe(String subscriptionName) throws JMSException
   {
      if (subscriptionName == null)
      {
         throw new InvalidDestinationException("Destination is null");
      }
      DurableSubscriptionHolder subscription = this.serverPeer.getClientManager().
            removeDurableSubscription(this.connectionEndpoint.clientID, subscriptionName);

      if (subscription == null)
      {
         throw new InvalidDestinationException("Cannot find durable subscription with name " +
            subscriptionName + " to unsubscribe");
      }
      subscription.getTopic().remove(subscription.getQueue());
      
   }
   
   public XAResource getXAResource()
   {
      log.warn("getXAResource should not be handled at the server endpoint");
      return null;
   }
   
   public int getAcknowledgeMode()
   {
      log.warn("getAcknowledgeMode should not be handled at the server endpoint");
      return -1;
   }
   
   public boolean getTransacted()
   {
      log.warn("getTransacted should not be handled at the server endpoint");
      return false;
   }
   
   public void addAsfMessage(Message m, String receiverID)
   {
      log.warn("addAsfMessage should not be handled at the server endpoint");
   }
   
   public MessageListener getMessageListener()
   {
      log.warn("getMessageListener should not be handled at the server endpoint");
      return null;
   }
   
   public void setMessageListener(MessageListener listener)
   {
      log.warn("setMessageListener should not be handled at the server endpoint");
   }
   
   public void run()
   {
      log.warn("run should not be handled at the server endpoint");
   }

   // Public --------------------------------------------------------

   public ServerProducerDelegate putProducerDelegate(String producerID, ServerProducerDelegate d)
   {
      synchronized(producers)
      {
         return (ServerProducerDelegate)producers.put(producerID, d);
      }
   }

   public ServerProducerDelegate getProducerDelegate(String producerID)
   {
      synchronized(producers)
      {
         return (ServerProducerDelegate)producers.get(producerID);
      }
   }

   public ServerConsumerDelegate putConsumerDelegate(String consumerID, ServerConsumerDelegate d)
   {
      synchronized(consumers)
      {
         return (ServerConsumerDelegate)consumers.put(consumerID, d);
      }
   }

   public ServerConsumerDelegate getConsumerDelegate(String consumerID)
   {
      synchronized(consumers)
      {
         return (ServerConsumerDelegate)consumers.get(consumerID);
      }
   }
	
	public ServerBrowserDelegate putBrowserDelegate(String browserID, ServerBrowserDelegate sbd)
   {
      synchronized(browsers)
      {
         return (ServerBrowserDelegate)browsers.put(browserID, sbd);
      }
   }
	
	public ServerBrowserDelegate getBrowserDelegate(String browserID)
   {
      synchronized(browsers)
      {
         return (ServerBrowserDelegate)browsers.get(browserID);
      }
   }

   public ServerConnectionDelegate getConnectionEndpoint()
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
            ((ServerConsumerDelegate)i.next()).setStarted(s);

         }
      }
   }

   // Protected -----------------------------------------------------

   /**
    * Generates a producerID that is unique per this SessionDelegate instance
    */
   protected String generateProducerID()
   {
      int id;
      synchronized(this)
      {
         id = producerIDCounter++;
      }
      return "Producer" + id;
   }
	
	

   /**
    * Generates a consumerID that is unique per this SessionDelegate instance
    */
   protected synchronized String generateConsumerID()
   {
      return "Consumer" + new GUID().toString();
   }
	
	/**
    * Generates a browserID that is unique per this SessionDelegate instance
    */
   protected String generateBrowserID()
   {
      int id;
      synchronized(this)
      {
         id = browserIDCounter++;
      }
      return "Browser" + id;
   }


   // Private -------------------------------------------------------

 
   
   // Inner classes -------------------------------------------------
}
