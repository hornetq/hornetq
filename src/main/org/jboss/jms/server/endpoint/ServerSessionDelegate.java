/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;


import org.jboss.jms.client.container.JMSInvocationHandler;
import org.jboss.jms.client.container.InvokerInterceptor;
import org.jboss.jms.client.container.JMSConsumerInvocationHandler;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.tx.LocalTx;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.AcknowledgmentHandler;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.util.PayloadKey;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.AbstractDestination;
import org.jboss.messaging.core.util.Lockable;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerCallbackHandler;
import org.jboss.util.id.GUID;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;
import java.lang.reflect.Proxy;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
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
   protected int consumerIDCounter;
	protected int browserIDCounter;

   protected Map producers;
   protected Map consumers;
	protected Map browsers;

   protected ServerPeer serverPeer;

   private InvokerCallbackHandler callbackHandler;

   // Constructors --------------------------------------------------

   public ServerSessionDelegate(String sessionID, ServerConnectionDelegate connectionEndpoint)
   {
      this.sessionID = sessionID;
      this.connectionEndpoint = connectionEndpoint;      
      producers = new HashMap();
      consumers = new HashMap();
		browsers = new HashMap();
      producerIDCounter = 0;
      consumerIDCounter = 0;
      serverPeer = connectionEndpoint.getServerPeer();
   }

   // SessionDelegate implementation --------------------------------

   public ProducerDelegate createProducerDelegate(Destination jmsDestination)
         throws JBossJMSException
   {

      // look-up destination
      DestinationManager dm = serverPeer.getDestinationManager();
      AbstractDestination destination = null;
      try
      {
         destination = dm.getDestination(jmsDestination);
      }
      catch(Exception e)
      {
         throw new JBossJMSException("Cannot map destination " + jmsDestination, e);
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
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CLIENT_ID, connectionEndpoint.getClientID(), PayloadKey.AS_IS);
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
            new ServerProducerDelegate(producerID, destination, jmsDestination, this);
      putProducerDelegate(producerID, spd);

      log.debug("created producer delegate (producerID=" + producerID + ")");

      return delegate;
   }

   public ConsumerDelegate createConsumerDelegate(Destination jmsDestination)
         throws JBossJMSException
   {
      // look-up destination
      DestinationManager dm = serverPeer.getDestinationManager();
      AbstractDestination destination = null;
      try
      {
         destination = dm.getDestination(jmsDestination);
      }
      catch(Exception e)
      {
         throw new JBossJMSException("Cannot map destination " + jmsDestination, e);
      }

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
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CLIENT_ID, connectionEndpoint.getClientID(), PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID, sessionID, PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CONSUMER_ID, consumerID, PayloadKey.AS_IS);
      h.getMetaData().mergeIn(metadata);

      // TODO
      ClassLoader loader = getClass().getClassLoader();
      Class[] interfaces = new Class[] { ConsumerDelegate.class, AcknowledgmentHandler.class };
      ConsumerDelegate delegate = (ConsumerDelegate)Proxy.newProxyInstance(loader, interfaces, h);

      if (callbackHandler == null)
      {
         throw new JBossJMSException("null callback handler");
      }

      // create the Consumer endpoint and register it with this SessionDelegate instance
      ServerConsumerDelegate scd =
            new ServerConsumerDelegate(consumerID, destination, callbackHandler, this);
      putConsumerDelegate(consumerID, scd);

      log.debug("created consumer endpoint (destination=" + jmsDestination + ")");

      return delegate;
   }

   public Message createMessage() throws JBossJMSException
   {
      throw new JBossJMSException("We don't create messages on the server");
   }
   
   public void close() throws JBossJMSException
   {
      log.debug("close()");
    
      //The traversal of the children is done in the ClosedInterceptor
   }
   
   public void closing() throws JMSException
   {
      log.debug("close()");

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
   
   
   
   public void sendTransaction(LocalTx tx)
      throws JMSException
   {            
      log.debug("Sending transaction to receiver");
      
      List messages = tx.getMessages();
      
      log.debug("Messages:" + messages);
      
      Iterator iter = messages.iterator();
      while (iter.hasNext())
      {
         Message m = (Message)iter.next();
         sendMessage(m);
         log.debug("Sent message");
      }
   }
	
	public BrowserDelegate createBrowserDelegate(Destination jmsDestination, String messageSelector)
   	throws JMSException
	{
	   // look-up destination
	   DestinationManager dm = serverPeer.getDestinationManager();
	   AbstractDestination destination = null;
	   try
	   {
	      destination = dm.getDestination(jmsDestination);
	   }
	   catch(Exception e)
	   {
	      throw new JBossJMSException("Cannot map destination " + jmsDestination, e);
	   }
	
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
	  
	   metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CLIENT_ID, connectionEndpoint.getClientID(), PayloadKey.AS_IS);
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
   
   void sendMessage(Message m) throws JMSException
   {
      //The JMSDestination header must already have been set for each message
      Destination dest = m.getJMSDestination();
      if (dest == null)
      {
         throw new IllegalStateException("JMSDestination header not set!");
      }
      
      Receiver receiver = null;
      try
      {
         DestinationManager dm = serverPeer.getDestinationManager();
         receiver = dm.getDestination(dest);
      }
      catch(Exception e)
      {
         throw new JBossJMSException("Cannot map destination " + dest, e);
      }
      
      m.setJMSMessageID(generateMessageID());         
   
      boolean acked = receiver.handle((Routable)m);
   
      if (!acked)
      {
         log.debug("The message was not acknowledged");
         //TODO deal with this properly
      }  
   }

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
   protected String generateConsumerID()
   {
      int id;
      synchronized(this)
      {
         id = consumerIDCounter++;
      }
      return "Consumer" + id;
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

   private String generateMessageID()
   {
      StringBuffer sb = new StringBuffer("ID:");
      sb.append(new GUID().toString());
      return sb.toString();
   }
   
   // Inner classes -------------------------------------------------
}
