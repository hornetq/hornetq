/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import org.jboss.jms.client.container.JMSInvocationHandler;
import org.jboss.jms.client.container.InvokerInterceptor;
import org.jboss.jms.client.container.JMSConsumerInvocationHandler;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.endpoint.Consumer;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.util.PayloadKey;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.messaging.core.local.AbstractDestination;
import org.jboss.messaging.core.util.Lockable;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerCallbackHandler;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;
import java.lang.reflect.Proxy;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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

   protected Map producers;
   protected Map consumers;

   protected ServerPeer serverPeer;

   private InvokerCallbackHandler callbackHandler;

   // Constructors --------------------------------------------------

   public ServerSessionDelegate(String sessionID, ServerConnectionDelegate connectionEndpoint)
   {
      this.sessionID = sessionID;
      this.connectionEndpoint = connectionEndpoint;
      producers = new HashMap();
      consumers = new HashMap();
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

      ProducerDelegate pd = null;
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
      pd = (ProducerDelegate)Proxy.newProxyInstance(loader, interfaces, h);

      // create the corresponding "server-side" ProducerDelegate and register it with this
      // SessionDelegate instance
      ServerProducerDelegate spd =
            new ServerProducerDelegate(producerID, destination, jmsDestination, this);
      putProducerDelegate(producerID, spd);

      log.debug("created producer delegate (producerID=" + producerID + ")");

      return pd;
   }


   public MessageConsumer createConsumer(Destination jmsDestination) throws JBossJMSException
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

      // the remote calls for Consumer do not need a target, will be handled by interceptors
      metadata.addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, "GenericTarget", PayloadKey.AS_IS);
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
      Class[] interfaces = new Class[] { MessageConsumer.class };
      MessageConsumer proxy = (MessageConsumer)Proxy.newProxyInstance(loader, interfaces, h);

      if (callbackHandler == null)
      {
         throw new JBossJMSException("null callback handler");
      }

      // create the Consumer endpoint and register it with this SessionDelegate instance
      Consumer c =  new Consumer(consumerID, destination, callbackHandler, this);
      putConsumerDelegate(consumerID, c);

      log.debug("creating consumer endpoint (destination=" + jmsDestination + ")");

      return proxy;
   }


   public Message createMessage() throws JMSException
   {
      throw new JMSException("We don't create messages on the server");
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

   public Consumer putConsumerDelegate(String consumerID, Consumer d)
   {
      synchronized(consumers)
      {
         return (Consumer)consumers.put(consumerID, d);
      }
   }

   public Consumer getConsumerDelegate(String consumerID)
   {
      synchronized(consumers)
      {
         return (Consumer)consumers.get(consumerID);
      }
   }

   public ServerConnectionDelegate getConnectionEndpoint()
   {
      return connectionEndpoint;
   }


   /**
    * IoC.
    */
   public void setCallbackHandler(InvokerCallbackHandler callbackHandler)
   {
      this.callbackHandler = callbackHandler;
   }

   // Package protected ---------------------------------------------

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


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
