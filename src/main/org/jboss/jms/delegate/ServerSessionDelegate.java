/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import org.jboss.jms.client.container.JMSInvocationHandler;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.util.PayloadKey;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aspects.remoting.InvokeRemoteInterceptor;
import org.jboss.messaging.core.local.AbstractDestination;
import org.jboss.logging.Logger;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;
import java.lang.reflect.Proxy;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerSessionDelegate implements SessionDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerSessionDelegate.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String sessionID;
   protected ServerConnectionDelegate parent;

   protected int producerIDCounter;
   protected int consumerIDCounter;

   protected Map producers;
   protected Map consumers;

   protected ServerPeer serverPeer;

   // Constructors --------------------------------------------------

   public ServerSessionDelegate(String sessionID, ServerConnectionDelegate parent)
   {
      this.sessionID = sessionID;
      this.parent = parent;
      producers = new HashMap();
      consumers = new HashMap();
      producerIDCounter = 0;
      consumerIDCounter = 0;
      serverPeer = parent.getServerPeer();
   }

   // SessionDelegate implementation --------------------------------

   public ProducerDelegate createProducerDelegate(Destination d) throws JBossJMSException
   {

      log.debug("Creating a producer delegate, destination = " + d);

      // look-up destination
      DestinationManager dm = serverPeer.getDestinationManager();
      AbstractDestination destination = null;
      try
      {
         destination = dm.getDestination(d);
      }
      catch(Exception e)
      {
         throw new JBossJMSException("Cannot map destination " + d, e);
      }

      log.debug("got destination: " + destination);

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
      metadata.addMetaData(InvokeRemoteInterceptor.REMOTING,
                           InvokeRemoteInterceptor.INVOKER_LOCATOR,
                           serverPeer.getLocator(),
                           PayloadKey.AS_IS);
      metadata.addMetaData(InvokeRemoteInterceptor.REMOTING,
                           InvokeRemoteInterceptor.SUBSYSTEM,
                           "AOP",
                           PayloadKey.AS_IS);
      // TODO: Is this really necessary? Can't I just use the producerID?
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CLIENT_ID, parent.getClientID(), PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID, sessionID, PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.PRODUCER_ID, producerID, PayloadKey.AS_IS);

      h.getMetaData().mergeIn(metadata);

      // TODO
      ClassLoader loader = getClass().getClassLoader();
      Class[] interfaces = new Class[] { ProducerDelegate.class };
      pd = (ProducerDelegate)Proxy.newProxyInstance(loader, interfaces, h);

      // create the corresponding "server-side" ProducerDelegate and register it with this
      // SessionDelegate instance
      ServerProducerDelegate spd = new ServerProducerDelegate(producerID, destination, this);
      putProducerDelegate(producerID, spd);
      return pd;
   }


   public ConsumerDelegate createConsumerDelegate(Destination d) throws JBossJMSException
   {
      log.debug("Creating a consumer delegate, destination = " + d);

      // look-up destination
      DestinationManager dm = serverPeer.getDestinationManager();
      AbstractDestination destination = null;
      try
      {
         destination = dm.getDestination(d);
      }
      catch(Exception e)
      {
         throw new JBossJMSException("Cannot map destination " + d, e);
      }

      log.debug("got destination: " + destination);

      // create the dynamic proxy that implements ConsumerDelegate

      ConsumerDelegate cd = null;
      Serializable oid = serverPeer.getConsumerAdvisor().getName();
      String stackName = "ConsumerStack";
      AdviceStack stack = AspectManager.instance().getAdviceStack(stackName);

      // TODO why do I need to the advisor to create the interceptor stack?
      Interceptor[] interceptors = stack.createInterceptors(serverPeer.getConsumerAdvisor(), null);

      // TODO: The ConnectionFactoryDelegate and ConnectionDelegate share the same locator (TCP/IP connection?). Performance?
      JMSInvocationHandler h = new JMSInvocationHandler(interceptors);

      String consumerID = generateConsumerID();

      SimpleMetaData metadata = new SimpleMetaData();
      // TODO: The ConnectionFactoryDelegate and ConnectionDelegate share the same locator (TCP/IP connection?). Performance?
      metadata.addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, oid, PayloadKey.AS_IS);
      metadata.addMetaData(InvokeRemoteInterceptor.REMOTING,
                           InvokeRemoteInterceptor.INVOKER_LOCATOR,
                           serverPeer.getLocator(),
                           PayloadKey.AS_IS);
      metadata.addMetaData(InvokeRemoteInterceptor.REMOTING,
                           InvokeRemoteInterceptor.SUBSYSTEM,
                           "AOP",
                           PayloadKey.AS_IS);
      // TODO: Hmmm, is this really necessary? Can't I just use the producerID?
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CLIENT_ID, parent.getClientID(), PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID, sessionID, PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.PRODUCER_ID, consumerID, PayloadKey.AS_IS);

      h.getMetaData().mergeIn(metadata);

      // TODO
      ClassLoader loader = getClass().getClassLoader();
      Class[] interfaces = new Class[] { ConsumerDelegate.class };
      cd = (ConsumerDelegate)Proxy.newProxyInstance(loader, interfaces, h);

      // create the corresponding "server-side" ConsumerDelegate and register it with this
      // SessionDelegate instance
      ServerConsumerDelegate scd = new ServerConsumerDelegate(consumerID, destination, this);
      putConsumerDelegate(consumerID, scd);
      return cd;
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
