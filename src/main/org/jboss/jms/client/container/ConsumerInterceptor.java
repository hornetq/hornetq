/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Set;
import java.util.Iterator;

import javax.jms.Destination;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.remoting.Remoting;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;

/**
 * 
 * Handles operations related to the consumer.
 * 
 * Important! There is one instance of this interceptor per instance of Consumer and Session.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ConsumerInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -5432273485632120909L;

   private static final Logger log = Logger.getLogger(ConsumerInterceptor.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   
   protected Destination destination;
   
   protected boolean noLocal;
   
   protected String messageSelector;
   
   protected String receiverID;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "ConsumerInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         String methodName = mi.getMethod().getName();
         SimpleMetaData invocationMetaData = invocation.getMetaData();

         if (log.isTraceEnabled()) { log.trace("handling " + methodName); }

         if ("createConsumerDelegate".equals(methodName))
         {
            // register/unregister a callback handler that deal with callbacks sent by the server

            InvokerLocator serverLocator = (InvokerLocator)invocationMetaData.
                  getMetaData(RemotingClientInterceptor.REMOTING, RemotingClientInterceptor.INVOKER_LOCATOR);

            if (serverLocator == null)
            {
               throw new RuntimeException("No InvokerLocator supplied. Can't invoke remotely!");
            }

            String subsystem = (String)invocationMetaData.
                  getMetaData(RemotingClientInterceptor.REMOTING, RemotingClientInterceptor.SUBSYSTEM);

            if (subsystem == null)
            {
               throw new RuntimeException("No subsystem supplied. Can't invoke remotely!");
            }

            // TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)
            Connector callbackServer = Remoting.getCallbackServer();
            InvokerLocator callbackServerLocator = callbackServer.getLocator();
            log.debug("Callback server listening on " + callbackServerLocator);

            boolean isCC = ((Boolean)mi.getArguments()[4]).booleanValue();


            MessageCallbackHandler msgHandler = new MessageCallbackHandler(isCC);


            Client client = new Client(serverLocator, subsystem);
            client.addListener(msgHandler, callbackServerLocator);


            // Optimization: I've already created the client so I may as well pass it along to be
            //               used by the InvokerInterceptor too.
            invocationMetaData.addMetaData(RemotingClientInterceptor.REMOTING,
                                           RemotingClientInterceptor.CLIENT,
                                           client, PayloadKey.TRANSIENT);

            // I will need this on the server-side to create the ConsumerDelegate instance
            invocationMetaData.addMetaData(JMSAdvisor.JMS, JMSAdvisor.REMOTING_SESSION_ID,
                                           client.getSessionId(), PayloadKey.AS_IS);

            ConsumerDelegate consumerDelegate = (ConsumerDelegate)invocation.invokeNext();

            JMSConsumerInvocationHandler consumerHandler =
                  (JMSConsumerInvocationHandler)Proxy.getInvocationHandler(consumerDelegate);

            consumerHandler.setMessageHandler(msgHandler);

            Destination theDest = ((Destination)mi.getArguments()[0]);
            String theSelector = ((String)mi.getArguments()[1]);
            boolean theNoLocal = ((Boolean)mi.getArguments()[2]).booleanValue();
            String theReceiverID =
               (String)consumerHandler.getMetaData().getMetaData(JMSAdvisor.JMS,
                                                                 JMSAdvisor.CONSUMER_ID);

            SessionDelegate del = getDelegate(invocation);

				msgHandler.setSessionDelegate(del);
				msgHandler.setReceiverID(theReceiverID);
            msgHandler.setCallbackServer(callbackServer, client); // TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)

            consumerDelegate.setDestination(theDest);
            consumerDelegate.setNoLocal(theNoLocal);
            consumerDelegate.setMessageSelector(theSelector);
            consumerDelegate.setReceiverID(theReceiverID);

            return consumerDelegate;
         }
         else if ("getDestination".equals(methodName))
         {
            return this.destination;
         }
         else if ("getNoLocal".equals(methodName))
         {
            return this.noLocal ? Boolean.TRUE : Boolean.FALSE;
         }
         else if ("getMessageSelector".equals(methodName))
         {
            return this.messageSelector;
         }
         else if ("getReceiverID".equals(methodName))
         {
            return this.receiverID;
         }
         else if ("setDestination".equals(methodName))
         {
            this.destination = (Destination)mi.getArguments()[0];
            return null;
         }
         else if ("setNoLocal".equals(methodName))
         {
            this.noLocal = ((Boolean)mi.getArguments()[0]).booleanValue();
            return null;
         }
         else if ("setMessageSelector".equals(methodName))
         {
            this.messageSelector = (String)mi.getArguments()[0];
            return null;
         }
         else if ("setReceiverID".equals(methodName))
         {
            this.receiverID = (String)mi.getArguments()[0];
            return null;
         }
         else if ("redeliver".equals(methodName) ||
                  "recover".equals(methodName))
         {
            // invalidate messages sitting in the consumers' buffers
            resetConsumerBuffers(mi);
         }
         else if ("closing".equals(methodName))
         {
            // invalidate messages sitting in the buffer
            MessageCallbackHandler msgHandler = getMessageHandler(mi);
            if (msgHandler != null)
            {
               List evictedMessageIDs = msgHandler.close();

               invocationMetaData.addMetaData(JMSAdvisor.JMS,
                                              JMSAdvisor.EVICTED_MESSAGE_IDS,
                                              evictedMessageIDs, PayloadKey.AS_IS);
            }
         }
      }
      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

	private JMSInvocationHandler getHandler(Invocation invocation)
   {
      return ((JMSMethodInvocation)invocation).getHandler();
   }

   private MessageCallbackHandler getMessageHandler(Invocation invocation)
   {
      Object handler = ((JMSMethodInvocation)invocation).getHandler();
      if (handler instanceof JMSConsumerInvocationHandler)
      {
         JMSConsumerInvocationHandler consumerHandler = (JMSConsumerInvocationHandler)handler;
         return consumerHandler.getMessageHandler();
      }
      return null;
   }

   private SessionDelegate getDelegate(Invocation invocation)
   {
      return (SessionDelegate)getHandler(invocation).getDelegate();
   }

   private void resetConsumerBuffers(Invocation invocation)
   {
      Set consumerIH = ((JMSMethodInvocation)invocation).getHandler().getChildren();
      for(Iterator i = consumerIH.iterator(); i.hasNext();)
      {
         JMSInvocationHandler h = (JMSInvocationHandler)i.next();
         MessageCallbackHandler msgHandler =
               (MessageCallbackHandler)h.getMetaData().
               getMetaData(JMSAdvisor.JMS, JMSAdvisor.CALLBACK_HANDLER);

         if (msgHandler != null)
         {
            msgHandler.reset();
         }
      }
   }

   // Inner classes -------------------------------------------------
}
