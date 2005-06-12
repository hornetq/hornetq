/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;


import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.remoting.Remoting;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ConsumerInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -5432273485632120909L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

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
         Method m = mi.getMethod();
         if (m.getName().equals("createConsumerDelegate"))
         {
            // register/unregister a callback handler that deal with callbacks sent by the server

            InvokerLocator locator =
                  (InvokerLocator)invocation.getMetaData(InvokerInterceptor.REMOTING,
                                                         InvokerInterceptor.INVOKER_LOCATOR);
            if (locator == null)
            {
               throw new RuntimeException("No InvokerLocator supplied.  Can't invoke remotely!");
            }
            String subsystem = (String)invocation.getMetaData(InvokerInterceptor.REMOTING,
                                                              InvokerInterceptor.SUBSYSTEM);
            if (subsystem == null)
            {
               throw new RuntimeException("No subsystem supplied.  Can't invoke remotely!");
            }
            Client client = new Client(locator, subsystem);
				
				SimpleMetaData metaData = invocation.getMetaData();
				
            MessageCallbackHandler msgHandler = new MessageCallbackHandler();
				
            client.addListener(msgHandler, Remoting.getCallbackServer().getLocator());
            
            // I created the client already, pass it along to be used by the InvokerInterceptor
            metaData.addMetaData(InvokerInterceptor.REMOTING, InvokerInterceptor.CLIENT,
                                 client, PayloadKey.TRANSIENT);
            // I will need this on the server-side to create the ConsumerDelegate instance
            metaData.addMetaData(JMSAdvisor.JMS, JMSAdvisor.REMOTING_SESSION_ID,
                                 client.getSessionId(), PayloadKey.AS_IS);

            Object consumerDelegate = invocation.invokeNext();

            JMSConsumerInvocationHandler ih =
                  (JMSConsumerInvocationHandler)Proxy.getInvocationHandler(consumerDelegate);
            ih.setMessageHandler(msgHandler);
				
				msgHandler.setSessionDelegate(getDelegate(invocation));
				msgHandler.setReceiverID((String)ih.getMetaData().
                                             getMetaData(JMSAdvisor.JMS, JMSAdvisor.CONSUMER_ID));

            return consumerDelegate;
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
   
   private SessionDelegate getDelegate(Invocation invocation)
   {
      return (SessionDelegate)getHandler(invocation).getDelegate();
   }
	
   // Inner classes -------------------------------------------------
}
