/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.messaging.jms.client.SessionDelegate;
import org.jboss.messaging.jms.client.BrowserDelegate;
import org.jboss.messaging.jms.client.ConsumerDelegate;
import org.jboss.messaging.jms.client.ProducerDelegate;
import org.jboss.messaging.jms.container.Container;
import org.jboss.messaging.jms.message.StandardMessageFactoryInterceptor;
import org.jboss.messaging.jms.message.StandardMessageFactoryInterceptor;

/**
 * An interceptor for creating delegates.
 *
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class FactoryInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   /** The singleton to be used in all interceptor stacks */
   public static final FactoryInterceptor singleton = new FactoryInterceptor();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "FactoryInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {

      Object result = invocation.invokeNext();

      String methodName = ((MethodInvocation)invocation).getMethod().getName();
      
      if (methodName.equals("createSession"))
      {
         return createSession(invocation, (SessionDelegate)result);
      }
      else if (methodName.equals("createBrowser"))
      {
         return createBrowser(invocation, (BrowserDelegate)result);
      }
      else if (methodName.equals("createConsumer"))
      {
         return createConsumer(invocation, (ConsumerDelegate)result);
      }
      else if (methodName.equals("createProducer"))
      {
         return createProducer(invocation, (ProducerDelegate)result);
      }
      else
      {
         return result;
      }
   }

   // Protected ------------------------------------------------------

   protected SessionDelegate createSession(Invocation invocation, SessionDelegate server)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[]
      {
         StandardMessageFactoryInterceptor.singleton,
         FactoryInterceptor.singleton
      };
      Container connection = Container.getContainer(invocation);
      return ClientContainerFactory.createSessionContainer(connection, server, interceptors, null);
   }

   protected BrowserDelegate createBrowser(Invocation invocation, BrowserDelegate server)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[0];
      Container session = Container.getContainer(invocation); 
      return ClientContainerFactory.createBrowserContainer(session, server, interceptors, null);
   }

   protected ConsumerDelegate createConsumer(Invocation invocation, ConsumerDelegate server)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[0];
      Container session = Container.getContainer(invocation); 
      return ClientContainerFactory.createConsumerContainer(session, server, interceptors, null);
   }

   protected ProducerDelegate createProducer(Invocation invocation, ProducerDelegate server)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[0];
      Container session = Container.getContainer(invocation); 
      return ClientContainerFactory.createProducerContainer(session, server, interceptors, null);
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
