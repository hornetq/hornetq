/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.BrowserDelegate;
import org.jboss.jms.client.ConsumerDelegate;
import org.jboss.jms.client.ProducerDelegate;
import org.jboss.jms.client.SessionDelegate;
import org.jboss.jms.container.Container;
import org.jboss.jms.message.standard.MessageFactoryInterceptor;

/**
 * An interceptor for creating delegates
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class FactoryInterceptor
   implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

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
      String methodName = ((MethodInvocation) invocation).method.getName();
      if (methodName.equals("createSession"))
         return createSession(invocation, (SessionDelegate) result);
      else if (methodName.equals("createBrowser"))
         return createBrowser(invocation, (BrowserDelegate) result);
      else if (methodName.equals("createConsumer"))
         return createConsumer(invocation, (ConsumerDelegate) result);
      else if (methodName.equals("createProducer"))
         return createProducer(invocation, (ProducerDelegate) result);
      else
         return result;
   }

   // Protected ------------------------------------------------------

   protected SessionDelegate createSession(Invocation invocation, SessionDelegate server)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[]
      {
         MessageFactoryInterceptor.singleton,
         FactoryInterceptor.singleton
      };
      Container connection = Container.getContainer(invocation); 
      return ClientContainerFactory.getSessionContainer(connection, server, interceptors, null);
   }

   protected BrowserDelegate createBrowser(Invocation invocation, BrowserDelegate server)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[0];
      Container session = Container.getContainer(invocation); 
      return ClientContainerFactory.getBrowserContainer(session, server, interceptors, null);
   }

   protected ConsumerDelegate createConsumer(Invocation invocation, ConsumerDelegate server)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[0];
      Container session = Container.getContainer(invocation); 
      return ClientContainerFactory.getConsumerContainer(session, server, interceptors, null);
   }

   protected ProducerDelegate createProducer(Invocation invocation, ProducerDelegate server)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[0];
      Container session = Container.getContainer(invocation); 
      return ClientContainerFactory.getProducerContainer(session, server, interceptors, null);
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
