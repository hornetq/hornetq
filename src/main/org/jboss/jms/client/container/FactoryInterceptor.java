/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.Interceptor;
import org.jboss.aop.Invocation;
import org.jboss.aop.MethodInvocation;
import org.jboss.jms.client.BrowserDelegate;
import org.jboss.jms.client.ConnectionDelegate;
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
      String methodName = ((MethodInvocation) invocation).method.getName();
      if (methodName.equals("createSession"))
         return createSession(invocation);
      else if (methodName.equals("createBrowser"))
         return createBrowser(invocation);
      else if (methodName.equals("createConsumer"))
         return createConsumer(invocation);
      else if (methodName.equals("createProducer"))
         return createProducer(invocation);
      else
         return invocation.invokeNext();
   }

   // Protected ------------------------------------------------------

   protected SessionDelegate createSession(Invocation invocation)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[]
      {
         MessageFactoryInterceptor.singleton,
         FactoryInterceptor.singleton
      };
      ConnectionDelegate connection = (ConnectionDelegate) Container.getProxy(invocation); 
      return ClientContainerFactory.getSessionContainer(connection, interceptors, null);
   }

   protected BrowserDelegate createBrowser(Invocation invocation)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[0];
      SessionDelegate session = (SessionDelegate) Container.getProxy(invocation); 
      return ClientContainerFactory.getBrowserContainer(session, interceptors, null);
   }

   protected ConsumerDelegate createConsumer(Invocation invocation)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[0];
      SessionDelegate session = (SessionDelegate) Container.getProxy(invocation); 
      return ClientContainerFactory.getConsumerContainer(session, interceptors, null);
   }

   protected ProducerDelegate createProducer(Invocation invocation)
      throws Throwable
   {
      Interceptor[] interceptors = new Interceptor[0];
      SessionDelegate session = (SessionDelegate) Container.getProxy(invocation); 
      return ClientContainerFactory.getProducerContainer(session, interceptors, null);
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
