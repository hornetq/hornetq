/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.message;

import javax.jms.Message;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.messaging.jms.client.SessionDelegate;
import org.jboss.messaging.jms.container.Container;

/**
 * An interceptor for creating standard messages
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class StandardMessageFactoryInterceptor
   implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static final StandardMessageFactoryInterceptor singleton = new StandardMessageFactoryInterceptor();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "StandardMessageFactoryInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      String methodName = ((MethodInvocation) invocation).getMethod().getName();
      if (methodName.equals("createMessage"))
         return createMessage(invocation);
      else
         return invocation.invokeNext();
   }

   // Protected ------------------------------------------------------

   protected Message createMessage(Invocation invocation)
      throws Throwable
   {
      return new StandardMessage((SessionDelegate) Container.getProxy(invocation));
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
