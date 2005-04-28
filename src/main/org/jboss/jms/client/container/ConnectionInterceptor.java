/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.server.container.JMSAdvisor;

import java.io.Serializable;
import java.lang.reflect.Method;

/**
 * Interceptor that fields any metod calls which can be handled locally.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ConnectionInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -3245645348483459328L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "ConnectionInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         Method m = mi.getMethod();
         String name = m.getName();
         if ("getClientID".equals(name))
         {
            return mi.getMetaData(JMSAdvisor.JMS, JMSAdvisor.CLIENT_ID);
         }
         else if ("setClientID".equals(name))
         {
            throw new WrapperException(new IllegalStateException("ClientID already set"));
         }
         else if ("getExceptionListener".equals(name))
         {
            return mi.getMetaData(JMSAdvisor.JMS, JMSAdvisor.EXCEPTION_LISTENER);
         }
         else if ("setExceptionListener".equals(name))
         {
            JMSInvocationHandler thisHandler =
               ((JMSMethodInvocation)invocation).getHandler();
            thisHandler.getMetaData()
               .addMetaData(JMSAdvisor.JMS,
                            JMSAdvisor.EXCEPTION_LISTENER,
                            mi.getArguments()[0],
                            PayloadKey.AS_IS);
            return null;
         }
      }
      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
