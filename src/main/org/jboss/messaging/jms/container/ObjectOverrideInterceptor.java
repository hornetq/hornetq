/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;

/**
 * An interceptor that fields the java.lang.Object methods. Usually the first in the interceptor
 * stack.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class ObjectOverrideInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   /** The singleton to be used in all interceptor stacks */
   public static ObjectOverrideInterceptor singleton = new ObjectOverrideInterceptor();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "ObjectOverrideInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;
      String methodName = mi.getMethod().getName();

      if (methodName.equals("equals"))
      {
         return equals(mi);
      }
      else if (methodName.equals("hashCode"))
      {
         return hashCode(mi);
      }
      else if (methodName.equals("toString"))
      {
         return toString(mi);
      }
      else
      {
         return invocation.invokeNext();
      }
   }

   // Protected ------------------------------------------------------

   protected String toString(MethodInvocation mi)
   {
      Object proxy = Container.getProxy(mi);
      String className = proxy.getClass().getInterfaces()[0].getName();
      StringBuffer buffer = new StringBuffer(20);
      buffer.append(className).append('@').append(System.identityHashCode(proxy));
      return buffer.toString();
   }

   protected Boolean equals(MethodInvocation mi)
   {
      return new Boolean(Container.getProxy(mi).equals(mi.getArguments()[0]));
   }

   protected Integer hashCode(MethodInvocation mi)
   {
      return new Integer(System.identityHashCode(Container.getProxy(mi)));
   }
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
