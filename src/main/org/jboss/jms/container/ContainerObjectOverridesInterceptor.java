/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.container;

import org.jboss.aop.Interceptor;
import org.jboss.aop.Invocation;
import org.jboss.aop.MethodInvocation;

/**
 * An interceptor for providing standard object methods
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class ContainerObjectOverridesInterceptor
   implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static ContainerObjectOverridesInterceptor singleton = new ContainerObjectOverridesInterceptor();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "ContainerObjectOverridesInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation) invocation;
      String methodName = mi.method.getName();
      if (methodName.equals("equals"))
         return equals(mi);
      else if (methodName.equals("hashCode"))
         return hashCode(mi);
      else if (methodName.equals("toString"))
         return toString(mi);
      else
         return invocation.invokeNext();
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
      return new Boolean(Container.getProxy(mi).equals(mi.arguments[0]));
   }

   protected Integer hashCode(MethodInvocation mi)
   {
      return new Integer(System.identityHashCode(Container.getProxy(mi)));
   }
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
