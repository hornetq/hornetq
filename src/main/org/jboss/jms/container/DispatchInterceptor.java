/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.container;

import java.lang.reflect.InvocationTargetException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;

/**
 * An interceptor for dispatching invocations.
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class DispatchInterceptor
   implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The target object */
   private Object target;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Create a new dispatch interceptor
    * 
    * @param target the target object
    */
   public DispatchInterceptor(Object target)
   {
      this.target = target;
   }

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "DispatchInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation) invocation;
      try
      {
         return mi.method.invoke(target, mi.arguments);
      }
      catch (Throwable t)
      {
         if (t instanceof InvocationTargetException)
            throw ((InvocationTargetException) t).getTargetException();
         throw t;
      }
   }

   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
