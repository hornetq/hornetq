/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.container;

import java.util.Arrays;

import org.jboss.aop.Interceptor;
import org.jboss.aop.Invocation;
import org.jboss.aop.MethodInvocation;
import org.jboss.logging.Logger;

/**
 * An interceptor for logging invocations.
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class LogInterceptor
   implements Interceptor
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(LogInterceptor.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static LogInterceptor singleton = new LogInterceptor();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "LogInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      StringBuffer desc = getDescription(invocation);
      log.info("invoke:" + desc);
      Object result = null;
      try
      {
         result = invocation.invokeNext();
         log.info("result: " + result + " of invoke:" + desc);
         return result;
      }
      catch (Throwable t)
      {
         log.info("error in invoke:" + desc, t);
         throw t;
      }
   }

   // Protected ------------------------------------------------------

   protected StringBuffer getDescription(Invocation invocation)
   {
      MethodInvocation mi = (MethodInvocation) invocation;
      StringBuffer buffer = new StringBuffer(50);
      buffer.append(" method=").append(mi.method);
      buffer.append(" params=");
      if (mi.arguments == null)
         buffer.append("[]");
      else
         buffer.append(Arrays.asList(mi.arguments));
      buffer.append(" object=").append(Container.getContainer(invocation));
      return buffer;
   }
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
