/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.Interceptor;
import org.jboss.aop.Invocation;
import org.jboss.jms.JBossJMSException;

/**
 * An interceptor for testing the thrown exceptions are
 * JMSExceptions.
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JMSExceptionInterceptor
   implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static JMSExceptionInterceptor singleton = new JMSExceptionInterceptor();

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "JMSExceptionInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      try
      {
         return invocation.invokeNext();
      }
      catch (Throwable t)
      {
         throw JBossJMSException.handle(t);
      }
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
