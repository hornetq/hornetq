/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.container;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;

/**
 * An interceptor for forwarding invocations.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public class ForwardInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The delegate container */
   private Container delegate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Create a new forwarding interceptor.
    * 
    * @param delegate the container to forward the invocation to.
    */
   public ForwardInterceptor(Container delegate)
   {
      this.delegate = delegate;
   }

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "ForwardInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      return delegate.invoke(invocation);
   }

   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
