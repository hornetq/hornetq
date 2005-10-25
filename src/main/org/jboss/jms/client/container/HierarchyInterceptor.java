/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import java.io.Serializable;
import java.lang.reflect.Proxy;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.logging.Logger;


/**
 * An interceptor for checking closed state. It waits for
 * other invocations to complete allowing the close.
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox</a>
 *
 * $Id$
 */
public class HierarchyInterceptor  implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 3762253041273221177L;
   
   // Attributes ----------------------------------------------------


   // Static --------------------------------------------------------

	private static final Logger log = Logger.getLogger(HierarchyInterceptor.class);
	
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "HierarchyInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      String methodName = ((MethodInvocation) invocation).getMethod().getName();
      
      Object retVal = invocation.invokeNext();
            
      if (methodName.equals("createSessionDelegate") ||
          methodName.equals("createProducerDelegate") ||
          methodName.equals("createConsumerDelegate") ||
          methodName.equals("createBrowserDelegate") ||
			 methodName.equals("createConnectionDelegate"))
      {
			if (log.isTraceEnabled()) log.trace("post " + methodName + ", updating hierarchy");
			
         JMSInvocationHandler thisHandler = ((JMSMethodInvocation)invocation).getHandler();
         JMSInvocationHandler returnedHandler =
               (JMSInvocationHandler)Proxy.getInvocationHandler(retVal);
         returnedHandler.setDelegate(retVal);
         thisHandler.addChild(returnedHandler);
      }
      
      return retVal;      
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}

