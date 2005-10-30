/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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

