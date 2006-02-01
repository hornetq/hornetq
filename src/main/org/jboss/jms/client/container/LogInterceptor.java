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

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.logging.Logger;

import java.lang.reflect.Method;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class LogInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(LogInterceptor.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "LogInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      Method method = null;
      String methodName = null;
      Object target = null;
      if (trace)
      {
         target = invocation.getTargetObject();

         if (!(invocation instanceof MethodInvocation))
         {
            log.trace("invoking non-method invocation: " + invocation);
         }
         else
         {
            MethodInvocation mi = (MethodInvocation)invocation;
            method = mi.getMethod();
            methodName = method.getName();
            StringBuffer sb = new StringBuffer();
            sb.append("invoking ").append(target).append('.').append(methodName).append('(');
            Object[] args = mi.getArguments();
            if (args != null)
            {
               for(int i = 0; i < args.length; i++)
               {
                  // special precaution to hide passwords
                  if ("createConnectionDelegate".equals(methodName) && i == 1)
                  {
                     sb.append("*****");
                  }
                  else
                  {
                     sb.append(args[i]);
                  }
                  if (i < args.length - 1)
                  {
                     sb.append(", ");
                  }
               }
            }
            sb.append(')');

            log.trace(sb.toString());
         }
      }

      Object res = invocation.invokeNext();

      if (trace)
      {
         if (method == null)
         {
            log.trace(invocation + " successfully invoked on " + target);
         }
         else
         {
            if (method.getReturnType() != Void.TYPE)
            {
               log.trace(target + "." + methodName + "() returned " + res);
            }
            else
            {
               log.trace(target + "." + methodName + "() ok");
            }
         }
      }
      return res;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
