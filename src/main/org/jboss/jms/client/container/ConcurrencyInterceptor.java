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

import javax.jms.JMSException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;

/**
 * Interceptor - useful in debugging to determine when concurrent access to objects is occurring
 * Not to be used in normal usage
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 */
public class ConcurrencyInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private Thread currentThread;
   
   //debug
   private String methodName;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ----------------------------------
   
   public String getName()
   {
      return "ConcurrencyInterceptor";
   }
   
   public Object invoke(Invocation invocation) throws Throwable
   {           
      //checkThread(invocation);
            
      
      Object res = invocation.invokeNext();
      //unsetThread();
      return res;
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private synchronized void checkThread(Invocation inv) throws JMSException
   {
      if (currentThread != null)
      {
         if (!Thread.currentThread().equals(currentThread))
         {
            String thisMethodName = ((MethodInvocation)inv).getMethod().getName();
            throw new JMSException("Attempting to execute " + thisMethodName + " but already executing " + methodName);
         }
      }
      else
      {
         currentThread = Thread.currentThread();
         //debug
         methodName = ((MethodInvocation)inv).getMethod().getName();
      }      
   }
   
   private synchronized void unsetThread()
   {
      currentThread = null;
   }
     
   // Inner Classes -------------------------------------------------
   
}


