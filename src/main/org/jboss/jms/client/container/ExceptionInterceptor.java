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

import javax.jms.JMSException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.logging.Logger;

/**
 * Interceptor that handles exceptions thrown from JMS calls
 * 
 * This interceptor is PER_VM
 *
 * TODO: This interceptor seem a bit useless. For logging, we can use the ClientLoggingInterceptor.
 *       Remove it?
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ExceptionInterceptor implements Interceptor
{	
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ExceptionInterceptor.class);
      
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Interceptor implementation -------------------------------------------------------------------

   public String getName()
   {
      return "ExceptionInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      try
      {
         return invocation.invokeNext();
      }       
      catch(JMSException e)
      {
         // JMSException should not be logged unless trace is on
         if (trace) { log.trace("Caught JMSException", e); }
         throw e;
      }
      catch (Throwable t)
      {
         // We log everything else
         
         log(t);
         
         throw t;         
      }      
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private void log(Throwable e)
   {
      log.error("Caught throwable", e);
      Throwable e2 = e.getCause();
      if (e2 != null)
      {
         log(e2);
      }
   }

   // Inner classes -------------------------------------------------
}
