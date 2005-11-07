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
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;
import org.jboss.jms.util.JBossJMSException;

import javax.jms.JMSException;
import java.io.Serializable;

/**
 * Interceptor that wraps any Exception into a JMSException.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSExceptionInterceptor implements Interceptor, Serializable
{
	
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSExceptionInterceptor.class);
   
   private static final long serialVersionUID = -1027228559036457690L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "JMSExceptionInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {

      if (log.isTraceEnabled()) { log.trace("handling " + ((MethodInvocation)invocation).getMethod().getName()); }

      try
      {
         return invocation.invokeNext();
      }
      catch(NotYetImplementedException e)
      {
         // TODO get rid of this
         throw e;
      }
      catch(WrapperException e)
      {
         if (log.isTraceEnabled()) { log.trace("Caught WrapperException"); }
         throw e.getPayload();
      }
      catch(JMSException e)
      {
         if (log.isTraceEnabled())  { log.trace("Caught JMSException:" + e); }
         Exception cause = e.getLinkedException();
         if (cause != null)
         {
            log.error("The cause of the JMSException: ", cause);
         }
         throw e;
      }
      catch (UnsupportedOperationException e)
      {
         //These must be propagated to the client
         throw e;         
      }
      catch (RuntimeException e)
      {         
         log.error("Caught RuntimeException", e);
         JMSException ex = new javax.jms.IllegalStateException(e.getMessage());
         ex.setLinkedException(e);
         throw ex; 
      }
      catch(Exception e)
      {
         if (log.isTraceEnabled()) { log.trace("Caught Exception:" + e); }
         log.error("The cause of the JMSException: ", e);
         throw new JBossJMSException(e.getMessage(), e);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
