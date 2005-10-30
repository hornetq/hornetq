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
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.logging.Logger;

import javax.jms.MessageListener;
import java.io.Serializable;
import java.lang.reflect.Method;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReceiverInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -5432273485632120909L;
   
   private static final Logger log = Logger.getLogger(ReceiverInterceptor.class);


   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "ReceiverInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         Method m = mi.getMethod();
         String name = m.getName();

         if (log.isTraceEnabled()) { log.trace("handling " + name); }

         Object[] args = mi.getArguments();
         MessageCallbackHandler messageHandler = (MessageCallbackHandler)mi.
               getMetaData(JMSAdvisor.JMS, JMSAdvisor.CALLBACK_HANDLER);

         if (name.equals("receive"))
         {
            long timeout = args == null ? 0 : ((Long)args[0]).longValue();
            return messageHandler.receive(timeout);
         }
         else if (name.equals("receiveNoWait"))
         {
            return messageHandler.receive(-1);
         }
         else if (name.equals("setMessageListener"))
         {
            MessageListener l = (MessageListener)args[0];
            messageHandler.setMessageListener(l);
            return null;
         }
         else if (name.equals("getMessageListener"))
         {
            return messageHandler.getMessageListener();
         }
      }
      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
