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
import java.lang.reflect.Method;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;

/**
 * Constructs various things that can be created entirely or partially on the client.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class FactoryInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -2377273484834534832L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation ------------------------------------

   public String getName()
   {
      return "FactoryInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      if (invocation instanceof JMSMethodInvocation)
      {
         JMSMethodInvocation mi = (JMSMethodInvocation)invocation;
         Method m = mi.getMethod();
         String methodName = m.getName();
         if ("createMessage".equals(methodName))
         {
            return new JBossMessage();
         }
         else if ("createBytesMessage".equals(methodName))
         {
            return new JBossBytesMessage();
         }
         else if ("createMapMessage".equals(methodName))
         {
            return new JBossMapMessage();
         }
         else if ("createObjectMessage".equals(methodName))
         {
         	JBossObjectMessage msg = new JBossObjectMessage();
         	if (mi.getArguments() != null)
         	{
         		msg.setObject((Serializable)mi.getArguments()[0]);
         	}
         	return msg;
         }
         else if ("createStreamMessage".equals(methodName))
         {
            return new JBossStreamMessage();
         }
         else if ("createTextMessage".equals(methodName))
         {
         	JBossTextMessage msg = new JBossTextMessage();
         	if (mi.getArguments() != null)
         	{
         		msg.setText((String)mi.getArguments()[0]);
         	}
         	return msg;
         }
      }

      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
		
   // Inner classes -------------------------------------------------
}
