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
import org.jboss.jms.message.BytesMessageDelegate;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.jms.message.MapMessageDelegate;
import org.jboss.jms.message.MessageDelegate;
import org.jboss.jms.message.ObjectMessageDelegate;
import org.jboss.jms.message.StreamMessageDelegate;
import org.jboss.jms.message.TextMessageDelegate;
import org.jboss.logging.Logger;

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
   
   private static final Logger log = Logger.getLogger(FactoryInterceptor.class);
   

   // Attributes ----------------------------------------------------
   
   protected long ordering;

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
            JBossMessage jbm = new JBossMessage();
            jbm.setOrdering(ordering++);
            return new MessageDelegate(jbm);
         }
         else if ("createBytesMessage".equals(methodName))
         {
            JBossBytesMessage jbm = new JBossBytesMessage();
            jbm.setOrdering(ordering++);
            return new BytesMessageDelegate(jbm);
         }
         else if ("createMapMessage".equals(methodName))
         {
            JBossMapMessage jbm = new JBossMapMessage();
            jbm.setOrdering(ordering++);
            return new MapMessageDelegate(jbm);
         }
         else if ("createObjectMessage".equals(methodName))
         {
            JBossObjectMessage jbm = new JBossObjectMessage();
            jbm.setOrdering(ordering++);
         	if (mi.getArguments() != null)
         	{
         		jbm.setObject((Serializable)mi.getArguments()[0]);
         	}
         	return new ObjectMessageDelegate(jbm);
         }
         else if ("createStreamMessage".equals(methodName))
         {
            JBossStreamMessage jbm = new JBossStreamMessage();
            jbm.setOrdering(ordering++);
            return new StreamMessageDelegate(jbm);
         }
         else if ("createTextMessage".equals(methodName))
         {
            JBossTextMessage jbm = new JBossTextMessage();
            jbm.setOrdering(ordering++);
         	if (mi.getArguments() != null)
         	{
         		jbm.setText((String)mi.getArguments()[0]);
         	}
         	return new TextMessageDelegate(jbm);
         }
      }

      return invocation.invokeNext();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
		
   // Inner classes -------------------------------------------------
}
