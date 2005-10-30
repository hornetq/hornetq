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

import java.io.ObjectStreamException;
import java.io.Serializable;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.InvocationResponse;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class InvokerInterceptor implements Interceptor, Serializable
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(InvokerInterceptor.class);

   public static final InvokerInterceptor singleton = new InvokerInterceptor();

   private static final long serialVersionUID = -8628256359399051120L;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   // InvokerInterceptor implementation -----------------------------

   public String getName()
   {
      return "InvokerInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      // look for a Client, it's possible that it has been created already
      Client client = (Client)invocation.getMetaData(RemotingClientInterceptor.REMOTING, RemotingClientInterceptor.CLIENT);

      if (client == null)
      {
         throw new IllegalStateException("Cannot find remoting client");
      }

      if (log.isTraceEnabled()) { log.trace("invoking " + ((MethodInvocation)invocation).getMethod().getName() + " on server"); }
      InvocationResponse response = (InvocationResponse)client.invoke(invocation, null);
      if (log.isTraceEnabled()) { log.trace("got server response for " + ((MethodInvocation)invocation).getMethod().getName()); }

      invocation.setResponseContextInfo(response.getContextInfo());
      return response.getResponse();
   }

   // Public --------------------------------------------------------

   Object readResolve() throws ObjectStreamException
   {
      return singleton;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

