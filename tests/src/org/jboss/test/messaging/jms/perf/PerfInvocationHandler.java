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
package org.jboss.test.messaging.jms.perf;

import javax.management.MBeanServer;

import org.jboss.logging.Logger;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class PerfInvocationHandler implements ServerInvocationHandler
{
   private static final Logger log = Logger.getLogger(PerfInvocationHandler.class);
   

   public Object invoke(InvocationRequest invocation) throws Throwable
   {
      log.trace("Received request");
      ServerRequest request = (ServerRequest)invocation.getParameter();
      return request.execute();         
   }

   public void addListener(InvokerCallbackHandler callbackHandler)
   {
      
   }

   public void removeListener(InvokerCallbackHandler callbackHandler)
   {
   }

   public void setMBeanServer(MBeanServer server)
   {      
   }
   
   public void setInvoker(ServerInvoker invoker)
   {      
   }
}