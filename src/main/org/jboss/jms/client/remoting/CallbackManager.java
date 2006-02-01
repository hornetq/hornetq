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
package org.jboss.jms.client.remoting;

import java.util.Map;

import javax.management.MBeanServer;

import org.jboss.jms.message.MessageDelegate;
import org.jboss.jms.server.endpoint.DeliveryRunnable;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * 
 * A CallbackManager.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * CallbackManager.java,v 1.1 2006/02/01 17:38:30 timfox Exp
 */
public class CallbackManager implements ServerInvocationHandler
{
   protected Map callbackHandlers;
   
   public CallbackManager()
   {
      callbackHandlers = new ConcurrentReaderHashMap();
   }
   
   public void registerHandler(int consumerID, MessageCallbackHandler handler)
   {
      callbackHandlers.put(new Integer(consumerID), handler);
   }
   
   public void unregisterHandler(int consumerID)
   {
      callbackHandlers.remove(new Integer(consumerID));
   }
   
   public void addListener(InvokerCallbackHandler arg0)
   { 
   }

   public Object invoke(InvocationRequest ir) throws Throwable
   {
      DeliveryRunnable dr = (DeliveryRunnable)ir.getParameter();
      
      int consumerID = dr.getConsumerID();
      
      MessageDelegate del = dr.getMessageDelegate();
      
      MessageCallbackHandler handler =
         (MessageCallbackHandler)callbackHandlers.get(new Integer(consumerID));
      
      if (handler == null)
      {
         throw new IllegalStateException("Cannot find handler for consumer: " + consumerID);
      }
      
      handler.handleMessage(del);
      
      return null;
   }

   public void removeListener(InvokerCallbackHandler arg0)
   {

   }

   public void setInvoker(ServerInvoker arg0)
   { 
   }

   public void setMBeanServer(MBeanServer arg0)
   {
   }

}
