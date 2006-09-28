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

import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;

import org.jboss.jms.server.endpoint.ClientDelivery;
import org.jboss.jms.server.remoting.MessagingMarshallable;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * 
 * A CallbackManager.
 * 
 * The CallbackManager is an InvocationHandler used for handling callbacks to message consumers
 * The callback is received and dispatched off to the relevant consumer
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * CallbackManager.java,v 1.1 2006/02/01 17:38:30 timfox Exp
 */
public class CallbackManager implements ServerInvocationHandler
{
   private static final Logger log = Logger.getLogger(CallbackManager.class);

   
   protected Map callbackHandlers;

   public CallbackManager()
   {
      callbackHandlers = new ConcurrentReaderHashMap();
   }
   
   public void registerHandler(int serverId, int consumerId, MessageCallbackHandler handler)
   {
      Long lookup = calcLookup(serverId, consumerId);
      
      callbackHandlers.put(lookup, handler);
   }
   
   public void unregisterHandler(int serverId, int consumerId)
   {
      Long lookup = calcLookup(serverId, consumerId);
      
      callbackHandlers.remove(lookup);
   }
   
   private Long calcLookup(int serverId, int consumerId)
   {
      log.info("calculating lookup for server:" + serverId + " consumer:" + consumerId);
      long id1 = serverId;
      
      id1 <<= 32;
      
      log.info("id1 is " + Long.toBinaryString(id1));
            
      long id2 = consumerId;
      
      log.info("id2 is " + Long.toBinaryString(id2));
      
      long lookup = id1 | id2;
      
      log.info("lookup is " + Long.toBinaryString(lookup));
      
      
      return new Long(lookup);
   }
   
   public void addListener(InvokerCallbackHandler arg0)
   { 
   }

   public Object invoke(InvocationRequest ir) throws Throwable
   {
      MessagingMarshallable mm = (MessagingMarshallable)ir.getParameter();
      
      ClientDelivery dr = (ClientDelivery)mm.getLoad();
      
      log.info("received message(s) from server " + dr.getServerId());
      
      Long lookup = calcLookup(dr.getServerId(), dr.getConsumerId());
      
      log.info("lookup key is " + lookup);
      
      List msgs = dr.getMessages();

      MessageCallbackHandler handler =
         (MessageCallbackHandler)callbackHandlers.get(lookup);
      
      if (handler == null)
      {
         throw new IllegalStateException("Cannot find handler for consumer: " + dr.getConsumerId() +  " and server " + dr.getServerId());
      }
      
      return new MessagingMarshallable(mm.getVersion(), handler.handleMessage(msgs));
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
