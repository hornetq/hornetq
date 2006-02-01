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
package org.jboss.jms.server.remoting;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.management.MBeanServer;

import org.jboss.aop.joinpoint.InvocationResponse;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.Util;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSServerInvocationHandler implements ServerInvocationHandler
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSServerInvocationHandler.class);
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private ServerInvoker invoker;
   private MBeanServer server;

   protected Map callbackHandlers;
   
   private boolean trace;
   
   // Constructors --------------------------------------------------

   public JMSServerInvocationHandler()
   {
      callbackHandlers = new HashMap();
      
      trace = log.isTraceEnabled();
   }

   // ServerInvocationHandler ---------------------------------------

   public void setMBeanServer(MBeanServer server)
   {
      this.server = server;
      if (trace) { log.trace("set MBeanServer to " + this.server); }
   }

   public void setInvoker(ServerInvoker invoker)
   {
      this.invoker = invoker;
      if (trace) {log.trace("set ServerInvoker to " + this.invoker); }
   }

   public Object invoke(InvocationRequest invocation) throws Throwable
   {      
      if (trace) { log.trace("Invoking:" + invocation); }
      
      MethodInvocation i = (MethodInvocation)invocation.getParameter();
      
      String s = (String)i.getMetaData(MetaDataConstants.JMS, MetaDataConstants.REMOTING_SESSION_ID);
      
      if (s != null)
      {
         Object callbackHandler = null;
         synchronized(callbackHandlers)
         {
            callbackHandler = callbackHandlers.get(s);
         }
         if (callbackHandler != null)
         {
            if (trace) { log.trace("found calllback handler for session " + Util.guidToString(s)); }
            i.getMetaData().addMetaData(MetaDataConstants.JMS,
                                        MetaDataConstants.CALLBACK_HANDLER, callbackHandler);
         }
         else
         {
            throw new javax.jms.IllegalStateException("Cannot find callback handler " +
                                                      "for session id " + s);
         }
      }

      InvocationResponse resp = JMSDispatcher.instance.invoke(i);
      
      return resp.getResponse();
   }

   public void addListener(InvokerCallbackHandler callbackHandler)
   {                 
      if (trace) { log.trace("adding callback handler " + callbackHandler); }
      
      if (callbackHandler instanceof ServerInvokerCallbackHandler)
      {
         ServerInvokerCallbackHandler h = (ServerInvokerCallbackHandler)callbackHandler;
         String id = h.getClientSessionId(); 
         
         synchronized(callbackHandlers)
         {
            if (callbackHandlers.containsKey(id))
            {
               String msg = "The remoting client " + id + " already has a callback handler";
               log.error(msg);
               throw new RuntimeException(msg);
            }
            callbackHandlers.put(id, h);
         }
      }
      else
      {
         throw new RuntimeException("Do not know how to use callback handler " + callbackHandler);
      }
   }

   public void removeListener(InvokerCallbackHandler callbackHandler)
   {
      if (trace) { log.trace("removing callback handler: " + callbackHandler); }
      synchronized(callbackHandlers)
      {
         for(Iterator i = callbackHandlers.keySet().iterator(); i.hasNext();)
         {
            Object key = i.next();
            if (callbackHandler.equals(callbackHandlers.get(key)))
            {
               callbackHandlers.remove(key);
               return;
            }
         }
      }
   }

   /**
    * @return a Collection of InvokerCallbackHandler
    */
   public Collection getListeners()
   {
      synchronized(callbackHandlers)
      {
         return callbackHandlers.values();
      }
   }

   // Public --------------------------------------------------------
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
