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

import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.wireformat.ClientDelivery;
import org.jboss.jms.wireformat.ConnectionFactoryUpdate;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.HandleCallbackException;
import org.jboss.remoting.callback.InvokerCallbackHandler;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * The CallbackManager is an InvocationHandler used for handling callbacks to message consumers.
 * The callback is received and dispatched off to the relevant consumer.
 * 
 * There is one instance of this class per remoting connection - which is to a unique server -
 * therefore there is no need to add the server id to the key when doing look ups.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CallbackManager implements InvokerCallbackHandler
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(CallbackManager.class);

   public static final String JMS_CALLBACK_SUBSYSTEM = "CALLBACK";

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   protected static CallbackManager theManager;

   // Attributes -----------------------------------------------------------------------------------

   // Map<Long(lookup)-MessageCallbackHandler>
   protected Map callbackHandlers;
   protected ConnectionFactoryCallbackHandler connectionfactoryCallbackHandler;

   // Constructors ---------------------------------------------------------------------------------

   public CallbackManager()
   {
      callbackHandlers = new ConcurrentReaderHashMap();
   }

   // InvokerCallbackHandler implementation --------------------------------------------------------

   public void handleCallback(Callback callback) throws HandleCallbackException
   {
      Object parameter = callback.getParameter();

      if (parameter instanceof ClientDelivery)
      {
         ClientDelivery dr = (ClientDelivery)parameter;
          
         Message msg = dr.getMessage();
         
         MessageProxy proxy = JBossMessage.
            createThinDelegate(dr.getDeliveryId(), (JBossMessage)msg, dr.getDeliveryCount());

         MessageCallbackHandler handler =
            (MessageCallbackHandler)callbackHandlers.get(new Integer(dr.getConsumerId()));

         if (handler == null)
         {
            // This is OK and can happen if the callback handler is deregistered on consumer close,
            // but there are messages still in transit which arrive later. In this case it is just
            // safe to ignore the message.

            if (trace) { log.trace(this + " callback handler not found, message arrived after consumer is closed"); }
            
            return;
         }

         try
         {
            handler.handleMessage(proxy);
         }
         catch (Exception e)
         {
            log.error("Failed to handle message", e);
            throw new HandleCallbackException(e.getMessage(), e);
         }
      }
      else if (parameter instanceof ConnectionFactoryUpdate)
      {
         ConnectionFactoryUpdate viewChange = (ConnectionFactoryUpdate)parameter;

         if (trace) { log.trace(this + " receiving cluster view change " + viewChange); }

         if (connectionfactoryCallbackHandler != null)
         {
            connectionfactoryCallbackHandler.handleMessage(viewChange);
         }
      }
      else
      {
         throw new HandleCallbackException("Unknow callback type: " + callback);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public void registerHandler(int consumerID, MessageCallbackHandler handler)
   {
      callbackHandlers.put(new Integer(consumerID), handler);
   }

   public void setConnectionDelegate (ClientConnectionDelegate connectionDelegate)
   {
      this.connectionfactoryCallbackHandler =
         new ConnectionFactoryCallbackHandler(connectionDelegate);
   }

   public MessageCallbackHandler unregisterHandler(int consumerID)
   { 
      return (MessageCallbackHandler)callbackHandlers.remove(new Integer(consumerID));
   }

   public String toString()
   {
      return "CallbackManager[" + Integer.toHexString(hashCode()) + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
