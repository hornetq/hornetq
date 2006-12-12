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

import org.jboss.jms.server.endpoint.ClientDelivery;
import org.jboss.jms.server.remoting.MessagingMarshallable;
import org.jboss.logging.Logger;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.HandleCallbackException;
import org.jboss.remoting.callback.InvokerCallbackHandler;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * 
 * A CallbackManager.
 * 
 * The CallbackManager is an InvocationHandler used for handling callbacks to message consumers.
 * The callback is received and dispatched off to the relevant consumer.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version 1.1
 *
 * CallbackManager.java,v 1.1 2006/02/01 17:38:30 timfox Exp
 */
public class CallbackManager implements InvokerCallbackHandler
{
   // Constants -----------------------------------------------------

   protected static final Logger log = Logger.getLogger(CallbackManager.class);

   public static final String JMS_CALLBACK_SUBSYSTEM = "CALLBACK";

   // Static --------------------------------------------------------

   protected static CallbackManager theManager;

   // Attributes ----------------------------------------------------

   // Map<Long(lookup)-MessageCallbackHandler>
   protected Map callbackHandlers;

   // Constructors --------------------------------------------------

   public CallbackManager()
   {
      callbackHandlers = new ConcurrentReaderHashMap();
   }

   // InvokerCallbackHandler implementation -------------------------

   public void handleCallback(Callback callback) throws HandleCallbackException
   {
      MessagingMarshallable mm = (MessagingMarshallable)callback.getParameter();
      ClientDelivery dr = (ClientDelivery)mm.getLoad();
      Long lookup = computeLookup(dr.getServerId(), dr.getConsumerId());
      List msgs = dr.getMessages();

      MessageCallbackHandler handler = (MessageCallbackHandler)callbackHandlers.get(lookup);

      if (handler == null)
      {
         throw new IllegalStateException("Cannot find handler for consumer: " + dr.getConsumerId() +
                                         " and server " + dr.getServerId());
      }

      handler.handleMessage(msgs);
   }

   // Public --------------------------------------------------------

   public void registerHandler(int serverID, int consumerID, MessageCallbackHandler handler)
   {
      Long lookup = computeLookup(serverID, consumerID);

      callbackHandlers.put(lookup, handler);
   }

   public MessageCallbackHandler unregisterHandler(int serverID, int consumerID)
   { 
      Long lookup = computeLookup(serverID, consumerID);

      return (MessageCallbackHandler)callbackHandlers.remove(lookup);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private Long computeLookup(int serverID, int consumerID)
   {
      long id1 = serverID;

      id1 <<= 32;

      long lookup = id1 | consumerID;

      return new Long(lookup);
   }

   // Inner classes -------------------------------------------------

}
