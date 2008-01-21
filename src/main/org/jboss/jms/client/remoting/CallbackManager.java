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

import org.jboss.jms.client.impl.ClientConsumer;
import org.jboss.messaging.util.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * The CallbackManager is an InvocationHandler used for handling callbacks to message consumers.
 * The callback is received and dispatched off to the relevant consumer.
 * 
 * There is one instance of this class per remoting connection - which is to a unique server -
 * therefore there is no need to add the server id to the key when doing look ups.
 * 
 * TODO this class should be merged with use of PacketDispatcher.client instance and 
 * ClientConsumerPacketHandler should wrap ClientConsumer class
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CallbackManager
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(CallbackManager.class);

   public static final String JMS_CALLBACK_SUBSYSTEM = "CALLBACK";

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   protected Map<String, ClientConsumer> callbackHandlers;

   // Constructors ---------------------------------------------------------------------------------

   public CallbackManager()
   {
      callbackHandlers = new ConcurrentReaderHashMap();
   }

   // Public ---------------------------------------------------------------------------------------

   public void registerHandler(String consumerID, ClientConsumer handler)
   {
      callbackHandlers.put(consumerID, handler);
   }

   public ClientConsumer unregisterHandler(String consumerID)
   { 
      return callbackHandlers.remove(consumerID);
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
