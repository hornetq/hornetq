/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ReceiveMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ClientMessage clientMessage;
   
   private ServerMessage serverMessage;
   
   private int deliveryCount;
   
   private long deliveryID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public ReceiveMessage(final ServerMessage message, final int deliveryCount, final long deliveryID)
   {
      super(RECEIVE_MSG);

      this.serverMessage = message;
      
      this.clientMessage = null;
      
      this.deliveryCount = deliveryCount;
      
      this.deliveryID = deliveryID;
   }
   
   public ReceiveMessage(final ClientMessage message)
   {
      super(RECEIVE_MSG);

      this.clientMessage = message;
   }
   
   public ReceiveMessage()
   {
      super(RECEIVE_MSG);
   }

   // Public --------------------------------------------------------

   public ClientMessage getClientMessage()
   {
      return clientMessage;
   }
   
   public ServerMessage getServerMessage()
   {
      return serverMessage;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }
   
   public long getDeliveryID()
   {
      return deliveryID;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(deliveryCount);
      buffer.putLong(deliveryID);
      serverMessage.encode(buffer);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      //TODO can be optimised
      
      deliveryCount = buffer.getInt();
      deliveryID = buffer.getLong();
      
      clientMessage = new ClientMessageImpl(deliveryCount, deliveryID);
      
      clientMessage.decode(buffer);
      
      clientMessage.getBody().flip();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
