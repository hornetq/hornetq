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

package org.jboss.messaging.core.remoting.impl.wireformat.cluster;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionReplicateSendMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SessionSendMessage.class);
   
   // Attributes ----------------------------------------------------

   private long producerID;
   
   private ServerMessage serverMessage;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionReplicateSendMessage(final long producerID, final ServerMessage message)
   {
      super(SESS_REPLICATE_SEND);

      this.producerID = producerID;
      
      this.serverMessage = message;
   }
      
   public SessionReplicateSendMessage()
   {
      super(SESS_REPLICATE_SEND);
   }

   // Public --------------------------------------------------------

   public long getProducerID()
   {
      return producerID;
   }
   
   public ServerMessage getServerMessage()
   {
      return serverMessage;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(producerID);      
      
      serverMessage.encode(buffer);
      
      buffer.putLong(serverMessage.getMessageID());
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      //TODO can be optimised
      
      producerID = buffer.getLong();
                  
      serverMessage = new ServerMessageImpl();
      
      serverMessage.decode(buffer);
      
      serverMessage.getBody().flip();   
      
      serverMessage.setMessageID(buffer.getLong());
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

