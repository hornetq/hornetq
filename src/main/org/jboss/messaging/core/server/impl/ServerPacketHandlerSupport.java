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
package org.jboss.messaging.core.server.impl;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;
import org.jboss.messaging.core.server.MessagingException;

/**
 * 
 * A ServerPacketHandlerSupport
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class ServerPacketHandlerSupport implements PacketHandler
{
   private static final Logger log = Logger.getLogger(ServerPacketHandlerSupport.class);
      
   public void handle(final Packet packet, final PacketSender sender)
   {
      Packet response;
      
      try
      {      
         response = doHandle(packet, sender);
      }
      catch (Exception e)
      {
         MessagingException me;
         
         if (e instanceof MessagingException)
         {
            me = (MessagingException)e;
         }
         else
         {
            log.error("Caught unexpected exception", e);
            
            me = new MessagingException(MessagingException.INTERNAL_ERROR);
         }
                  
         response = new MessagingExceptionMessage(me);         
      }
      
      // reply if necessary
      if (response != null && !packet.isOneWay())
      {
         response.normalize(packet);
         
         try
         {
            sender.send(response);
         }
         catch (Exception e)
         {
            log.error("Failed to send packet", e);
         }
      }
   }
   
   protected abstract Packet doHandle(final Packet packet, final PacketSender sender) throws Exception;

}
