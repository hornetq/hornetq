/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.core.protocol;

import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_LARGE_MSG;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_MSG;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.client.impl.ClientLargeMessageImpl;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketDecoder;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveClientLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         10/12/12
 */
public class ClientPacketDecoder extends PacketDecoder
{
   private static final long serialVersionUID = 6952614096979334582L;
   public static final ClientPacketDecoder INSTANCE = new ClientPacketDecoder();

   @Override
   public  Packet decode(final HornetQBuffer in)
   {
      final byte packetType = in.readByte();

      Packet packet;

      switch (packetType)
      {
         case SESS_RECEIVE_MSG:
         {
            packet = new SessionReceiveMessage(new ClientMessageImpl());
            break;
         }
         case SESS_RECEIVE_LARGE_MSG:
         {
            packet = new SessionReceiveClientLargeMessage(new ClientLargeMessageImpl());
            break;
         }
         default:
         {
            packet = super.decode(packetType);
         }
      }

      packet.decode(in);

      return packet;
   }

}
