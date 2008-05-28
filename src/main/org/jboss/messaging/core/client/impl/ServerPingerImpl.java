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
package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.KeepAliveHandler;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerPingerImpl implements PacketHandler
{
   private static Logger log = Logger.getLogger(ServerPingerImpl.class);
   private static boolean traceEnabled = log.isTraceEnabled();
   private long id;
   KeepAliveHandler keepAliveHandler;

   public ServerPingerImpl(KeepAliveHandler keepAliveHandler, Long id)
   {
      this.keepAliveHandler = keepAliveHandler;
      this.id = id;
   }

   public long getID()
   {
      return id;
   }

   public void handle(Packet packet, PacketReturner sender)
   {
      Ping ping = (Ping) packet;
      if(traceEnabled)
      {
         log.trace("received ping:" + ping);
      }
      Pong pong = keepAliveHandler.ping(ping);

      if(pong != null)
      {
         try
         {
            sender.send(pong);
         }
         catch (Exception e)
         {
            log.warn("error sending pong to server", e);
         }
      }
   }
}
