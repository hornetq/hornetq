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

import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.mina.CleanUpNotifier;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.client.ServerPonger;
import org.jboss.messaging.core.exception.MessagingException;
import org.apache.mina.common.IoSession;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerPongerImpl implements PacketHandler, ServerPonger
{
   private static Logger log = Logger.getLogger(ServerPongerImpl.class);
   private static boolean traceEnabled = log.isTraceEnabled();
   IoSession session;
   private long id;
   long interval;
   long timeout;
   CleanUpNotifier cleanUpNotifier;
   KeepAliveHandler keepAliveHandler;
   CountDownLatch latch = new CountDownLatch(1);

   public ServerPongerImpl(CleanUpNotifier cleanUpNotifier, KeepAliveHandler keepAliveHandler, IoSession session, long id, long timeout, long interval)
   {
      this.cleanUpNotifier = cleanUpNotifier;
      this.keepAliveHandler = keepAliveHandler;
      this.session = session;
      this.id = id;
      this.timeout = timeout;
      this.interval = interval;
   }


   public void run()
   {
      boolean pinged;
      latch = new CountDownLatch(1);
      try
      {
         pinged = latch.await(timeout + interval, TimeUnit.MILLISECONDS);
         if(!pinged)
         {
            log.warn("no ping received from server, cleaning up connection.");
            cleanUpNotifier.fireCleanup(session.getId(), new MessagingException(MessagingException.CONNECTION_TIMEDOUT, "no ping received from server"));
         }
      }
      catch (InterruptedException e)
      {
      }
   }

   public long getID()
   {
      return id;
   }

   public void handle(Packet packet, PacketReturner sender)
   {
      Ping ping = (Ping) packet;
      latch.countDown();
      if(traceEnabled)
      {
         log.trace("received ping:" + ping);
      }
      Pong pong = keepAliveHandler.ping(ping);
      if(pong != null)
      {
         session.write(pong);
      }
   }
}
