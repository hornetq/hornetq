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

import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.mina.CleanUpNotifier;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.server.ClientPinger;
import org.jboss.messaging.core.exception.MessagingException;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ClientPingerImpl implements ClientPinger, PacketHandler
{
   private static Logger log = Logger.getLogger(ClientPingerImpl.class);

   private static boolean isTraceEnabled = log.isTraceEnabled();
   /**
    * the current active connections
    */
   private Map<Long, ConnectionHolder> connections = new ConcurrentHashMap<Long, ConnectionHolder>();
   /**
    * holds connections we are waiting for for replies
    */
   List<Long> replies = new ArrayList<Long>();
   /**
    * the server
    */
   private MessagingServer server;
   /**
    * the cleanupnotifier to use on failed pings
    */
   private CleanUpNotifier cleanUpNotifier;
   private KeepAliveFactory keepAliveFactory;
   private PacketReturner sender;
   long id = 0;
   private Pong pong = null;

   public ClientPingerImpl(MessagingServer server, KeepAliveFactory keepAliveFactory, CleanUpNotifier cleanUpNotifier, final PacketReturner sender)
   {
      this.server = server;
      this.keepAliveFactory = keepAliveFactory;
      this.cleanUpNotifier = cleanUpNotifier;
      this.sender = sender;
   }

   public void run()
   {
      id = server.getRemotingService().getDispatcher().generateID();
      server.getRemotingService().getDispatcher().register(this);
      Ping ping = keepAliveFactory.ping(sender.getSessionID());
      ping.setTargetID(0);
      ping.setResponseTargetID(id);
      while(keepAliveFactory.isPinging(sender.getSessionID()))
      {
         synchronized (this)
         {
            try
            {
               wait(server.getConfiguration().getKeepAliveInterval() * 1000);
            }
            catch (InterruptedException e)
            {
            }
         }
         pong = null;
         try
         {
            sender.send(ping);
            synchronized (this)
            {
               wait(server.getConfiguration().getKeepAliveTimeout() * 1000);
            }
            if(pong == null)
            {
               cleanUpNotifier.fireCleanup(sender.getSessionID(), new MessagingException(MessagingException.CONNECTION_TIMEDOUT, "unable to ping client"));
               break;
            }
         }
         catch (Exception e)
         {
            log.warn("problem cleaning up session: " + sender.getSessionID(), e);
         }
      }
      server.getRemotingService().getDispatcher().unregister(id);
   }

   public long getID()
   {
      return id;
   }

   public void handle(Packet packet, PacketReturner sender)
   {
      Pong pong = (Pong) packet;
      if(isTraceEnabled)
      {
         log.trace("received reply" + pong);
      }
      this.pong = pong;
   }

   /**
    * simple holder class for sessions
    */
   class ConnectionHolder
   {
      AtomicInteger connectionCount = new AtomicInteger(1);
      Long sessionId;
      PacketReturner packetReturner;

      public ConnectionHolder(Long sessionId, PacketReturner packetReturner)
      {
         this.sessionId = sessionId;
         this.packetReturner = packetReturner;
      }

      public Integer increment()
      {
         return connectionCount.getAndIncrement();
      }

      public Integer decrement()
      {
         return connectionCount.getAndDecrement();
      }

      public Integer get()
      {
         return connectionCount.get();
      }

      public boolean equals(Object o)
      {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         ConnectionHolder that = (ConnectionHolder) o;

         if (!sessionId.equals(that.sessionId)) return false;

         return true;
      }

      public int hashCode()
      {
         return sessionId.hashCode();
      }

      public long getSessionId()
      {
         return sessionId;
      }

      public PacketReturner getPacketReturner()
      {
         return packetReturner;
      }
   }
}
