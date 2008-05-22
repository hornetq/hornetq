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
public class ClientPingerImpl implements ClientPinger
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

   public ClientPingerImpl(MessagingServer server)
   {
      this.server = server;
   }

   public void run()
   {
      try
      {
         synchronized (this)
         {
            replies.clear();
            //ping all the sessions
            for (Long sessionId : connections.keySet())
            {
               try
               {
                  Ping ping = new Ping(sessionId);
                  ping.setTargetID(0);
                  connections.get(sessionId).getPacketReturner().send(ping);
                  replies.add(sessionId);
                  if(isTraceEnabled)
                  {
                     log.trace("sending " + ping);
                  }
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
            //wait for the keep alive timeout period
            try
            {
               wait(server.getConfiguration().getKeepAliveTimeout() * 1000);
            }
            catch (InterruptedException e)
            {
            }
         }
         //at this point cleanup any replies we havent received
         for (Long reply : replies)
         {
            if(cleanUpNotifier != null)
               cleanUpNotifier.fireCleanup(reply, new MessagingException(MessagingException.CONNECTION_TIMEDOUT, "unable to ping client"));
            connections.remove(reply);
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   /**
    * pong received from client
    * @param pong
    */
   public void pong(Pong pong)
   {
      if(isTraceEnabled)
      {
         log.trace("received reply" + pong);
      }
      replies.remove(pong.getSessionID());
   }

   /**
    * register a connection.
    *
    * @param remotingSessionID
    * @param sender
    */
   public void registerConnection(long remotingSessionID, PacketReturner sender)
   {
      if (connections.get(remotingSessionID) == null)
      {
         connections.put(remotingSessionID, new ConnectionHolder(remotingSessionID, sender));
      }
      else
      {
         connections.get(remotingSessionID).increment();
      }

   }

   /**
    * unregister a connection.
    *
    * @param remotingSessionID
    */
   public void unregister(long remotingSessionID)
   {
      ConnectionHolder connectionHolder = connections.get(remotingSessionID);
      if(connectionHolder != null)
      {
         connectionHolder.decrement();
         if(connectionHolder.get() == 0)
         {
            connections.remove(remotingSessionID);
         }
      }
   }

   /**
    * register the cleanup notifier to use
    *
    * @param cleanUpNotifier
    */
   public void registerCleanUpNotifier(CleanUpNotifier cleanUpNotifier)
   {
      this.cleanUpNotifier = cleanUpNotifier;
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
