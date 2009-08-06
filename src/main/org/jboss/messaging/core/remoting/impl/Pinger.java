/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.messaging.core.remoting.impl;

import java.util.concurrent.Future;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;

/**
 * A Pinger
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class Pinger implements Runnable, ChannelHandler
{   
   private static final Logger log = Logger.getLogger(Pinger.class);
   
   private volatile boolean closed;

   private Future<?> future;
   
   private long lastPingReceived;
   
   private final long expiryPeriod;
   
   private final ChannelHandler extraHandler;
   
   private final Runnable connectionFailedAction;
   
   private final Channel channel0;
   
   private boolean first = true;
   
   private boolean stopPinging;   
   
   public Pinger(final RemotingConnection conn, final long expiryPeriod, final ChannelHandler extraHandler,
                 final Runnable connectionFailedAction, final long lastPingReceived)
   {
      this.expiryPeriod = expiryPeriod;
      
      this.extraHandler = extraHandler;
      
      this.connectionFailedAction = connectionFailedAction;
      
      this.channel0 = conn.getChannel(0, -1, false); 
      
      this.lastPingReceived = lastPingReceived;
      
      channel0.setHandler(this);
   }
      
   public synchronized void setFuture(final Future<?> future)
   {
      this.future = future;
   }
   
   public synchronized void handlePacket(final Packet packet)
   {
      if (closed)
      {
         return;
      }
      
      if (packet.getType() == PacketImpl.PING)
      {
         lastPingReceived = System.currentTimeMillis();
      }
      else if (extraHandler != null)
      {
         extraHandler.handlePacket(packet);
      }
      else
      {
         throw new IllegalStateException("Invalid packet " + packet.getType());
      }
   }
   
   public synchronized void run()
   {
      if (closed)
      {
         return;
      }
      
      if (!first && ( System.currentTimeMillis() - lastPingReceived > expiryPeriod))
      {
         connectionFailedAction.run();
      }
      else if (!stopPinging)
      {      
         channel0.send(new Ping());
      }
      
      first = false;
   }
     
   public void close()
   {
      if (future != null)
      {                      
         future.cancel(false);
      }

      closed = true;
   }
   
   public synchronized void stopPinging()
   {
      this.stopPinging = true;
   }
}
