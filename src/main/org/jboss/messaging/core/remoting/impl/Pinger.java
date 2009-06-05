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
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;

/**
 * A Pinger
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class Pinger implements Runnable
{
   private static final Logger log = Logger.getLogger(Pinger.class);
   
   private boolean closed;

   private RemotingConnection conn;

   private Future<?> future;

   public Pinger(final RemotingConnection conn)
   {
      this.conn = conn;
   }

   public synchronized void setFuture(final Future<?> future)
   {
      this.future = future;
   }

   public synchronized void run()
   {
      if (closed)
      {
         return;
      }
      
      //TODO - for now we *always* sent the ping otherwise.
      //Checking dataSent does not work, for the following reason:
      //If a packet is sent just after the last ping, then no ping will be sent the next time.
      //Which means the amount of time between pings can approach 2 * ( 0.5 * client failure check period) = failure check period
      //so, due to time taken to actually travel across network + scheduling difference the client failure checker
      //can easily time out.
      
//      if (!conn.isDataSent())
//      {
         // We only send a ping if no data has been sent since last ping

         Ping ping = new Ping();

         Channel channel0 = conn.getChannel(0, -1, false);
         
         channel0.send(ping);
    //  }

      conn.clearDataSent();
   }

   public synchronized void close()
   {
      future.cancel(false);

      closed = true;
   }
}
