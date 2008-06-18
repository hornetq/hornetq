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

package org.jboss.messaging.core.ping.impl;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.ping.Pinger;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;

/**
 * A PingerImpl.Pings the Client or SErver and waits for the PingTimeout for a response. If none occurs clean up is
 * carried out.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public class PingerImpl implements Pinger
{
   private static final Logger log = Logger.getLogger(PingerImpl.class);

   private static boolean isTraceEnabled = log.isTraceEnabled();

   private final PacketDispatcher dispatcher;

   private final RemotingSession session;

   private final ResponseHandler pongHandler;

   private final long pongTimeout;

   private CleanUpNotifier failureHandler;


   public PingerImpl(final PacketDispatcher dispatcher, final RemotingSession session, final long pongTimeout,
                     final ResponseHandler pongHandler, final CleanUpNotifier failureHandler)
   {
      this.dispatcher = dispatcher;

      this.session = session;

      this.pongTimeout = pongTimeout;

      this.failureHandler = failureHandler;

      this.pongHandler = pongHandler;
      
      dispatcher.register(pongHandler);
   }

   public void close()
   {
      dispatcher.unregister(pongHandler.getID());
   }

   public void run()
   {
      Ping ping = new Ping(session.getID());

      ping.setTargetID(0);
      ping.setExecutorID(session.getID());
      ping.setResponseTargetID(pongHandler.getID());
      pongHandler.reset();
      try
      {
         if (isTraceEnabled)
         {
            log.trace("sending ping: " + ping);
         }
         session.write(ping);
      }
      catch (Exception e)
      {
         log.error("Caught unexpected exception", e);

         failureHandler.fireCleanup(session.getID(), new MessagingException(MessagingException.CONNECTION_TIMEDOUT, e.getMessage()));
         return;
      }
      
      //now we have sent a ping, wait for a pong
      Packet response = pongHandler.waitForResponse(pongTimeout);

      if (response == null)
      {
         failureHandler.fireCleanup(session.getID(), new MessagingException(MessagingException.CONNECTION_TIMEDOUT, "no pong received"));
         return;
      }
      else
      {
         Pong pong = (Pong) response;
         if (isTraceEnabled)
         {
            log.trace("pong received: " + pong);
         }
         if (pong.isSessionFailed())
         {
            //Session has already been failed on the server side - so we need to fail here too.
            pongHandler.setFailed();

            failureHandler.fireCleanup(session.getID(), new MessagingException(MessagingException.CONNECTION_TIMEDOUT, "pong received but session already failed"));
         }
      }
   }
}
