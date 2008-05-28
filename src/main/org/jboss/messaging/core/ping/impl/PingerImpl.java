/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.ping.impl;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.ping.Pinger;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.mina.CleanUpNotifier;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;

/**
 * A PingerImpl.Pings the Client or SErver and waits for the KeepAliveTimeout for a response. If none occurs clean up is
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

   private final NIOSession session;

   private final PongHandler pongHandler;

   private final long pongTimeout;

   private CleanUpNotifier failureHandler;


   public PingerImpl(final PacketDispatcher dispatcher, final NIOSession session, final long pongTimeout,
                     final CleanUpNotifier failureHandler)
   {
      this.dispatcher = dispatcher;

      this.session = session;

      long handlerID = dispatcher.generateID();

      this.pongTimeout = pongTimeout;

      this.failureHandler = failureHandler;

      pongHandler = new PongHandler(handlerID);

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
      pongHandler.response = null;
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
      }
      //now we have sent a ping, wait for a pong
      Packet response = pongHandler.waitForResponse(pongTimeout);

      if (response == null)
      {
         failureHandler.fireCleanup(session.getID(), new MessagingException(MessagingException.CONNECTION_TIMEDOUT, "no pong received"));
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

   //TODO - duplicated from RemotingConnectionImpl - TODO combine
   private static class PongHandler implements PacketHandler
   {
      private long id;

      private Packet response;

      private boolean failed;

      PongHandler(final long id)
      {
         this.id = id;
      }

      public long getID()
      {
         return id;
      }

      public synchronized void setFailed()
      {
         failed = true;
      }

      public synchronized void handle(final Packet packet, final PacketReturner sender)
      {
         if (failed)
         {
            //Ignore any responses that come back after we timed out
            return;
         }

         this.response = packet;

         notify();
      }

      public synchronized Packet waitForResponse(final long timeout)
      {
         if (failed)
         {
            throw new IllegalStateException("Cannot wait for response - pinger has failed");
         }

         long toWait = timeout;
         long start = System.currentTimeMillis();

         while (response == null && toWait > 0)
         {
            try
            {
               wait(toWait);
            }
            catch (InterruptedException e)
            {
            }

            long now = System.currentTimeMillis();

            toWait -= now - start;

            start = now;
         }

         if (response == null)
         {
            failed = true;
         }

         return response;
      }

   }


}
