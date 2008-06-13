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
