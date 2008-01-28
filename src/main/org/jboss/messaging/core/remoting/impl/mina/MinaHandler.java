/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import java.util.concurrent.TimeoutException;

import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.keepalive.KeepAliveTimeoutException;
import org.apache.mina.filter.reqres.Response;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.Ping;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaHandler extends IoHandlerAdapter
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaHandler.class);

   // Attributes ----------------------------------------------------

   private final PacketDispatcher dispatcher;

   private KeepAliveNotifier keepAliveManager;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public MinaHandler(PacketDispatcher dispatcher, KeepAliveNotifier keepAliveManager)
   {
      this.dispatcher = dispatcher;
      this.keepAliveManager = keepAliveManager;
   }

   // Public --------------------------------------------------------

   // IoHandlerAdapter overrides ------------------------------------

   @Override
   public void exceptionCaught(IoSession session, Throwable cause)
         throws Exception
   {
      if (cause instanceof KeepAliveTimeoutException && keepAliveManager != null)
      {
         String serverSessionID = Long.toString(session.getId());
         TimeoutException e = new TimeoutException();
         e.initCause(cause);
         keepAliveManager.notifyKeepAliveTimeout(e, serverSessionID);
      }
      // FIXME ugly way to know we're on the server side
      // close session only on the server side
      if (dispatcher != PacketDispatcher.client)
      {
         session.close();
      }
   }
   
   @Override
   public void messageReceived(final IoSession session, Object message)
         throws Exception
   {
      if (message instanceof Response)
      {
         log.trace("received response " + message);
         // response is handled by the reqres filter.
         // do nothing
         return;
      }
      
      if (message instanceof Ping)
      {
         log.trace("received ping " + message);
         // response is handled by the keep-alive filter.
         // do nothing
         return;
      }

      if (!(message instanceof AbstractPacket))
      {
         throw new IllegalArgumentException("Unknown message type: " + message);
      }

      AbstractPacket packet = (AbstractPacket) message;
      PacketSender sender = new PacketSender()
      {
         public void send(AbstractPacket p)
         {
            session.write(p);
         }
         
         public String getSessionID()
         {
            return Long.toString(session.getId());
         }
      };

      if (log.isTraceEnabled())
         log.trace("received packet " + packet);

      dispatcher.dispatch(packet, sender);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
