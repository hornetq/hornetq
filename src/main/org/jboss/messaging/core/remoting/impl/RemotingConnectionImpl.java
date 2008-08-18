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

package org.jboss.messaging.core.remoting.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.MessagingBuffer;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.Connection;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt> $Id: RemotingConnectionImpl.java 4633
 *          2008-07-04 11:43:34Z timfox $
 */
public class RemotingConnectionImpl implements RemotingConnection
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingConnectionImpl.class);

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------

   private final Connection transportConnection;

   private final List<FailureListener> failureListeners = new ArrayList<FailureListener>();

   private final PacketDispatcher dispatcher;

   private final Location location;

   private final long blockingCallTimeout;

   private ScheduledFuture<?> future;

   private boolean firstTime = true;

   private volatile boolean gotPong;

   private Runnable pinger;

   private volatile PacketHandler pongHandler;

   private volatile boolean destroyed;

   // Constructors
   // ---------------------------------------------------------------------------------

   public RemotingConnectionImpl(final Connection transportConnection,
         final PacketDispatcher dispatcher, final Location location,
         final long blockingCallTimeout, final long pingPeriod,
         final ScheduledExecutorService pingExecutor)
   {
      this.transportConnection = transportConnection;

      this.dispatcher = dispatcher;

      this.location = location;

      this.blockingCallTimeout = blockingCallTimeout;

      pinger = new Pinger();

      pongHandler = new PongHandler(dispatcher.generateID());

      dispatcher.register(pongHandler);

      future = pingExecutor.scheduleWithFixedDelay(pinger, pingPeriod, pingPeriod,
                                                   TimeUnit.MILLISECONDS);
   }

   public RemotingConnectionImpl(final Connection transportConnection,
         final PacketDispatcher dispatcher, final Location location,
         final long blockingCallTimeout)
   {
      this.transportConnection = transportConnection;

      this.dispatcher = dispatcher;

      this.location = location;

      this.blockingCallTimeout = blockingCallTimeout;
   }

   // Public
   // ---------------------------------------------------------------------------------------

   // RemotingConnection implementation
   // ------------------------------------------------------------

   public Object getID()
   {
      return transportConnection.getID();
   }

   /**
    * send the packet and block until a response is received (<code>oneWay</code>
    * is set to <code>false</code>)
    */
   public Packet sendBlocking(final long targetID, final long executorID, final Packet packet) throws MessagingException
   {
      packet.setTargetID(targetID);
      packet.setExecutorID(executorID);

      return sendBlocking(packet);
   }

   public Packet sendBlocking(final Packet packet) throws MessagingException
   {
      long handlerID = dispatcher.generateID();

      ResponseHandlerImpl handler = new ResponseHandlerImpl(handlerID);

      dispatcher.register(handler);

      try
      {
         packet.setResponseTargetID(handlerID);

         doWrite(packet);

         Packet response = handler.waitForResponse(blockingCallTimeout);

         if (response == null)
         {
            MessagingException me = new MessagingException(MessagingException.CONNECTION_TIMEDOUT,
               "No response received for " + packet);
            fail(me);
            throw me;
         }

         if (response instanceof MessagingExceptionMessage)
         {
            MessagingExceptionMessage message = (MessagingExceptionMessage) response;

            throw message.getException();
         }
         else
         {
            return response;
         }
      }
      finally
      {
         dispatcher.unregister(handlerID);
      }
   }

   public void sendOneWay(final long targetID, final long executorID,
         final Packet packet)
   {
      packet.setTargetID(targetID);
      packet.setExecutorID(executorID);

      doWrite(packet);
   }

   public void sendOneWay(final Packet packet)
   {
      doWrite(packet);
   }

   public synchronized void addFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      if (failureListeners.contains(listener))
      {
         throw new IllegalStateException("FailureListener already set");
      }

      failureListeners.add(listener);
   }

   public synchronized boolean removeFailureListener(final FailureListener listener)
   {
      if (listener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      return failureListeners.remove(listener);
   }

   public PacketDispatcher getPacketDispatcher()
   {
      return dispatcher;
   }

   public Location getLocation()
   {
      return location;
   }

   public MessagingBuffer createBuffer(final int size)
   {
      return transportConnection.createBuffer(size);
   }

   public synchronized void fail(final MessagingException me)
   {
      log.warn(me.getMessage());

      destroy();

      // Then call the listeners
      for (FailureListener listener : failureListeners)
      {
         try
         {
            listener.connectionFailed(me);
         }
         catch (Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others from
            // executing
            log.error("Failed to execute failure listener", t);
         }
      }
   }

   public synchronized void destroy()
   {
      if (destroyed)
      {
         return;
      }

      if (future != null)
      {
         future.cancel(false);
      }

      if (pongHandler != null)
      {
         dispatcher.unregister(pongHandler.getID());
      }

      destroyed = true;

      // We close the underlying transport connection
      transportConnection.close();
   }

   // Package protected
   // ----------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void doWrite(final Packet packet)
   {
      if (destroyed)
      {
         throw new IllegalStateException("Cannot write packet to connection, it is destroyed");
      }

      MessagingBuffer buffer = transportConnection.createBuffer(PacketImpl.INITIAL_BUFFER_SIZE);

      packet.encode(buffer);

      transportConnection.write(buffer);
   }


   // Inner classes
   // --------------------------------------------------------------------------------

   private class Pinger implements Runnable
   {
      public synchronized void run()
      {
         if (!firstTime && !gotPong)
         {
            // Error - didn't get pong back
            MessagingException me = new MessagingException(MessagingException.NOT_CONNECTED,
                  "Did not receive pong from server");

            fail(me);
         }

         gotPong = false;
         firstTime = false;

         // Send ping
         Packet ping = new PacketImpl(PacketImpl.PING);
         ping.setTargetID(0);
         ping.setExecutorID(0);
         ping.setResponseTargetID(pongHandler.getID());

         doWrite(ping);
      }
   }

   private class PongHandler implements PacketHandler
   {
      private final long id;

      PongHandler(final long id)
      {
         this.id = id;
      }

      public long getID()
      {
         return id;
      }

      public void handle(Object connectionID, Packet packet)
      {
         gotPong = true;
      }

   }

}
