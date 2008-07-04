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

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectorRegistryFactory;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.RemotingConnector;
import org.jboss.messaging.core.remoting.RemotingSession;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class RemotingConnectionImpl implements RemotingConnection
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingConnectionImpl.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final Location location;

   private final ConnectionParams connectionParams;

   private RemotingConnector connector;

   private RemotingSession session;

   private List<RemotingSessionListener> sessionListeners = new ArrayList<RemotingSessionListener>();

   // Constructors ---------------------------------------------------------------------------------

   public RemotingConnectionImpl(final Location location, ConnectionParams connectionParams) throws IllegalArgumentException
   {
      if (location == null)
      {
         throw new IllegalArgumentException("location must not be null");
      }
      if (connectionParams == null)
      {
         throw new IllegalArgumentException("connection params must not be null");
      }

      this.location = location;
      this.connectionParams = connectionParams;

      log.trace(this + " created with configuration " + location);
   }

   // Public ---------------------------------------------------------------------------------------

   // RemotingConnection implementation ------------------------------------------------------------

   public void start() throws Throwable
   {
      if (log.isTraceEnabled())
      {
         log.trace(this + " started remoting connection");
      }

      connector = ConnectorRegistryFactory.getRegistry().getConnector(location, connectionParams);

      session = connector.connect();

      if (log.isDebugEnabled())
         log.debug("Using " + connector + " to connect to " + location);

      log.trace(this + " started");
   }


   public void stop()
   {
      log.trace(this + " stop");

      try
      {
         if (connector != null)
         {
            for (RemotingSessionListener sessionListener : sessionListeners)
            {
               connector.removeSessionListener(sessionListener);
            }

            RemotingConnector connectorFromRegistry = ConnectorRegistryFactory.getRegistry().removeConnector(location);

            if (connectorFromRegistry != null)
            {
               connectorFromRegistry.disconnect();
            }
         }
      }
      catch (Throwable ignore)
      {
         log.trace(this + " failed to disconnect the new client", ignore);
      }

      connector = null;
      log.trace(this + " closed");
   }

   public long getSessionID()
   {
      if (session == null || !session.isConnected())
      {
         return -1;
      }
      return session.getID();
   }

   /**
    * send the packet and block until a response is received (<code>oneWay</code> is set to <code>false</code>)
    */
   public Packet sendBlocking(final long targetID, final long executorID, final Packet packet) throws MessagingException
   {
      checkConnected();

      long handlerID = connector.getDispatcher().generateID();

      ResponseHandlerImpl handler = new ResponseHandlerImpl(handlerID);

      connector.getDispatcher().register(handler);

      try
      {
         packet.setTargetID(targetID);
         packet.setExecutorID(executorID);
         packet.setResponseTargetID(handlerID);

         try
         {
            session.write(packet);
         }
         catch (Exception e)
         {
            log.error("Caught unexpected exception", e);

            throw new MessagingException(MessagingException.INTERNAL_ERROR);
         }

         Packet response = handler.waitForResponse(connectionParams.getCallTimeout());

         if (response == null)
         {
            throw new IllegalStateException("No response received for " + packet);
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
         connector.getDispatcher().unregister(handlerID);
      }
   }

   public void sendOneWay(final long targetID, final long executorID, final Packet packet) throws MessagingException
   {
      checkConnected();

      packet.setTargetID(targetID);
      packet.setExecutorID(executorID);

      try
      {
         session.write(packet);
      }
      catch (Exception e)
      {
         log.error("Caught unexpected exception", e);

         throw new MessagingException(MessagingException.INTERNAL_ERROR);
      }
   }

   public synchronized void addRemotingSessionListener(final RemotingSessionListener newListener)
   {
      if (newListener == null)
      {
         throw new IllegalStateException("FailureListener cannot be null");
      }

      if (sessionListeners.contains(newListener))
      {
         throw new IllegalStateException("FailureListener already set");
      }

      if (newListener != null)
      {
         sessionListeners.add(newListener);
         connector.addSessionListener(newListener);
      }
   }

   public synchronized void removeRemotingSessionListener(final RemotingSessionListener listener)
   {
      sessionListeners.remove(listener);
      connector.removeSessionListener(listener);
   }


   public PacketDispatcher getPacketDispatcher()
   {
      return connector.getDispatcher();
   }

   public Location getLocation()
   {
      return location;
   }
   
   public MessagingBuffer createBuffer(final int size)
   {
      return connector.createBuffer(size);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void checkConnected() throws MessagingException
   {
      if (session == null)
      {
         throw new IllegalStateException("Client " + this
                 + " is not connected.");
      }
      if (!session.isConnected())
      {
         throw new MessagingException(MessagingException.NOT_CONNECTED);
      }
   }
}
