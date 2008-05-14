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
package org.jboss.messaging.core.client.impl;

import static org.jboss.messaging.core.remoting.ConnectorRegistrySingleton.REGISTRY;

import java.util.Timer;
import java.util.TimerTask;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;

/**
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class RemotingConnectionImpl implements RemotingConnection
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RemotingConnectionImpl.class);
   
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final Location location;

   private final ConnectionParams connectionParams;

   private NIOConnector connector;
   
   private NIOSession session;
   
   private RemotingSessionListener listener;

   private transient PacketDispatcher dispatcher;
   
   // Constructors ---------------------------------------------------------------------------------

   public RemotingConnectionImpl(final Location location, ConnectionParams connectionParams, final PacketDispatcher dispatcher) throws Exception
   {
      assert location != null;
      assert dispatcher != null;
      assert connectionParams != null;
      
      this.location = location;
      this.connectionParams = connectionParams;
      this.dispatcher = dispatcher;
      
      log.trace(this + " created with configuration " + location);
   }

   // Public ---------------------------------------------------------------------------------------

   // RemotingConnection implementation ------------------------------------------------------------
   
   public void start() throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace(this + " started remoting connection"); }

      connector = REGISTRY.getConnector(location, connectionParams, dispatcher);
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
            if (listener != null)
               connector.removeSessionListener(listener);
            NIOConnector connectorFromRegistry = REGISTRY.removeConnector(location);
            if (connectorFromRegistry != null)
               connectorFromRegistry.disconnect();
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
      
      long handlerID = dispatcher.generateID();
      
      ResponseHandler handler = new ResponseHandler(handlerID);
      
      dispatcher.register(handler);
      
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
         
         Packet response = handler.waitForResponse(1000 * connectionParams.getTimeout());
         
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
         dispatcher.unregister(handlerID);
      }           
   }
   
   public void sendOneWay(final long targetID, final long executorID, final Packet packet) throws MessagingException
   {
      assert packet != null;

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
   
   public synchronized void setRemotingSessionListener(final RemotingSessionListener newListener)
   {
      if (listener != null && newListener != null)
      {
         throw new IllegalStateException("FailureListener already set to " + listener);
      }

      if (newListener != null)
      {
         connector.addSessionListener(newListener);
      }
      else 
      {
         connector.removeSessionListener(listener);
      }
      this.listener = newListener;
   }
   
   public PacketDispatcher getPacketDispatcher()
   {
      return dispatcher;
   }
   
   public Location getLocation()
   {
   	return location;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
      
   private static class ResponseHandler implements PacketHandler
   {
      private long id;
      
      private Packet response;
      
      ResponseHandler(final long id)
      {
         this.id = id;
      }

      public long getID()
      {
         return id;
      }

      public synchronized void handle(final Packet packet, final PacketReturner sender)
      {
         this.response = packet;
         
         notify();
      }
      
      public synchronized Packet waitForResponse(final long timeout)
      {
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
         
         return response;         
      }
      
   }

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
