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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jboss.messaging.core.remoting.ConnectorRegistrySingleton.REGISTRY;

import org.jboss.messaging.core.client.FailureListener;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;

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

   private final Configuration config;

   private NIOConnector connector;
   
   private NIOSession session;
   
   private FailureListener listener;

   private transient PacketDispatcher dispatcher;

   // Constructors ---------------------------------------------------------------------------------

   public RemotingConnectionImpl(final Configuration config, final PacketDispatcher dispatcher) throws Exception
   {
      assert config != null;
      assert dispatcher != null;
      
      this.config = config;
      this.dispatcher = dispatcher;
      
      log.trace(this + " created with configuration " + config);
   }

   // Public ---------------------------------------------------------------------------------------

   // RemotingConnection implementation ------------------------------------------------------------
   
   public void start() throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace(this + " started remoting connection"); }

      connector = REGISTRY.getConnector(config, dispatcher);
      session = connector.connect();

      if (log.isDebugEnabled())
         log.debug("Using " + connector + " to connect to " + config);

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
               connector.removeFailureListener(listener);
            NIOConnector connectorFromRegistry = REGISTRY.removeConnector(config);
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
   
   public String getSessionID()
   {
      if (session == null || !session.isConnected())
      {
         return null;
      }
      return session.getID();
   }
    
   public Packet send(String targetID, Packet packet)
         throws MessagingException
   {
      return send(targetID, targetID, packet);
   }

   /**
    * send the packet and block until a response is received (<code>oneWay</code> is set to <code>false</code>)
    */
   public Packet send(final String targetID, final String executorID, final Packet packet) throws MessagingException
   {
      return send(targetID, executorID, packet, false);
   }
   
   public Packet send(final String targetID, final String executorID, final Packet packet, final boolean oneWay) throws MessagingException
   {
      assert packet != null;

      packet.setTargetID(targetID);
      packet.setExecutorID(executorID);
      
      Packet response;
      
      try
      {      
         response = (Packet) send(packet, oneWay);
      }
      catch (Exception e)
      {
         log.error("Caught unexpected exception", e);
         
         throw new MessagingException(MessagingException.INTERNAL_ERROR);
      }
      
      if (oneWay == false && response == null)
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
   
   public synchronized void setFailureListener(final FailureListener newListener)
   {
      if (listener != null && newListener != null)
      {
         throw new IllegalStateException("FailureListener already set to " + listener);
      }

      if (newListener != null)
      {
         connector.addFailureListener(newListener);
      }
      else 
      {
         connector.removeFailureListener(listener);
      }
      this.listener = newListener;
   }
   
   public PacketDispatcher getPacketDispatcher()
   {
      return dispatcher;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private Packet send(final Packet packet, final boolean oneWay) throws Exception
   {
      assert packet != null;
      checkConnected();
      packet.setOneWay(oneWay);

      if (oneWay)
      {
         session.write(packet);
         return null;
      } else 
      {
         Packet response = (Packet) session.writeAndBlock(packet, 
               config.getTimeout(), SECONDS);
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
