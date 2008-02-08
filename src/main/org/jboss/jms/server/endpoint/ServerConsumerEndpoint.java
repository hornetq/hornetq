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
package org.jboss.jms.server.endpoint;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONS_FLOWTOKEN;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.Consumer;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.HandleStatus;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

/**
 * Concrete implementation of a ClientConsumer. 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Partially derived from JBM 1.x version by:
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt> $Id$
 */
public class ServerConsumerEndpoint implements Consumer
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerEndpoint.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private final String id;

   private final Queue messageQueue;

   private final ServerSessionEndpoint sessionEndpoint;

   private final boolean noLocal;

   private final Filter filter;

   private boolean started;

   // This lock protects starting and stopping
   private final Object startStopLock;

   private final AtomicInteger availableTokens = new AtomicInteger(0);
   
   private final boolean autoDeleteQueue;
   
   private final boolean enableFlowControl;
   
   private final PersistenceManager persistenceManager;

   // Constructors ---------------------------------------------------------------------------------

   ServerConsumerEndpoint(MessagingServer sp, String id, Queue messageQueue,                          
					           ServerSessionEndpoint sessionEndpoint, Filter filter,
					           boolean noLocal, boolean autoDeleteQueue, boolean enableFlowControl)
   {
      this.id = id;

      this.messageQueue = messageQueue;

      this.sessionEndpoint = sessionEndpoint;

      this.noLocal = noLocal;

      this.startStopLock = new Object();

      this.filter = filter;
                
      this.started = this.sessionEndpoint.getConnectionEndpoint().isStarted();
      
      this.autoDeleteQueue = autoDeleteQueue;
      
      this.enableFlowControl = enableFlowControl;
      
      this.persistenceManager = sessionEndpoint.getConnectionEndpoint().getMessagingServer().getPersistenceManager();
      
      // adding the consumer to the queue
      messageQueue.addConsumer(this);
      
      messageQueue.deliver();
   }

   // Receiver implementation ----------------------------------------------------------------------

   public HandleStatus handle(MessageReference ref) throws Exception
   {
      if (enableFlowControl && availableTokens.get() == 0)
      {
         if (trace) { log.trace(this + " is NOT accepting messages!"); }

         return HandleStatus.BUSY;
      }

      if (ref.getMessage().isExpired())
      {         
         ref.expire(persistenceManager);
         
         return HandleStatus.HANDLED;
      }
      
      synchronized (startStopLock)
      {
         // If the consumer is stopped then we don't accept the message, it should go back into the
         // queue for delivery later.
         if (!started)
         {
            return HandleStatus.BUSY;
         }
         
         Message message = ref.getMessage();
         
         if (filter != null && !filter.match(message))
         {
            return HandleStatus.NO_MATCH;
         }
         
         if (noLocal)
         {
            String conId = message.getConnectionID();

            if (sessionEndpoint.getConnectionEndpoint().getConnectionID().equals(conId))
            {
            	PersistenceManager pm = sessionEndpoint.getConnectionEndpoint().getMessagingServer().getPersistenceManager();
            	            	            	
            	ref.acknowledge(pm);
            	
             	return HandleStatus.HANDLED;
            }            
         }
                         
         if (enableFlowControl)
         {
            availableTokens.decrementAndGet();
         }
                   
         try
         {
         	sessionEndpoint.handleDelivery(ref, this, replier);
         }
         catch (Exception e)
         {
         	log.error("Failed to handle delivery", e);
         	
         	started = false; // DO NOT return null or the message might get delivered more than once
         }
                          
         return HandleStatus.HANDLED;
      }
   }
   
   // Closeable implementation ---------------------------------------------------------------------

   public void close() throws Exception
   {
      if (trace)
      {
         log.trace(this + " close");
      }
      
      setStarted(false);

      messageQueue.removeConsumer(this);
      
      sessionEndpoint.getConnectionEndpoint().getMessagingServer().getRemotingService().getDispatcher().unregister(id);     
      
      if (autoDeleteQueue)
      {
         if (messageQueue.getConsumerCount() == 0)
         {
            MessagingServer server = sessionEndpoint.getConnectionEndpoint().getMessagingServer();
            
            server.getPostOffice().removeBinding(messageQueue.getName());
            
            if (messageQueue.isDurable())
            {
               server.getPersistenceManager().deleteAllReferences(messageQueue);
            }
         }
      }
      
      sessionEndpoint.removeConsumer(id);           
   }

   // ConsumerEndpoint implementation --------------------------------------------------------------

   public void receiveTokens(int tokens) throws Exception
   {
      availableTokens.addAndGet(tokens);

      promptDelivery();      
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ConsumerEndpoint[" + id + "]";
   }

   public PacketHandler newHandler()
   {
      return new ServerConsumerEndpointPacketHandler();
   }

   // Package protected ----------------------------------------------------------------------------
   
   String getID()
   {
   	return this.id;
   }

   void setStarted(boolean started)
   {
      boolean useStarted;
      
      synchronized (startStopLock)
      {
         this.started = started;   
         
         useStarted = started;         
      }
      
      //Outside the lock
      if (useStarted)
      {
         promptDelivery();
      }
   }
    
//   void localClose() throws Exception
//   {
//      if (trace) { log.trace(this + " grabbed the main lock in close() " + this); }
//
//      messageQueue.removeConsumer(this);
//      
//      sessionEndpoint.getConnectionEndpoint().getMessagingServer().getRemotingService().getDispatcher().unregister(id);     
//      
//      if (autoDeleteQueue)
//      {
//         if (messageQueue.getConsumerCount() == 0)
//         {
//            MessagingServer server = sessionEndpoint.getConnectionEndpoint().getMessagingServer();
//            
//            server.getPostOffice().removeBinding(messageQueue.getName());
//            
//            if (messageQueue.isDurable())
//            {
//               server.getPersistenceManager().deleteAllReferences(messageQueue);
//            }
//         }
//      }
//   }

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void promptDelivery()
   {
      sessionEndpoint.promptDelivery(messageQueue);
   }

   private PacketSender replier;

   //FIXME - this is a hack - we shouldn't have to wait for a change rate message before we can send
   //a message to the client
   private void setReplier(PacketSender replier)
   {
      this.replier = replier;
   }

   // Inner classes --------------------------------------------------------------------------------
   
   private class ServerConsumerEndpointPacketHandler extends ServerPacketHandlerSupport
   {

      public String getID()
      {
         return ServerConsumerEndpoint.this.id;
      }

      public Packet doHandle(Packet packet, PacketSender sender) throws Exception
      {
         Packet response = null;

         PacketType type = packet.getType();
         
         if (type == CONS_FLOWTOKEN)
         {
            setReplier(sender);

            ConsumerFlowTokenMessage message = (ConsumerFlowTokenMessage) packet;
            
            receiveTokens(message.getTokens());
         }
         else if (type == CLOSE)
         {
            close();
            
            setReplier(null);
         }
         else
         {
            throw new MessagingException(MessagingException.UNSUPPORTED_PACKET,
                  "Unsupported packet " + type);
         }

         // reply if necessary
         if (response == null && packet.isOneWay() == false)
         {
            response = new NullPacket();               
         }
         
         return response;
      }

      @Override
      public String toString()
      {
         return "ServerConsumerEndpointPacketHandler[id=" + id + "]";
      }
   }
}
