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

import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONS_CHANGERATE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CLOSE;

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
import org.jboss.messaging.core.remoting.wireformat.ConsumerChangeRateMessage;
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

   private String id;

   private Queue messageQueue;

   private ServerSessionEndpoint sessionEndpoint;

   private boolean noLocal;

   private Filter filter;

   private boolean started;

   // This lock protects starting and stopping
   private Object startStopLock;

   // Must be volatile
   private volatile boolean clientAccepting;

   private int prefetchSize;
   
   private volatile int sendCount;
   
   private boolean firstTime = true;
   
   private boolean autoDeleteQueue;

   // Constructors ---------------------------------------------------------------------------------

   ServerConsumerEndpoint(MessagingServer sp, String id, Queue messageQueue,                          
					           ServerSessionEndpoint sessionEndpoint, Filter filter,
					           boolean noLocal, 
					           int prefetchSize, boolean autoDeleteQueue)
   {
      if (trace)
      {
         log.trace("constructing consumer endpoint " + id);
      }

      this.id = id;

      this.messageQueue = messageQueue;

      this.sessionEndpoint = sessionEndpoint;

      this.noLocal = noLocal;

      // Always start as false - wait for consumer to initiate.
      this.clientAccepting = false;
      
      this.startStopLock = new Object();

      this.prefetchSize = prefetchSize;
      
      this.filter = filter;
                
      this.started = this.sessionEndpoint.getConnectionEndpoint().isStarted();
      
      this.autoDeleteQueue = autoDeleteQueue;
      
      // adding the consumer to the queue
      messageQueue.addConsumer(this);
      
      messageQueue.deliver();

      log.trace(this + " constructed");
   }

   // Receiver implementation ----------------------------------------------------------------------

   public HandleStatus handle(MessageReference ref) throws Exception
   {
      if (trace)
      {
         log.trace(this + " receives " + ref + " for delivery");
      }
      
      // This is ok to have outside lock - is volatile
      if (!clientAccepting)
      {
         if (trace) { log.trace(this + " is NOT accepting messages!"); }

         return HandleStatus.BUSY;
      }

      if (ref.getMessage().isExpired())
      {         
         sessionEndpoint.expireDelivery(ref);
         
         return HandleStatus.HANDLED;
      }
      
      synchronized (startStopLock)
      {
         // If the consumer is stopped then we don't accept the message, it should go back into the
         // queue for delivery later.
         if (!started)
         {
            if (trace) { log.trace(this + " NOT started"); }

            return HandleStatus.BUSY;
         }
         
         if (trace) { log.trace(this + " has startStopLock lock, preparing the message for delivery"); }

         Message message = ref.getMessage();
         
         if (!accept(message))
         {
            return HandleStatus.NO_MATCH;
         }
         
         if (noLocal)
         {
            String conId = message.getConnectionID();

            if (trace) { log.trace("message connection id: " + conId + " current connection connection id: " + sessionEndpoint.getConnectionEndpoint().getConnectionID()); }

            if (sessionEndpoint.getConnectionEndpoint().getConnectionID().equals(conId))
            {
            	if (trace) { log.trace("Message from local connection so rejecting"); }
            	
            	PersistenceManager pm = sessionEndpoint.getConnectionEndpoint().getMessagingServer().getPersistenceManager();
            	            	            	
            	ref.acknowledge(pm);
            	
             	return HandleStatus.HANDLED;
            }            
         }
                  
         sendCount++;
         
         int num = prefetchSize;
         
         if (firstTime)
         {
            //We make sure we have a little extra buffer on the client side
            num = num + num / 3 ;
         }
         
         if (sendCount == num)
         {
            clientAccepting = false;
            
            firstTime = false;
         }          
                   
         try
         {
         	sessionEndpoint.handleDelivery(ref, this, replier);
         }
         catch (Exception e)
         {
         	log.error("Failed to handle delivery", e);
         	
         	this.started = false; // DO NOT return null or the message might get delivered more than once
         }
                          
         return HandleStatus.HANDLED;
      }
   }
   
   // Filter implementation ------------------------------------------------------------------------

   public boolean accept(Message msg)
   {
      if (filter != null)
      {
         boolean accept = filter.match(msg);

         if (trace) { log.trace("message filter " + (accept ? "accepts " : "DOES NOT accept ") + "the message"); }
         
         return accept;
      }
      else
      {
         return true;
      }
   }

   // Closeable implementation ---------------------------------------------------------------------

   public void close() throws Exception
   {
      if (trace)
      {
         log.trace(this + " close");
      }
      
      stop();

      localClose();

      sessionEndpoint.removeConsumer(id);
           
   }

   // ConsumerEndpoint implementation --------------------------------------------------------------

   public void changeRate(float newRate) throws Exception
   {
      if (trace)
      {
         log.trace(this + " changing rate to " + newRate);
      }

      if (newRate > 0)
      {
         sendCount = 0;
         
         clientAccepting = true;
      }
      else
      {
         clientAccepting = false;
      }

      if (clientAccepting)
      {
         promptDelivery();
      }
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
      //No need to lock since caller already has the lock
      this.started = started;      
   }
    
   void localClose() throws Exception
   {
      if (trace) { log.trace(this + " grabbed the main lock in close() " + this); }

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
   }

   void start()
   {
      synchronized (startStopLock)
      {
         if (started)
         {
            return;
         }

         started = true;
      }

      // Prompt delivery
      promptDelivery();
   }

   void stop() throws Exception
   {
      synchronized (startStopLock)
      {
         if (!started)
         {
            return;
         }

         started = false;         
      }
   }

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
         
         if (type == CONS_CHANGERATE)
         {
            setReplier(sender);

            ConsumerChangeRateMessage message = (ConsumerChangeRateMessage) packet;
            
            changeRate(message.getRate());
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
