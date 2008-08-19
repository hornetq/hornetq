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

package org.jboss.messaging.core.server.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;

/**
 * Concrete implementation of a ClientConsumer. 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision: 3783 $</tt> $Id: ServerConsumerImpl.java 3783 2008-02-25 12:15:14Z timfox $
 */
public class ServerConsumerImpl implements ServerConsumer
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerImpl.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final boolean trace = log.isTraceEnabled();

   private final long id;
   
   private final long clientTargetID;

   private final Queue messageQueue;
   
   private final Filter filter;
   
   private final ServerSession session;
         
   private final Object startStopLock = new Object();

   private final AtomicInteger availableCredits;
   
   private boolean started;
    
   private final StorageManager storageManager;
   
   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;
   
   private final PostOffice postOffice;
   
   private final boolean replicated = false;
   
   // Constructors ---------------------------------------------------------------------------------
 
   public ServerConsumerImpl(final ServerSession session, final long clientTargetID,
                      final Queue messageQueue, final Filter filter,
   		             final boolean enableFlowControl, final int maxRate, 
					       final boolean started,					      
					       final StorageManager storageManager,
					       final HierarchicalRepository<QueueSettings> queueSettingsRepository,
					       final PostOffice postOffice,
					       final PacketDispatcher dispatcher)
   {
   	this.clientTargetID = clientTargetID;
      
      this.messageQueue = messageQueue;
      
      this.filter = filter;
      
      this.session = session;              
      
      this.started = started;
      
      if (enableFlowControl)
      {
         availableCredits = new AtomicInteger(0);
      }
      else
      {
      	availableCredits = null;
      }
      
    //  this.remotingConnection = remotingConnection;
      
      this.storageManager = storageManager;
      
      this.queueSettingsRepository = queueSettingsRepository;
      
      this.postOffice = postOffice;
      
//      this.replicated = remotingConnection.isReplicated();
//      
//      if (replicated)
//      {
//         PacketDispatcher replicatingDispatcher =
//            remotingConnection.getReplicatingConnection().getPacketDispatcher();
//         replicatedDeliveryResponseHandler = new ReplicatedDeliveryResponseHandler(replicatingDispatcher.generateID());
//         
//         replicatingDispatcher.register(replicatedDeliveryResponseHandler);                  
//      }
      
      //Also increment the id on the local one so ids are in step
    //  dispatcher.generateID();
      
      this.id = dispatcher.generateID();
               
      messageQueue.addConsumer(this);
   }
   
   // ServerConsumer implementation ----------------------------------------------------------------------

   public long getID()
   {
   	return id;
   }
   
   public long getClientTargetID()
   {
      return clientTargetID;
   }
   
//   public void handleReplicatedDelivery(final long messageID, final long responseTargetID) throws Exception
//   {
//      MessageReference ref = messageQueue.removeFirst();
//      
//      //Sanity check - can remove once stable
//      if (ref.getMessage().getMessageID() != messageID)
//      {
//         throw new IllegalStateException("Message with id " + messageID + " should be at head of queue " +
//                  "but instead I found " + ref.getMessage().getMessageID());
//      }
//      
//      HandleStatus handled = handle(ref);
//      
//      //Sanity check
//      if (handled != HandleStatus.HANDLED)
//      {
//         throw new IllegalStateException("Should be handled");
//      }
//      
//      Packet response = new ConsumerReplicateDeliveryResponseMessage(messageID);
//      
//      response.setTargetID(responseTargetID);
//      
//      remotingConnection.sendOneWay(response);
//   }
   
   public void handleReplicatedDeliveryResponse(final long messageID) throws Exception
   {
      session.deliverDeferredDelivery(messageID);
   }
   
   public HandleStatus handle(MessageReference ref) throws Exception
   {                    
      if (availableCredits != null && availableCredits.get() <= 0)
      {
         return HandleStatus.BUSY;
      }
      
      if (ref.getMessage().isExpired())
      {         
         ref.expire(storageManager, postOffice, queueSettingsRepository);
         
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
         
         ServerMessage message = ref.getMessage();
         
         if (filter != null && !filter.match(message))
         {
            return HandleStatus.NO_MATCH;
         }
                          
         if (availableCredits != null)
         {
            availableCredits.addAndGet(-message.getEncodeSize());
         }
                   
         session.handleDelivery(ref, this);
         
//         if (replicated)
//         {
//            //Replicate the delivery
//            
//          server  Packet packet = new ConsumerReplicateDeliveryMessage(message.getMessageID());
//            
//            packet.setTargetID(this.id);
//            packet.setResponseTargetID(this.replicatedDeliveryResponseHandler.getID());
//            
//            //remotingConnection.replicatePacket(packet);
//         }
                  
         return HandleStatus.HANDLED;
      }
   }
   
   public void close() throws Exception
   {  
      setStarted(false);

      messageQueue.removeConsumer(this);
           
      session.removeConsumer(this);  
        
//      if (replicated)
//      {
//         PacketDispatcher replicatingDispatcher =
//            remotingConnection.getReplicatingConnection().getPacketDispatcher();
//         
//         replicatingDispatcher.unregister(replicatedDeliveryResponseHandler.getID());
//      }
   }
   
   public void setStarted(final boolean started)
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
   
   public void receiveCredits(final int credits) throws Exception
   {      
      if (availableCredits != null)
      {
         int previous = availableCredits.getAndAdd(credits);

         if (previous <= 0 && (previous + credits) > 0)
         {
            promptDelivery();
         }
      }  	
   }      
   
   public Queue getQueue()
   {
      return messageQueue;
   }

   // Public -----------------------------------------------------------------------------
     
   // Private --------------------------------------------------------------------------------------

   private void promptDelivery()
   {
      session.promptDelivery(messageQueue);
   } 
   
   // Inner classes ------------------------------------------------------------------------
   
//   private class ReplicatedDeliveryResponseHandler implements PacketHandler
//   {
//      ReplicatedDeliveryResponseHandler(final long id)
//      {
//         this.id = id;
//      }
//      
//      private final long id;
//
//      public long getID()
//      {
//         return id;
//      }
//
//      public void handle(final long connectionID, final Packet packet)
//      {
//         try
//         {
//            ConsumerReplicateDeliveryResponseMessage msg = (ConsumerReplicateDeliveryResponseMessage)packet;
//            
//            handleReplicatedDeliveryResponse(msg.getMessageID());
//         }
//         catch (Exception e)
//         {
//            log.error("Failed to handle replicate delivery response", e);
//         }
//      }
//      
//   }
}
