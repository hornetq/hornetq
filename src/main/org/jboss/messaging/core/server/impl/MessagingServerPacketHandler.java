/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

import static org.jboss.messaging.core.management.NotificationType.CONSUMER_CLOSED;
import static org.jboss.messaging.core.management.NotificationType.CONSUMER_CREATED;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.CREATESESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.CREATE_QUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.REATTACH_SESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.REPLICATE_CREATESESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.REPLICATE_STARTUP_INFO;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.Notification;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReplicateCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateRedistributionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateRemoteBindingAddedMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateRemoteBindingRemovedMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateRemoteConsumerAddedMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateRemoteConsumerRemovedMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateStartupInfoMessage;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.cluster.ClusterConnection;
import org.jboss.messaging.core.server.cluster.RemoteQueueBinding;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;

/**
 * A packet handler for all packets that need to be handled at the server level
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MessagingServerPacketHandler implements ChannelHandler
{
   private static final Logger log = Logger.getLogger(MessagingServerPacketHandler.class);

   private final MessagingServer server;

   private final Channel channel1;

   private final RemotingConnection connection;
   
   public MessagingServerPacketHandler(final MessagingServer server,
                                       final Channel channel1,
                                       final RemotingConnection connection)
   {
      this.server = server;

      this.channel1 = channel1;

      this.connection = connection;
   }

   public void handlePacket(final Packet packet)
   {
      byte type = packet.getType();
      
      if (!server.isInitialised() && type != PacketImpl.REPLICATE_STARTUP_INFO)
      {
         throw new IllegalStateException("First packet must be startup info for backup " + type);        
      }

      // All these operations need to be idempotent since they are outside of the session
      // reliability replay functionality
      switch (type)
      {
         case REPLICATE_STARTUP_INFO:
         {          
            ReplicateStartupInfoMessage msg = (ReplicateStartupInfoMessage)packet;
            
            try
            {
               server.initialiseBackup(msg.getNodeID(), msg.getCurrentMessageID());
            }
            catch (Exception e)
            {
               log.error("Failed to initialise", e);
            }
            
            break;
         }
         case CREATESESSION:
         {
            CreateSessionMessage request = (CreateSessionMessage)packet;

            handleCreateSession(request);

            break;
         }
         case REPLICATE_CREATESESSION:
         {
            ReplicateCreateSessionMessage request = (ReplicateCreateSessionMessage)packet;

            handleReplicateCreateSession(request);

            break;
         }
         case REATTACH_SESSION:
         {
            ReattachSessionMessage request = (ReattachSessionMessage)packet;

            handleReattachSession(request);

            break;
         }
         case CREATE_QUEUE:
         {
            // Create queue can also be fielded here in the case of a replicated store and forward queue creation

            CreateQueueMessage request = (CreateQueueMessage)packet;
            
            handleCreateQueue(request);

            break;
         }
         case PacketImpl.REPLICATE_ADD_REMOTE_QUEUE_BINDING:
         {
            ReplicateRemoteBindingAddedMessage request = (ReplicateRemoteBindingAddedMessage)packet;

            handleAddRemoteQueueBinding(request);

            break;
         }
         case PacketImpl.REPLICATE_REMOVE_REMOTE_QUEUE_BINDING:
         {
            ReplicateRemoteBindingRemovedMessage request = (ReplicateRemoteBindingRemovedMessage)packet;

            handleRemoveRemoteQueueBinding(request);

            break;
         }
         case PacketImpl.REPLICATE_ADD_REMOTE_CONSUMER:
         {
            ReplicateRemoteConsumerAddedMessage request = (ReplicateRemoteConsumerAddedMessage)packet;

            handleAddRemoteConsumer(request);

            break;
         }
         case PacketImpl.REPLICATE_REMOVE_REMOTE_CONSUMER:
         {
            ReplicateRemoteConsumerRemovedMessage request = (ReplicateRemoteConsumerRemovedMessage)packet;

            handleRemoveRemoteConsumer(request);

            break;
         }
         case PacketImpl.REPLICATE_ACKNOWLEDGE:
         {
            ReplicateAcknowledgeMessage request = (ReplicateAcknowledgeMessage)packet;

            handleReplicateAcknowledge(request);

            break;
         }
         case PacketImpl.REPLICATE_REDISTRIBUTION:
         {
            ReplicateRedistributionMessage message = (ReplicateRedistributionMessage)packet;
            
            handleReplicateRedistribution(message);
            
            break;
         }
         default:
         {
            log.error("Invalid packet " + packet);
         }
      }
   }

   private void doHandleCreateSession(final CreateSessionMessage request, final long oppositeChannelID)
   {
      Packet response;
      try
      {
         response = server.createSession(request.getName(),
                                         request.getSessionChannelID(),
                                         oppositeChannelID,
                                         request.getUsername(),
                                         request.getPassword(),
                                         request.getMinLargeMessageSize(),
                                         request.getVersion(),
                                         connection,
                                         request.isAutoCommitSends(),
                                         request.isAutoCommitAcks(),
                                         request.isPreAcknowledge(),
                                         request.isXA(),
                                         request.getWindowSize());
      }
      catch (Exception e)
      {
         log.error("Failed to create session", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel1.send(response);
   }

   private void handleCreateSession(final CreateSessionMessage request)
   {
      Channel replicatingChannel = server.getReplicatingChannel();

      if (replicatingChannel == null)
      {
         doHandleCreateSession(request, -1);
      }
      else
      {
         final long replicatedChannelID = replicatingChannel.getConnection().generateChannelID();

         Packet replPacket = new ReplicateCreateSessionMessage(request.getName(),
                                                               replicatedChannelID,
                                                               request.getSessionChannelID(),
                                                               request.getVersion(),
                                                               request.getUsername(),
                                                               request.getPassword(),
                                                               request.getMinLargeMessageSize(),
                                                               request.isXA(),
                                                               request.isAutoCommitSends(),
                                                               request.isAutoCommitAcks(),
                                                               request.isPreAcknowledge(),
                                                               request.getWindowSize());

         replicatingChannel.replicatePacket(replPacket, 1, new Runnable()
         {
            public void run()
            {
               doHandleCreateSession(request, replicatedChannelID);
            }
         });
      }
   }

   private void handleReplicateCreateSession(final ReplicateCreateSessionMessage request)
   {
      try
      {
         server.replicateCreateSession(request.getName(),
                                       request.getReplicatedSessionChannelID(),
                                       request.getOriginalSessionChannelID(),
                                       request.getUsername(),
                                       request.getPassword(),
                                       request.getMinLargeMessageSize(),
                                       request.getVersion(),
                                       connection,
                                       request.isAutoCommitSends(),
                                       request.isAutoCommitAcks(),
                                       request.isPreAcknowledge(),
                                       request.isXA(),
                                       request.getWindowSize());
      }
      catch (Exception e)
      {
         log.error("Failed to handle replicate create session", e);
      }
   }

   private void handleReattachSession(final ReattachSessionMessage request)
   {
      Packet response;

      try
      {
         response = server.reattachSession(connection, request.getName(), request.getLastReceivedCommandID());
      }
      catch (Exception e)
      {
         log.error("Failed to reattach session", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel1.send(response);
   }

   private void handleCreateQueue(final CreateQueueMessage request)
   {
      try
      {
         server.createQueue(request.getAddress(), request.getQueueName(), request.getFilterString(), request.isDurable(), request.isTemporary());
      }
      catch (Exception e)
      {
         log.error("Failed to handle create queue", e);
      }
   }

   private void handleAddRemoteQueueBinding(final ReplicateRemoteBindingAddedMessage request)
   {
      ClusterConnection cc = server.getClusterManager().getClusterConnection(request.getClusterConnectionName());

      if (cc == null)
      {
         throw new IllegalStateException("No cluster connection found with name " + request.getClusterConnectionName());
      }

      try
      {
         cc.handleReplicatedAddBinding(request.getAddress(),
                                       request.getUniqueName(),
                                       request.getRoutingName(),
                                       request.getRemoteQueueID(),
                                       request.getFilterString(),
                                       request.getSfQueueName(),
                                       request.getDistance());
      }
      catch (Exception e)
      {
         log.error("Failed to handle add remote queue binding", e);
      }
   }

   private void handleRemoveRemoteQueueBinding(final ReplicateRemoteBindingRemovedMessage request)
   {
      try
      {
         Binding binding = server.getPostOffice().removeBinding(request.getUniqueName());

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding to remove " + request.getUniqueName());
         }
      }
      catch (Exception e)
      {
         log.error("Failed to handle remove remote queue binding", e);
      }
   }

   private void handleAddRemoteConsumer(final ReplicateRemoteConsumerAddedMessage request)
   {
      RemoteQueueBinding binding = (RemoteQueueBinding)server.getPostOffice()
                                                             .getBinding(request.getUniqueBindingName());

      if (binding == null)
      {
         throw new IllegalStateException("Cannot find binding to remove " + request.getUniqueBindingName());
      }

      try
      {
         binding.addConsumer(request.getFilterString());
      }
      catch (Exception e)
      {
         log.error("Failed to handle add remote consumer", e);
      }
      
      // Need to propagate the consumer add
      Notification notification = new Notification(null, CONSUMER_CREATED, request.getProperties());

      try
      {
         server.getManagementService().sendNotification(notification);
      }
      catch (Exception e)
      {
         log.error("Failed to handle add remote consumer", e);
      }
   }

   private void handleRemoveRemoteConsumer(final ReplicateRemoteConsumerRemovedMessage request)
   {
      RemoteQueueBinding binding = (RemoteQueueBinding)server.getPostOffice()
                                                             .getBinding(request.getUniqueBindingName());

      if (binding == null)
      {
         throw new IllegalStateException("Cannot find binding to remove " + request.getUniqueBindingName());
      }

      try
      {
         binding.removeConsumer(request.getFilterString());
      }
      catch (Exception e)
      {
         log.error("Failed to handle remove remote consumer", e);
      }
      
      // Need to propagate the consumer close
      Notification notification = new Notification(null, CONSUMER_CLOSED, request.getProperties());

      try
      {
         server.getManagementService().sendNotification(notification);
      }
      catch (Exception e)
      {
         log.error("Failed to handle remove remote consumer", e);
      }
   }

   private void handleReplicateAcknowledge(final ReplicateAcknowledgeMessage request)
   {
      Binding binding = server.getPostOffice().getBinding(request.getUniqueName());

      if (binding == null)
      {
         throw new IllegalStateException("Cannot find binding " + request.getUniqueName());
      }

      try
      {
         Queue queue = (Queue)binding.getBindable();
         
         MessageReference ref = queue.removeFirstReference(request.getMessageID());
         
         queue.acknowledge(ref);
      }
      catch (Exception e)
      {
         log.error("Failed to handle remove remote consumer", e);
      }
   }
   
   private void handleReplicateRedistribution(final ReplicateRedistributionMessage request)
   {
      Binding binding = server.getPostOffice().getBinding(request.getQueueName());

      if (binding == null)
      {
         throw new IllegalStateException("Cannot find binding " + request.getQueueName());
      }

      try
      {
         server.handleReplicateRedistribution(request.getQueueName(), request.getMessageID());
      }
      catch (Exception e)
      {
         log.error("Failed to handle remove remote consumer", e);
      }
   }
}