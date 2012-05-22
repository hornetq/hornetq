/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.protocol.core.impl;

import static org.hornetq.core.protocol.core.impl.PacketImpl.CLUSTER_TOPOLOGY;
import static org.hornetq.core.protocol.core.impl.PacketImpl.CLUSTER_TOPOLOGY_V2;
import static org.hornetq.core.protocol.core.impl.PacketImpl.CREATESESSION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.CREATESESSION_RESP;
import static org.hornetq.core.protocol.core.impl.PacketImpl.CREATE_QUEUE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.DELETE_QUEUE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.DISCONNECT;
import static org.hornetq.core.protocol.core.impl.PacketImpl.EXCEPTION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.NODE_ANNOUNCE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.NULL_RESPONSE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.PACKETS_CONFIRMED;
import static org.hornetq.core.protocol.core.impl.PacketImpl.PING;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REATTACH_SESSION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REATTACH_SESSION_RESP;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_APPEND;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_APPEND_TX;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_COMMIT_ROLLBACK;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_COMPARE_DATA;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_DELETE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_DELETE_TX;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_LARGE_MESSAGE_BEGIN;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_LARGE_MESSAGE_END;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_LARGE_MESSAGE_WRITE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_PAGE_EVENT;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_PAGE_WRITE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_PREPARE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REPLICATION_RESPONSE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_ACKNOWLEDGE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_ADD_METADATA;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_ADD_METADATA2;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_BINDINGQUERY;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_BINDINGQUERY_RESP;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_CLOSE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_COMMIT;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_CONSUMER_CLOSE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_CREATECONSUMER;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_EXPIRED;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_FLOWTOKEN;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_FORCE_CONSUMER_DELIVERY;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_INDIVIDUAL_ACKNOWLEDGE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_PRODUCER_CREDITS;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_PRODUCER_REQUEST_CREDITS;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_QUEUEQUERY;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_QUEUEQUERY_RESP;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_CONTINUATION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_LARGE_MSG;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_RECEIVE_MSG;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_ROLLBACK;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_SEND;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_SEND_CONTINUATION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_SEND_LARGE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_START;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_STOP;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_UNIQUE_ADD_METADATA;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_COMMIT;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_END;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_FORGET;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_GET_TIMEOUT;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_GET_TIMEOUT_RESP;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_INDOUBT_XIDS;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_INDOUBT_XIDS_RESP;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_JOIN;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_PREPARE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_RESP;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_RESUME;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_ROLLBACK;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_SET_TIMEOUT;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_SET_TIMEOUT_RESP;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_START;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SESS_XA_SUSPEND;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SUBSCRIBE_TOPOLOGY;
import static org.hornetq.core.protocol.core.impl.PacketImpl.SUBSCRIBE_TOPOLOGY_V2;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.BackupRegistrationFailedMessage;
import org.hornetq.core.protocol.core.impl.wireformat.BackupRegistrationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V2;
import org.hornetq.core.protocol.core.impl.wireformat.CreateQueueMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.DisconnectMessage;
import org.hornetq.core.protocol.core.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.LiveIsStoppingMessage;
import org.hornetq.core.protocol.core.impl.wireformat.NodeAnnounceMessage;
import org.hornetq.core.protocol.core.impl.wireformat.NullResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.PacketsConfirmedMessage;
import org.hornetq.core.protocol.core.impl.wireformat.Ping;
import org.hornetq.core.protocol.core.impl.wireformat.ReattachSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReattachSessionResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationAddMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationAddTXMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationCommitMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationCompareDataMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationDeleteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationDeleteTXMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageBeginMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageEndMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageEventMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPrepareMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationSyncFileMessage;
import org.hornetq.core.protocol.core.impl.wireformat.RollbackMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionAcknowledgeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionAddMetaDataMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionAddMetaDataMessageV2;
import org.hornetq.core.protocol.core.impl.wireformat.SessionBindingQueryMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionBindingQueryResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionCloseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionCommitMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionConsumerCloseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionCreateConsumerMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionDeleteQueueMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionExpireMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionForceConsumerDelivery;
import org.hornetq.core.protocol.core.impl.wireformat.SessionIndividualAcknowledgeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionProducerCreditsMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionQueueQueryMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionRequestProducerCreditsMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionUniqueAddMetaDataMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXACommitMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXAEndMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXAForgetMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXAJoinMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXAPrepareMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXAResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXAResumeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXARollbackMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXASetTimeoutMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionXAStartMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessageV2;
import org.hornetq.core.server.HornetQMessageBundle;

/**
 * A PacketDecoder
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public final class PacketDecoder
{

   private PacketDecoder()
   {
      // Utility
   }

   public static Packet decode(final HornetQBuffer in)
   {
      final byte packetType = in.readByte();

      Packet packet;

      switch (packetType)
      {
         case PING:
         {
            packet = new Ping();
            break;
         }
         case DISCONNECT:
         {
            packet = new DisconnectMessage();
            break;
         }
         case EXCEPTION:
         {
            packet = new HornetQExceptionMessage();
            break;
         }
         case PACKETS_CONFIRMED:
         {
            packet = new PacketsConfirmedMessage();
            break;
         }
         case CREATESESSION:
         {
            packet = new CreateSessionMessage();
            break;
         }
         case CREATESESSION_RESP:
         {
            packet = new CreateSessionResponseMessage();
            break;
         }
         case REATTACH_SESSION:
         {
            packet = new ReattachSessionMessage();
            break;
         }
         case REATTACH_SESSION_RESP:
         {
            packet = new ReattachSessionResponseMessage();
            break;
         }
         case SESS_CLOSE:
         {
            packet = new SessionCloseMessage();
            break;
         }
         case SESS_CREATECONSUMER:
         {
            packet = new SessionCreateConsumerMessage();
            break;
         }
         case SESS_ACKNOWLEDGE:
         {
            packet = new SessionAcknowledgeMessage();
            break;
         }
         case SESS_EXPIRED:
         {
            packet = new SessionExpireMessage();
            break;
         }
         case SESS_COMMIT:
         {
            packet = new SessionCommitMessage();
            break;
         }
         case SESS_ROLLBACK:
         {
            packet = new RollbackMessage();
            break;
         }
         case SESS_QUEUEQUERY:
         {
            packet = new SessionQueueQueryMessage();
            break;
         }
         case SESS_QUEUEQUERY_RESP:
         {
            packet = new SessionQueueQueryResponseMessage();
            break;
         }
         case CREATE_QUEUE:
         {
            packet = new CreateQueueMessage();
            break;
         }
         case DELETE_QUEUE:
         {
            packet = new SessionDeleteQueueMessage();
            break;
         }
         case SESS_BINDINGQUERY:
         {
            packet = new SessionBindingQueryMessage();
            break;
         }
         case SESS_BINDINGQUERY_RESP:
         {
            packet = new SessionBindingQueryResponseMessage();
            break;
         }
         case SESS_XA_START:
         {
            packet = new SessionXAStartMessage();
            break;
         }
         case SESS_XA_END:
         {
            packet = new SessionXAEndMessage();
            break;
         }
         case SESS_XA_COMMIT:
         {
            packet = new SessionXACommitMessage();
            break;
         }
         case SESS_XA_PREPARE:
         {
            packet = new SessionXAPrepareMessage();
            break;
         }
         case SESS_XA_RESP:
         {
            packet = new SessionXAResponseMessage();
            break;
         }
         case SESS_XA_ROLLBACK:
         {
            packet = new SessionXARollbackMessage();
            break;
         }
         case SESS_XA_JOIN:
         {
            packet = new SessionXAJoinMessage();
            break;
         }
         case SESS_XA_SUSPEND:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
            break;
         }
         case SESS_XA_RESUME:
         {
            packet = new SessionXAResumeMessage();
            break;
         }
         case SESS_XA_FORGET:
         {
            packet = new SessionXAForgetMessage();
            break;
         }
         case SESS_XA_INDOUBT_XIDS:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
            break;
         }
         case SESS_XA_INDOUBT_XIDS_RESP:
         {
            packet = new SessionXAGetInDoubtXidsResponseMessage();
            break;
         }
         case SESS_XA_SET_TIMEOUT:
         {
            packet = new SessionXASetTimeoutMessage();
            break;
         }
         case SESS_XA_SET_TIMEOUT_RESP:
         {
            packet = new SessionXASetTimeoutResponseMessage();
            break;
         }
         case SESS_XA_GET_TIMEOUT:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
            break;
         }
         case SESS_XA_GET_TIMEOUT_RESP:
         {
            packet = new SessionXAGetTimeoutResponseMessage();
            break;
         }
         case SESS_START:
         {
            packet = new PacketImpl(PacketImpl.SESS_START);
            break;
         }
         case SESS_STOP:
         {
            packet = new PacketImpl(PacketImpl.SESS_STOP);
            break;
         }
         case SESS_FLOWTOKEN:
         {
            packet = new SessionConsumerFlowCreditMessage();
            break;
         }
         case SESS_SEND:
         {
            packet = new SessionSendMessage();
            break;
         }
         case SESS_SEND_LARGE:
         {
            // Using a ClientMessage, but that will be replaced later..
            // This is just to avoid reading a byte[] to read the message
            packet = new SessionSendLargeMessage();
            break;
         }
         case SESS_RECEIVE_MSG:
         {
            packet = new SessionReceiveMessage();
            break;
         }
         case SESS_RECEIVE_LARGE_MSG:
         {
            packet = new SessionReceiveLargeMessage();
            break;
         }
         case SESS_CONSUMER_CLOSE:
         {
            packet = new SessionConsumerCloseMessage();
            break;
         }
         case SESS_INDIVIDUAL_ACKNOWLEDGE:
         {
            packet = new SessionIndividualAcknowledgeMessage();
            break;
         }
         case NULL_RESPONSE:
         {
            packet = new NullResponseMessage();
            break;
         }
         case SESS_RECEIVE_CONTINUATION:
         {
            packet = new SessionReceiveContinuationMessage();
            break;
         }
         case SESS_SEND_CONTINUATION:
         {
            packet = new SessionSendContinuationMessage();
            break;
         }
         case SESS_PRODUCER_REQUEST_CREDITS:
         {
            packet = new SessionRequestProducerCreditsMessage();
            break;
         }
         case SESS_PRODUCER_CREDITS:
         {
            packet = new SessionProducerCreditsMessage();
            break;
         }
         case REPLICATION_APPEND:
         {
            packet = new ReplicationAddMessage();
            break;
         }
         case REPLICATION_APPEND_TX:
         {
            packet = new ReplicationAddTXMessage();
            break;
         }
         case REPLICATION_DELETE:
         {
            packet = new ReplicationDeleteMessage();
            break;
         }
         case REPLICATION_DELETE_TX:
         {
            packet = new ReplicationDeleteTXMessage();
            break;
         }
         case REPLICATION_PREPARE:
         {
            packet = new ReplicationPrepareMessage();
            break;
         }
         case REPLICATION_COMMIT_ROLLBACK:
         {
            packet = new ReplicationCommitMessage();
            break;
         }
         case REPLICATION_RESPONSE:
         {
            packet = new ReplicationResponseMessage();
            break;
         }
         case REPLICATION_PAGE_WRITE:
         {
            packet = new ReplicationPageWriteMessage();
            break;
         }
         case REPLICATION_PAGE_EVENT:
         {
            packet = new ReplicationPageEventMessage();
            break;
         }
         case REPLICATION_LARGE_MESSAGE_BEGIN:
         {
            packet = new ReplicationLargeMessageBeginMessage();
            break;
         }
         case REPLICATION_LARGE_MESSAGE_END:
         {
            packet = new ReplicationLargeMessageEndMessage();
            break;
         }
         case REPLICATION_LARGE_MESSAGE_WRITE:
         {
            packet = new ReplicationLargeMessageWriteMessage();
            break;
         }
         case REPLICATION_COMPARE_DATA:
         {
            packet = new ReplicationCompareDataMessage();
            break;
         }
         case SESS_FORCE_CONSUMER_DELIVERY:
         {
            packet = new SessionForceConsumerDelivery();
            break;
         }
         case CLUSTER_TOPOLOGY:
         {
            packet = new ClusterTopologyChangeMessage();
            break;
         }
         case CLUSTER_TOPOLOGY_V2:
         {
            packet = new ClusterTopologyChangeMessage_V2();
            break;
         }
         case NODE_ANNOUNCE:
         {
            packet = new NodeAnnounceMessage();
            break;
         }
         case SUBSCRIBE_TOPOLOGY:
         {
            packet = new SubscribeClusterTopologyUpdatesMessage();
            break;
         }
         case SUBSCRIBE_TOPOLOGY_V2:
         {
            packet = new SubscribeClusterTopologyUpdatesMessageV2();
            break;
         }
         case SESS_ADD_METADATA:
         {
            packet = new SessionAddMetaDataMessage();
            break;
         }
         case SESS_ADD_METADATA2:
         {
            packet = new SessionAddMetaDataMessageV2();
            break;
         }
         case SESS_UNIQUE_ADD_METADATA:
         {
            packet = new SessionUniqueAddMetaDataMessage();
            break;
         }
         case PacketImpl.BACKUP_REGISTRATION:
         {
            packet = new BackupRegistrationMessage();
            break;
         }
         case PacketImpl.BACKUP_REGISTRATION_FAILED:
         {
            packet = new BackupRegistrationFailedMessage();
            break;
         }
         case PacketImpl.REPLICATION_START_FINISH_SYNC:
         {
            packet = new ReplicationStartSyncMessage();
            break;
         }
         case PacketImpl.REPLICATION_SYNC_FILE:
         {
            packet = new ReplicationSyncFileMessage();
            break;
         }
         case PacketImpl.REPLICATION_SCHEDULED_FAILOVER:
         {
            packet = new LiveIsStoppingMessage();
            break;
         }
         default:
         {
            throw HornetQMessageBundle.BUNDLE.invalidType(packetType);
         }
      }

      packet.decode(in);

      return packet;
   }

}
