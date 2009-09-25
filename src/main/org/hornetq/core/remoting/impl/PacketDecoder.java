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

package org.hornetq.core.remoting.impl;

import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.CREATESESSION;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.CREATESESSION_RESP;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.CREATE_QUEUE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.DELETE_QUEUE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.DISCONNECT;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.EXCEPTION;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.NULL_RESPONSE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.PACKETS_CONFIRMED;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.PING;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.REATTACH_SESSION;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.REATTACH_SESSION_RESP;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_ACKNOWLEDGE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_BINDINGQUERY;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_BINDINGQUERY_RESP;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_CLOSE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_COMMIT;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_CONSUMER_CLOSE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_CREATECONSUMER;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_EXPIRED;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_FLOWTOKEN;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_QUEUEQUERY;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_QUEUEQUERY_RESP;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_RECEIVE_CONTINUATION;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_RECEIVE_MSG;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_ROLLBACK;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_SEND;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_SEND_CONTINUATION;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_SEND_LARGE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_START;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_STOP;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_COMMIT;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_END;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_FORGET;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_GET_TIMEOUT;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_GET_TIMEOUT_RESP;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_INDOUBT_XIDS;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_INDOUBT_XIDS_RESP;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_JOIN;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_PREPARE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_RESP;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_RESUME;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_ROLLBACK;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SET_TIMEOUT;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SET_TIMEOUT_RESP;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_START;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SUSPEND;

import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.impl.wireformat.CreateQueueMessage;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.remoting.impl.wireformat.NullResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.PacketsConfirmedMessage;
import org.hornetq.core.remoting.impl.wireformat.Ping;
import org.hornetq.core.remoting.impl.wireformat.ReattachSessionMessage;
import org.hornetq.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.RollbackMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionCloseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionExpiredMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendLargeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.hornetq.core.remoting.spi.HornetQBuffer;

/**
 * A PacketDecoder
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class PacketDecoder
{
   public Packet decode(final HornetQBuffer in)
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
            packet = new PacketImpl(DISCONNECT);
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
            packet = new SessionExpiredMessage();
            break;
         }
         case SESS_COMMIT:
         {
            packet = new PacketImpl(PacketImpl.SESS_COMMIT);
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
            packet = new SessionSendLargeMessage();
            break;
         }
         case SESS_RECEIVE_MSG:
         {
            packet = new SessionReceiveMessage();
            break;
         }
         case SESS_CONSUMER_CLOSE:
         {
            packet = new SessionConsumerCloseMessage();
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
         default:
         {
            throw new IllegalArgumentException("Invalid type: " + packetType);
         }
      }

      packet.decode(in);

      return packet;
   }

}
