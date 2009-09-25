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

package org.hornetq.core.server.impl;

import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.CREATE_QUEUE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.DELETE_QUEUE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_ACKNOWLEDGE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_BINDINGQUERY;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_CLOSE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_COMMIT;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_CONSUMER_CLOSE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_CREATECONSUMER;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_EXPIRED;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_FLOWTOKEN;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_QUEUEQUERY;
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
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_INDOUBT_XIDS;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_JOIN;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_PREPARE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_RESUME;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_ROLLBACK;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SET_TIMEOUT;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_START;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SUSPEND;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.impl.wireformat.CreateQueueMessage;
import org.hornetq.core.remoting.impl.wireformat.RollbackMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionExpiredMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendLargeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionSendMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.hornetq.core.server.ServerSession;

/**
 * A ServerSessionPacketHandler
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 */
public class ServerSessionPacketHandler implements ChannelHandler
{
   private static final Logger log = Logger.getLogger(ServerSessionPacketHandler.class);

   private final ServerSession session;

   public ServerSessionPacketHandler(final ServerSession session)
   {
      this.session = session;
   }

   public long getID()
   {
      return session.getID();
   }

   public void handlePacket(final Packet packet)
   {
      byte type = packet.getType();

      try
      {
         switch (type)
         {
            case SESS_CREATECONSUMER:
            {
               SessionCreateConsumerMessage request = (SessionCreateConsumerMessage)packet;
               session.handleCreateConsumer(request);
               break;
            }
            case CREATE_QUEUE:
            {
               CreateQueueMessage request = (CreateQueueMessage)packet;
               session.handleCreateQueue(request);
               break;
            }
            case DELETE_QUEUE:
            {
               SessionDeleteQueueMessage request = (SessionDeleteQueueMessage)packet;
               session.handleDeleteQueue(request);
               break;
            }
            case SESS_QUEUEQUERY:
            {
               SessionQueueQueryMessage request = (SessionQueueQueryMessage)packet;
               session.handleExecuteQueueQuery(request);
               break;
            }
            case SESS_BINDINGQUERY:
            {
               SessionBindingQueryMessage request = (SessionBindingQueryMessage)packet;
               session.handleExecuteBindingQuery(request);
               break;
            }
            case SESS_ACKNOWLEDGE:
            {
               SessionAcknowledgeMessage message = (SessionAcknowledgeMessage)packet;
               session.handleAcknowledge(message);
               break;
            }
            case SESS_EXPIRED:
            {
               SessionExpiredMessage message = (SessionExpiredMessage)packet;
               session.handleExpired(message);
               break;
            }
            case SESS_COMMIT:
            {
               session.handleCommit(packet);
               break;
            }
            case SESS_ROLLBACK:
            {
               session.handleRollback((RollbackMessage)packet);
               break;
            }
            case SESS_XA_COMMIT:
            {
               SessionXACommitMessage message = (SessionXACommitMessage)packet;
               session.handleXACommit(message);
               break;
            }
            case SESS_XA_END:
            {
               SessionXAEndMessage message = (SessionXAEndMessage)packet;
               session.handleXAEnd(message);
               break;
            }
            case SESS_XA_FORGET:
            {
               SessionXAForgetMessage message = (SessionXAForgetMessage)packet;
               session.handleXAForget(message);
               break;
            }
            case SESS_XA_JOIN:
            {
               SessionXAJoinMessage message = (SessionXAJoinMessage)packet;
               session.handleXAJoin(message);
               break;
            }
            case SESS_XA_RESUME:
            {
               SessionXAResumeMessage message = (SessionXAResumeMessage)packet;
               session.handleXAResume(message);
               break;
            }
            case SESS_XA_ROLLBACK:
            {
               SessionXARollbackMessage message = (SessionXARollbackMessage)packet;
               session.handleXARollback(message);
               break;
            }
            case SESS_XA_START:
            {
               SessionXAStartMessage message = (SessionXAStartMessage)packet;
               session.handleXAStart(message);
               break;
            }
            case SESS_XA_SUSPEND:
            {
               session.handleXASuspend(packet);
               break;
            }
            case SESS_XA_PREPARE:
            {
               SessionXAPrepareMessage message = (SessionXAPrepareMessage)packet;
               session.handleXAPrepare(message);
               break;
            }
            case SESS_XA_INDOUBT_XIDS:
            {
               session.handleGetInDoubtXids(packet);
               break;
            }
            case SESS_XA_GET_TIMEOUT:
            {
               session.handleGetXATimeout(packet);
               break;
            }
            case SESS_XA_SET_TIMEOUT:
            {
               SessionXASetTimeoutMessage message = (SessionXASetTimeoutMessage)packet;
               session.handleSetXATimeout(message);
               break;
            }
            case SESS_START:
            {
               session.handleStart(packet);
               break;
            }
            case SESS_STOP:
            {
               session.handleStop(packet);
               break;
            }
            case SESS_CLOSE:
            {
               session.handleClose(packet);
               break;
            }
            case SESS_CONSUMER_CLOSE:
            {
               SessionConsumerCloseMessage message = (SessionConsumerCloseMessage)packet;
               session.handleCloseConsumer(message);
               break;
            }
            case SESS_FLOWTOKEN:
            {
               SessionConsumerFlowCreditMessage message = (SessionConsumerFlowCreditMessage)packet;
               session.handleReceiveConsumerCredits(message);
               break;
            }
            case SESS_SEND:
            {
               SessionSendMessage message = (SessionSendMessage)packet;
               session.handleSend(message);
               break;
            }
            case SESS_SEND_LARGE:
            {
               SessionSendLargeMessage message = (SessionSendLargeMessage)packet;
               session.handleSendLargeMessage(message);
               break;
            }
            case SESS_SEND_CONTINUATION:
            {
               SessionSendContinuationMessage message = (SessionSendContinuationMessage)packet;
               session.handleSendContinuations(message);
               break;
            }
         }
      }
      catch (Throwable t)
      {
         log.error("Caught unexpected exception", t);
      }
   }
}
