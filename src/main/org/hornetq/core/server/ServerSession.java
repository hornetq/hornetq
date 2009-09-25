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

package org.hornetq.core.server;

import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
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
import org.hornetq.core.server.impl.ServerSessionPacketHandler;

/**
 *
 * A ServerSession
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 *
 */
public interface ServerSession
{
   String getName();

   long getID();

   String getUsername();

   String getPassword();

   int getMinLargeMessageSize();

   Object getConnectionID();

   void removeConsumer(ServerConsumer consumer) throws Exception;

   void close() throws Exception;

   void promptDelivery(Queue queue);

   void handleAcknowledge(final SessionAcknowledgeMessage packet);

   void handleExpired(final SessionExpiredMessage packet);

   void handleRollback(RollbackMessage packet);

   void handleCommit(Packet packet);

   void handleXACommit(SessionXACommitMessage packet);

   void handleXAEnd(SessionXAEndMessage packet);

   void handleXAForget(SessionXAForgetMessage packet);

   void handleXAJoin(SessionXAJoinMessage packet);

   void handleXAPrepare(SessionXAPrepareMessage packet);

   void handleXAResume(SessionXAResumeMessage packet);

   void handleXARollback(SessionXARollbackMessage packet);

   void handleXAStart(SessionXAStartMessage packet);

   void handleXASuspend(Packet packet);

   void handleGetInDoubtXids(Packet packet);

   void handleGetXATimeout(Packet packet);

   void handleSetXATimeout(SessionXASetTimeoutMessage packet);

   void handleStart(Packet packet);

   void handleStop(Packet packet);

   void handleCreateQueue(CreateQueueMessage packet);

   void handleDeleteQueue(SessionDeleteQueueMessage packet);

   void handleCreateConsumer(SessionCreateConsumerMessage packet);

   void handleExecuteQueueQuery(SessionQueueQueryMessage packet);

   void handleExecuteBindingQuery(SessionBindingQueryMessage packet);

   void handleCloseConsumer(SessionConsumerCloseMessage packet);

   void handleReceiveConsumerCredits(SessionConsumerFlowCreditMessage packet);

   void handleSendContinuations(SessionSendContinuationMessage packet);

   void handleSend(SessionSendMessage packet);

   void handleSendLargeMessage(SessionSendLargeMessage packet);

   void handleClose(Packet packet);

   int transferConnection(RemotingConnection newConnection, int lastReceivedCommandID);

   Channel getChannel();
   
   ServerSessionPacketHandler getHandler();
   
   void setHandler(ServerSessionPacketHandler handler);

}
