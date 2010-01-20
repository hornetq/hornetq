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

import java.util.List;

import javax.transaction.xa.Xid;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.RemotingConnection;

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

   String getUsername();

   String getPassword();

   int getMinLargeMessageSize();

   Object getConnectionID();

   void removeConsumer(ServerConsumer consumer) throws Exception;

   void close() throws Exception;

   void handleAcknowledge(long consumerID, long messageID) throws Exception;

   void handleExpired(long consumerID, long messageID) throws Exception;

   void handleRollback(boolean considerLastMessageAsDelivered) throws Exception;

   void handleCommit() throws Exception;

   void handleXACommit(Xid xid, boolean onePhase) throws Exception;

   void handleXAEnd(Xid xid) throws Exception;

   void handleXAForget(Xid xid) throws Exception;

   void handleXAJoin(Xid xid) throws Exception;

   void handleXAPrepare(Xid xid) throws Exception;

   void handleXAResume(Xid xid) throws Exception;

   void handleXARollback(Xid xid) throws Exception;

   void handleXAStart(Xid xid) throws Exception;

   void handleXASuspend() throws Exception;

   List<Xid> handleGetInDoubtXids();

   int handleGetXATimeout();

   void handleSetXATimeout(int timeout);

   void handleStart();

   void handleStop();

   void handleCreateQueue(SimpleString address,
                          SimpleString name,
                          SimpleString filterString,
                          boolean temporary,
                          boolean durable) throws Exception;

   void handleDeleteQueue(SimpleString name) throws Exception;

   void handleCreateConsumer(long consumerID, SimpleString name, SimpleString filterString, boolean browseOnly) throws Exception;

   QueueQueryResult handleExecuteQueueQuery(SimpleString name) throws Exception;

   BindingQueryResult handleExecuteBindingQuery(SimpleString address);

   void handleCloseConsumer(long consumerID) throws Exception;

   void handleReceiveConsumerCredits(long consumerID, int credits) throws Exception;

   void handleSendContinuations(int packetSize, byte[] body, boolean continues) throws Exception;

   void handleSend(ServerMessage message) throws Exception;

   void handleSendLargeMessage(byte[] largeMessageHeader) throws Exception;

   void handleForceConsumerDelivery(long consumerID, long sequence) throws Exception;

   void handleRequestProducerCredits(SimpleString address, int credits) throws Exception;

   void handleClose() throws Exception;

   int transferConnection(RemotingConnection newConnection, int lastReceivedCommandID);

   Channel getChannel();

   void setTransferring(boolean transferring);
   
   void runConnectionFailureRunners();
   
   void setCallback(SessionCallback callback);
}
