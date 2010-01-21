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

   void acknowledge(long consumerID, long messageID) throws Exception;

   void expire(long consumerID, long messageID) throws Exception;

   void rollback(boolean considerLastMessageAsDelivered) throws Exception;

   void commit() throws Exception;

   void xaCommit(Xid xid, boolean onePhase) throws Exception;

   void xaEnd(Xid xid) throws Exception;

   void xaForget(Xid xid) throws Exception;

   void xaJoin(Xid xid) throws Exception;

   void xaPrepare(Xid xid) throws Exception;

   void xaResume(Xid xid) throws Exception;

   void xaRollback(Xid xid) throws Exception;

   void xaStart(Xid xid) throws Exception;

   void xaSuspend() throws Exception;

   List<Xid> xaGetInDoubtXids();

   int xaGetTimeout();

   void xaSetTimeout(int timeout);

   void start();

   void stop();

   void createQueue(SimpleString address,
                          SimpleString name,
                          SimpleString filterString,
                          boolean temporary,
                          boolean durable) throws Exception;

   void deleteQueue(SimpleString name) throws Exception;

   void createConsumer(long consumerID, SimpleString name, SimpleString filterString, boolean browseOnly) throws Exception;

   QueueQueryResult executeQueueQuery(SimpleString name) throws Exception;

   BindingQueryResult executeBindingQuery(SimpleString address);

   void closeConsumer(long consumerID) throws Exception;

   void receiveConsumerCredits(long consumerID, int credits) throws Exception;

   void sendContinuations(int packetSize, byte[] body, boolean continues) throws Exception;

   void send(ServerMessage message) throws Exception;

   void sendLarge(byte[] largeMessageHeader) throws Exception;

   void forceConsumerDelivery(long consumerID, long sequence) throws Exception;

   void requestProducerCredits(SimpleString address, int credits) throws Exception;

   void close() throws Exception;

   void setTransferring(boolean transferring);
   
   void runConnectionFailureRunners();
   
   void setCallback(SessionCallback callback);
}
