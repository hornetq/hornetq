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
import java.util.Set;

import javax.transaction.xa.Xid;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.json.JSONArray;

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

   RemotingConnection getRemotingConnection();

   void removeConsumer(long consumerID) throws Exception;

   void acknowledge(long consumerID, long messageID) throws Exception;

   void individualAcknowledge(long consumerID, long messageID) throws Exception;

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

   void xaFailed(Xid xid) throws Exception;

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

   void createConsumer(long consumerID, SimpleString queueName, SimpleString filterString, boolean browseOnly) throws Exception;

   QueueQueryResult executeQueueQuery(SimpleString name) throws Exception;

   BindingQueryResult executeBindingQuery(SimpleString address) throws Exception;

   void closeConsumer(long consumerID) throws Exception;

   void receiveConsumerCredits(long consumerID, int credits) throws Exception;

   void sendContinuations(int packetSize, long totalBodySize, byte[] body, boolean continues) throws Exception;

   void send(ServerMessage message, boolean direct) throws Exception;

   void sendLarge(MessageInternal msg) throws Exception;

   void forceConsumerDelivery(long consumerID, long sequence) throws Exception;

   void requestProducerCredits(SimpleString address, int credits) throws Exception;

   void close(boolean failed) throws Exception;

   void waitContextCompletion() throws Exception;

   void setTransferring(boolean transferring);

   Set<ServerConsumer> getServerConsumers();

   void addMetaData(String key, String data);

   boolean addUniqueMetaData(String key, String data);

   String getMetaData(String key);

   String[] getTargetAddresses();

   /**
    * Add all the producers detail to the JSONArray object.
    * This is a method to be used by the management layer.
    * @param objs
    * @throws Exception
    */
   void describeProducersInfo(JSONArray objs) throws Exception;

   String getLastSentMessageID(String address);

   long getCreationTime();

   OperationContext getSessionContext();

    List<MessageReference> getInTXMessagesForConsumer(long consumerId);
}
