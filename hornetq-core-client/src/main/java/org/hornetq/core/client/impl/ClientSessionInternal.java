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

package org.hornetq.core.client.impl;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * A ClientSessionInternal
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ClientSessionInternal extends ClientSession
{
   String getName();

   void acknowledge(long consumerID, long messageID) throws HornetQException;

   void individualAcknowledge(long consumerID, long messageID) throws HornetQException;

   boolean isCacheLargeMessageClient();

   int getMinLargeMessageSize();

   boolean isCompressLargeMessages();

   void expire(long consumerID, long messageID) throws HornetQException;

   void addConsumer(ClientConsumerInternal consumer);

   void addProducer(ClientProducerInternal producer);

   void removeConsumer(ClientConsumerInternal consumer) throws HornetQException;

   void removeProducer(ClientProducerInternal producer);

   void handleReceiveMessage(long consumerID, SessionReceiveMessage message) throws Exception;

   void handleReceiveLargeMessage(long consumerID, SessionReceiveLargeMessage message) throws Exception;

   void handleReceiveContinuation(long consumerID, SessionReceiveContinuationMessage continuation) throws Exception;

   void preHandleFailover(CoreRemotingConnection connection);

   void handleFailover(CoreRemotingConnection backupConnection, HornetQException cause);

   RemotingConnection getConnection();

   Channel getChannel();

   void cleanUp(boolean failingOver) throws HornetQException;

   void returnBlocking();

   void setForceNotSameRM(boolean force);

   ClientSessionFactoryInternal getSessionFactory();

   void workDone();

   void forceDelivery(long consumerID, long sequence) throws HornetQException;

   void sendProducerCreditsMessage(int credits, SimpleString address);

   ClientProducerCredits getCredits(SimpleString address, boolean anon);

   void returnCredits(SimpleString address);

   void handleReceiveProducerCredits(SimpleString address, int credits);

   void handleReceiveProducerFailCredits(SimpleString address, int credits);

   ClientProducerCreditManager getProducerCreditManager();

   /** This will set the address at the message */
   void setAddress(Message message, SimpleString address);

   void checkDefaultAddress(SimpleString address);

   void setPacketSize(int packetSize);

   void resetIfNeeded() throws HornetQException;

   void markRollbackOnly();

   /** This is used internally to control and educate the user
    *  about using the thread boundaries properly.
    *  if more than one thread is using the session simultaneously
    *  this will generate a big warning on the docs.
    *  There are a limited number of places where we can call this such as acks and sends. otherwise we
    *  could get false warns
    *  */
   void startCall();

   /**
    * @see #startCall()
    */
   void endCall();

   /**
    * Sets a stop signal to true. This will cancel
    */
   void setStopSignal();
}
