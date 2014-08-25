/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.spi.core.remoting;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.concurrent.Executor;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.core.client.impl.ClientConsumerInternal;
import org.hornetq.core.client.impl.ClientLargeMessageInternal;
import org.hornetq.core.client.impl.ClientMessageInternal;
import org.hornetq.core.client.impl.ClientProducerCreditsImpl;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.IDGenerator;
import org.hornetq.utils.SimpleIDGenerator;

/**
 * @author Clebert Suconic
 */

public abstract class SessionContext
{
   protected ClientSessionInternal session;

   protected SendAcknowledgementHandler sendAckHandler;

   protected volatile RemotingConnection remotingConnection;

   protected final IDGenerator idGenerator = new SimpleIDGenerator(0);


   public SessionContext(RemotingConnection remotingConnection)
   {
      this.remotingConnection = remotingConnection;
   }


   public ClientSessionInternal getSession()
   {
      return session;
   }

   public void setSession(ClientSessionInternal session)
   {
      this.session = session;
   }

   /**
    * it will eather reattach or reconnect, preferably reattaching it.
    *
    * @param newConnection
    * @return true if it was possible to reattach
    * @throws HornetQException
    */
   public abstract boolean reattachOnNewConnection(RemotingConnection newConnection) throws HornetQException;

   public RemotingConnection getRemotingConnection()
   {
      return remotingConnection;
   }


   public abstract void closeConsumer(ClientConsumer consumer) throws HornetQException;

   public abstract void sendConsumerCredits(ClientConsumer consumer, int credits);

   public abstract boolean supportsLargeMessage();

   protected void handleReceiveLargeMessage(ConsumerContext consumerID, ClientLargeMessageInternal clientLargeMessage, long largeMessageSize) throws Exception
   {
      ClientSessionInternal session = this.session;
      if (session != null)
      {
         session.handleReceiveLargeMessage(consumerID, clientLargeMessage, largeMessageSize);
      }
   }

   protected void handleReceiveMessage(ConsumerContext consumerID, final ClientMessageInternal message) throws Exception
   {

      ClientSessionInternal session = this.session;
      if (session != null)
      {
         session.handleReceiveMessage(consumerID, message);
      }
   }

   protected void handleReceiveContinuation(final ConsumerContext consumerID, byte[] chunk, int flowControlSize, boolean isContinues) throws Exception
   {
      ClientSessionInternal session = this.session;
      if (session != null)
      {
         session.handleReceiveContinuation(consumerID, chunk, flowControlSize, isContinues);
      }
   }

   protected void handleReceiveProducerCredits(SimpleString address, int credits)
   {
      ClientSessionInternal session = this.session;
      if (session != null)
      {
         session.handleReceiveProducerCredits(address, credits);
      }

   }

   protected void handleReceiveProducerFailCredits(SimpleString address, int credits)
   {
      ClientSessionInternal session = this.session;
      if (session != null)
      {
         session.handleReceiveProducerFailCredits(address, credits);
      }

   }

   public abstract int getCreditsOnSendingFull(MessageInternal msgI);

   public abstract void sendFullMessage(MessageInternal msgI, boolean sendBlocking, SendAcknowledgementHandler handler, SimpleString defaultAddress) throws HornetQException;

   /**
    * it should return the number of credits (or bytes) used to send this packet
    *
    * @param msgI
    * @return
    * @throws HornetQException
    */
   public abstract int sendInitialChunkOnLargeMessage(MessageInternal msgI) throws HornetQException;


   public abstract int sendLargeMessageChunk(MessageInternal msgI, long messageBodySize, boolean sendBlocking, boolean lastChunk, byte[] chunk, SendAcknowledgementHandler messageHandler) throws HornetQException;


   public abstract void setSendAcknowledgementHandler(final SendAcknowledgementHandler handler);

   public abstract void createSharedQueue(SimpleString address,
                                          SimpleString queueName,
                                          SimpleString filterString,
                                          boolean durable) throws HornetQException;

   public abstract void deleteQueue(SimpleString queueName) throws HornetQException;

   public abstract void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable, boolean temp) throws HornetQException;

   public abstract ClientSession.QueueQuery queueQuery(SimpleString queueName) throws HornetQException;

   public abstract void forceDelivery(ClientConsumer consumer, long sequence) throws HornetQException;

   public abstract ClientSession.AddressQuery addressQuery(final SimpleString address) throws HornetQException;

   public abstract void simpleCommit() throws HornetQException;


   /**
    * If we are doing a simple rollback on the RA, we need to ack the last message sent to the consumer,
    * otherwise DLQ won't work.
    * <p/>
    * this is because we only ACK after on the RA, We may review this if we always acked earlier.
    *
    * @param lastMessageAsDelivered
    * @throws HornetQException
    */
   public abstract void simpleRollback(boolean lastMessageAsDelivered) throws HornetQException;

   public abstract void sessionStart() throws HornetQException;

   public abstract void sessionStop() throws HornetQException;

   public abstract void sendACK(boolean individual, boolean block, final ClientConsumer consumer, final Message message) throws HornetQException;

   public abstract void expireMessage(final ClientConsumer consumer, Message message) throws HornetQException;

   public abstract void sessionClose() throws HornetQException;

   public abstract void addSessionMetadata(String key, String data) throws HornetQException;

   public abstract void addUniqueMetaData(String key, String data) throws HornetQException;

   public abstract void sendProducerCreditsMessage(final int credits, final SimpleString address);

   public abstract void xaCommit(Xid xid, boolean onePhase) throws XAException, HornetQException;

   public abstract void xaEnd(Xid xid, int flags) throws XAException, HornetQException;

   public abstract void xaForget(Xid xid) throws XAException, HornetQException;

   public abstract int xaPrepare(Xid xid) throws XAException, HornetQException;

   public abstract Xid[] xaScan() throws HornetQException;

   public abstract void xaRollback(Xid xid, boolean wasStarted) throws HornetQException, XAException;

   public abstract void xaStart(Xid xid, int flags) throws XAException, HornetQException;

   public abstract boolean configureTransactionTimeout(int seconds) throws HornetQException;

   public abstract ClientConsumerInternal createConsumer(SimpleString queueName, SimpleString filterString, int windowSize, int maxRate, int ackBatchSize, boolean browseOnly,
                                                         Executor executor, Executor flowControlExecutor) throws HornetQException;

   /**
    * Performs a round trip to the server requesting what is the current tx timeout on the session
    *
    * @return
    */
   public abstract int recoverSessionTimeout() throws HornetQException;

   public abstract int getServerVersion();

   public abstract void recreateSession(final String username,
                                        final String password,
                                        final int minLargeMessageSize,
                                        final boolean xa,
                                        final boolean autoCommitSends,
                                        final boolean autoCommitAcks,
                                        final boolean preAcknowledge,
                                        final SimpleString defaultAddress) throws HornetQException;


   public abstract void recreateConsumerOnServer(ClientConsumerInternal consumerInternal) throws HornetQException;

   public abstract void xaFailed(Xid xid) throws HornetQException;

   public abstract void restartSession() throws HornetQException;

   public abstract void resetMetadata(HashMap<String, String> metaDataToSend);


   // Failover utility classes

   /**
    * Interrupt and return any blocked calls
    */
   public abstract void returnBlocking(HornetQException cause);

   /**
    * it will lock the communication channel of the session avoiding anything to come while failover is happening.
    * It happens on preFailover from ClientSessionImpl
    */
   public abstract void lockCommunications();


   public abstract void releaseCommunications();

   public abstract void cleanup();


   public abstract void linkFlowControl(SimpleString address, ClientProducerCreditsImpl clientProducerCredits);
}
