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

import static org.hornetq.core.exception.HornetQException.TRANSACTION_ROLLED_BACK;
import static org.hornetq.utils.SimpleString.toSimpleString;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.SendAcknowledgementHandler;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.CommandConfirmationHandler;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.CreateQueueMessage;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.ReattachSessionMessage;
import org.hornetq.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.RollbackMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionCloseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionExpiredMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;
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
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.IDGenerator;
import org.hornetq.utils.SimpleIDGenerator;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TokenBucketLimiterImpl;

/*
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 * @version <tt>$Revision: 3603 $</tt> $Id: ClientSessionImpl.java 3603 2008-01-21 18:49:20Z timfox $
 *
 * $Id: ClientSessionImpl.java 3603 2008-01-21 18:49:20Z timfox $
 *
 */
public class ClientSessionImpl implements ClientSessionInternal, FailureListener, CommandConfirmationHandler
{
   // Constants ----------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientSessionImpl.class);

   private final boolean trace = log.isTraceEnabled();

   public static final int INITIAL_MESSAGE_BODY_SIZE = 1024;

   // Attributes ----------------------------------------------------------------------------

   private final FailoverManager connectionManager;

   private final String name;

   private final String username;

   private final String password;

   private final boolean xa;

   private final Executor executor;

   private volatile RemotingConnection remotingConnection;

   private final Set<ClientProducerInternal> producers = new ConcurrentHashSet<ClientProducerInternal>();

   // Consumers must be an ordered map so if we fail we recreate them in the same order with the same ids
   private final Map<Long, ClientConsumerInternal> consumers = new LinkedHashMap<Long, ClientConsumerInternal>();

   private volatile boolean closed;

   private final boolean autoCommitAcks;

   private final boolean preAcknowledge;

   private final boolean autoCommitSends;

   private final boolean blockOnAcknowledge;

   private final boolean autoGroup;

   private final int ackBatchSize;

   private final int consumerWindowSize;

   private final int consumerMaxRate;

   private final int producerWindowSize;

   private final int producerMaxRate;

   private final boolean blockOnNonPersistentSend;

   private final boolean blockOnPersistentSend;

   private final int minLargeMessageSize;

   private final boolean cacheLargeMessageClient;

   private final Channel channel;

   private final int version;

   // For testing only
   private boolean forceNotSameRM;

   private final IDGenerator idGenerator = new SimpleIDGenerator(0);

   private volatile boolean started;

   private SendAcknowledgementHandler sendAckHandler;

   private volatile boolean closedSent;

   private volatile boolean rollbackOnly;

   private volatile boolean workDone;

   // Constructors ----------------------------------------------------------------------------

   public ClientSessionImpl(final FailoverManager connectionManager,
                            final String name,
                            final String username,
                            final String password,
                            final boolean xa,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean preAcknowledge,
                            final boolean blockOnAcknowledge,
                            final boolean autoGroup,
                            final int ackBatchSize,
                            final int consumerWindowSize,
                            final int consumerMaxRate,
                            final int producerWindowSize,
                            final int producerMaxRate,
                            final boolean blockOnNonPersistentSend,
                            final boolean blockOnPersistentSend,
                            final boolean cacheLargeMessageClient,
                            final int minLargeMessageSize,
                            final RemotingConnection remotingConnection,
                            final int version,
                            final Channel channel,
                            final Executor executor) throws HornetQException
   {
      this.connectionManager = connectionManager;

      this.name = name;

      this.username = username;

      this.password = password;

      this.remotingConnection = remotingConnection;

      this.executor = executor;

      this.xa = xa;

      this.autoCommitAcks = autoCommitAcks;

      this.preAcknowledge = preAcknowledge;

      this.autoCommitSends = autoCommitSends;

      this.blockOnAcknowledge = blockOnAcknowledge;

      this.autoGroup = autoGroup;

      this.channel = channel;

      this.version = version;

      this.ackBatchSize = ackBatchSize;

      this.consumerWindowSize = consumerWindowSize;

      this.consumerMaxRate = consumerMaxRate;

      this.producerWindowSize = producerWindowSize;

      this.producerMaxRate = producerMaxRate;

      this.blockOnNonPersistentSend = blockOnNonPersistentSend;

      this.blockOnPersistentSend = blockOnPersistentSend;

      this.cacheLargeMessageClient = cacheLargeMessageClient;

      this.minLargeMessageSize = minLargeMessageSize;
   }

   // ClientSession implementation
   // -----------------------------------------------------------------

   public void createQueue(final SimpleString address, final SimpleString queueName) throws HornetQException
   {
      internalCreateQueue(address, queueName, null, false, false);
   }

   public void createQueue(final SimpleString address, final SimpleString queueName, final boolean durable) throws HornetQException
   {
      internalCreateQueue(address, queueName, null, durable, false);
   }

   public void createQueue(final String address, final String queueName, final boolean durable) throws HornetQException
   {
      createQueue(toSimpleString(address), toSimpleString(queueName), durable);
   }

   public void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable) throws HornetQException
   {
      internalCreateQueue(address, queueName, filterString, durable, false);
   }

   public void createQueue(String address, String queueName, String filterString, boolean durable) throws HornetQException
   {
      createQueue(toSimpleString(address), toSimpleString(queueName), toSimpleString(filterString), durable);
   }

   public void createTemporaryQueue(SimpleString address, SimpleString queueName) throws HornetQException
   {
      internalCreateQueue(address, queueName, null, false, true);
   }

   public void createTemporaryQueue(String address, String queueName) throws HornetQException
   {
      internalCreateQueue(toSimpleString(address), toSimpleString(queueName), null, false, true);
   }

   public void createTemporaryQueue(SimpleString address, SimpleString queueName, SimpleString filter) throws HornetQException
   {
      internalCreateQueue(address, queueName, filter, false, true);
   }

   public void createTemporaryQueue(String address, String queueName, String filter) throws HornetQException
   {
      internalCreateQueue(toSimpleString(address), toSimpleString(queueName), toSimpleString(filter), false, true);
   }

   public void deleteQueue(final SimpleString queueName) throws HornetQException
   {
      checkClosed();

      channel.sendBlocking(new SessionDeleteQueueMessage(queueName));
   }

   public void deleteQueue(final String queueName) throws HornetQException
   {
      deleteQueue(toSimpleString(queueName));
   }

   public SessionQueueQueryResponseMessage queueQuery(final SimpleString queueName) throws HornetQException
   {
      checkClosed();

      SessionQueueQueryMessage request = new SessionQueueQueryMessage(queueName);

      SessionQueueQueryResponseMessage response = (SessionQueueQueryResponseMessage)channel.sendBlocking(request);

      return response;
   }

   public SessionBindingQueryResponseMessage bindingQuery(final SimpleString address) throws HornetQException
   {
      checkClosed();

      SessionBindingQueryMessage request = new SessionBindingQueryMessage(address);

      SessionBindingQueryResponseMessage response = (SessionBindingQueryResponseMessage)channel.sendBlocking(request);

      return response;
   }

   public ClientConsumer createConsumer(final SimpleString queueName) throws HornetQException
   {
      return createConsumer(queueName, null, false);
   }

   public ClientConsumer createConsumer(final String queueName) throws HornetQException
   {
      return createConsumer(toSimpleString(queueName));
   }

   public ClientConsumer createConsumer(final SimpleString queueName, final SimpleString filterString) throws HornetQException
   {
      return createConsumer(queueName, filterString, consumerWindowSize, consumerMaxRate, false);
   }

   public void createQueue(final String address, final String queueName) throws HornetQException
   {
      internalCreateQueue(toSimpleString(address), toSimpleString(queueName), null, false, false);
   }

   public ClientConsumer createConsumer(final String queueName, final String filterString) throws HornetQException
   {
      return createConsumer(toSimpleString(queueName), toSimpleString(filterString));
   }

   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final boolean browseOnly) throws HornetQException
   {
      return createConsumer(queueName, filterString, consumerWindowSize, consumerMaxRate, browseOnly);
   }
   
   public ClientConsumer createConsumer(final SimpleString queueName,                                        
                                        final boolean browseOnly) throws HornetQException
   {
      return createConsumer(queueName, null, consumerWindowSize, consumerMaxRate, browseOnly);
   }

   public ClientConsumer createConsumer(final String queueName, final String filterString, final boolean browseOnly) throws HornetQException
   {
      return createConsumer(toSimpleString(queueName), toSimpleString(filterString), browseOnly);
   }
   
   public ClientConsumer createConsumer(final String queueName, final boolean browseOnly) throws HornetQException
   {
      return createConsumer(toSimpleString(queueName), null, browseOnly);
   }

   /*
    * Note, we DO NOT currently support direct consumers (i.e. consumers we're delivery occurs on the remoting thread.
    * Direct consumers have issues with blocking and failover.
    * E.g. if direct then inside MessageHandler call a blocking method like rollback or acknowledge (blocking)
    * This can block until failove completes, which disallows the thread to be used to deliver any responses to the client
    * during that period, so failover won't occur.
    * If we want direct consumers we need to rethink how they work
   */
   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final int windowSize,
                                        final int maxRate,
                                        final boolean browseOnly) throws HornetQException
   {
      return internalCreateConsumer(queueName, filterString, windowSize, maxRate, browseOnly);
   }

   public ClientConsumer createConsumer(final String queueName,
                                        final String filterString,
                                        final int windowSize,
                                        final int maxRate,
                                        final boolean browseOnly) throws HornetQException
   {
      return createConsumer(toSimpleString(queueName), toSimpleString(filterString), windowSize, maxRate, browseOnly);
   }

   public ClientProducer createProducer() throws HornetQException
   {
      return createProducer((SimpleString)null);
   }

   public ClientProducer createProducer(final SimpleString address) throws HornetQException
   {
      return createProducer(address, producerMaxRate);
   }

   public ClientProducer createProducer(final String address) throws HornetQException
   {
      return createProducer(toSimpleString(address));
   }

   public ClientProducer createProducer(final SimpleString address, final int maxRate) throws HornetQException
   {
      return createProducer(address, maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
   }

   public ClientProducer createProducer(final String address, final int rate) throws HornetQException
   {
      return createProducer(toSimpleString(address), rate);
   }

   public ClientProducer createProducer(final SimpleString address,
                                        final int maxRate,
                                        final boolean blockOnNonPersistentSend,
                                        final boolean blockOnPersistentSend) throws HornetQException
   {
      return internalCreateProducer(address, maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
   }

   public ClientProducer createProducer(final String address,
                                        final int maxRate,
                                        final boolean blockOnNonPersistentSend,
                                        final boolean blockOnPersistentSend) throws HornetQException
   {
      return createProducer(toSimpleString(address), maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
   }

   public XAResource getXAResource()
   {
      return this;
   }

   public void commit() throws HornetQException
   {
      checkClosed();

      if (rollbackOnly)
      {
         throw new HornetQException(TRANSACTION_ROLLED_BACK,
                                    "The transaction was rolled back on failover to a backup server");
      }

      flushAcks();

      channel.sendBlocking(new PacketImpl(PacketImpl.SESS_COMMIT));

      workDone = false;
   }

   public void rollback() throws HornetQException
   {
      rollback(false);
   }

   public void rollback(final boolean isLastMessageAsDelivered) throws HornetQException
   {
      checkClosed();

      // We do a "JMS style" rollback where the session is stopped, and the buffer is cancelled back
      // first before rolling back
      // This ensures messages are received in the same order after rollback w.r.t. to messages in the buffer
      // For core we could just do a straight rollback, it really depends if we want JMS style semantics or not...

      boolean wasStarted = started;

      if (wasStarted)
      {
         stop();
      }

      // We need to make sure we don't get any inflight messages
      for (ClientConsumerInternal consumer : consumers.values())
      {
         consumer.clear();
      }

      // Acks must be flushed here *after connection is stopped and all onmessages finished executing
      flushAcks();

      channel.sendBlocking(new RollbackMessage(isLastMessageAsDelivered));

      if (wasStarted)
      {
         start();
      }
   }

   public ClientMessage createClientMessage(final byte type,
                                            final boolean durable,
                                            final long expiration,
                                            final long timestamp,
                                            final byte priority)
   {
      HornetQBuffer body = createBuffer(INITIAL_MESSAGE_BODY_SIZE);

      return new ClientMessageImpl(type, durable, expiration, timestamp, priority, body);
   }

   public ClientMessage createClientMessage(final byte type, final boolean durable)
   {
      HornetQBuffer body = remotingConnection.createBuffer(INITIAL_MESSAGE_BODY_SIZE);

      return new ClientMessageImpl(type, durable, body);
   }

   public ClientMessage createClientMessage(final boolean durable)
   {
      HornetQBuffer body = createBuffer(INITIAL_MESSAGE_BODY_SIZE);

      return new ClientMessageImpl(durable, body);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.client.impl.ClientSessionInternal#createBuffer(int)
    */
   public HornetQBuffer createBuffer(final int size)
   {
      return ChannelBuffers.dynamicBuffer(size);
   }

   public boolean isClosed()
   {
      return closed;
   }

   public boolean isAutoCommitSends()
   {
      return autoCommitSends;
   }

   public boolean isAutoCommitAcks()
   {
      return autoCommitAcks;
   }

   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public boolean isXA()
   {
      return xa;
   }

   public void start() throws HornetQException
   {
      checkClosed();

      if (!started)
      {
         for (ClientConsumerInternal clientConsumerInternal : consumers.values())
         {            
            clientConsumerInternal.start();
         }

         channel.send(new PacketImpl(PacketImpl.SESS_START));

         started = true;
      }
   }

   public void stop() throws HornetQException
   {
      checkClosed();

      if (started)
      {
         for (ClientConsumerInternal clientConsumerInternal : consumers.values())
         {
            clientConsumerInternal.stop();
         }

         channel.sendBlocking(new PacketImpl(PacketImpl.SESS_STOP));

         started = false;
      }
   }

   public void addFailureListener(final FailureListener listener)
   {
      connectionManager.addFailureListener(listener);
   }

   public boolean removeFailureListener(final FailureListener listener)
   {
      return connectionManager.removeFailureListener(listener);
   }

   public int getVersion()
   {
      return version;
   }

   // ClientSessionInternal implementation
   // ------------------------------------------------------------

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   /**
    * @return the cacheLargeMessageClient
    */
   public boolean isCacheLargeMessageClient()
   {
      return cacheLargeMessageClient;
   }

   public String getName()
   {
      return name;
   }

   // This acknowledges all messages received by the consumer so far
   public void acknowledge(final long consumerID, final long messageID) throws HornetQException
   {
      // if we're pre-acknowledging then we don't need to do anything
      if (preAcknowledge)
      {
         return;
      }

      checkClosed();

      SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(consumerID, messageID, blockOnAcknowledge);

      if (blockOnAcknowledge)
      {
         channel.sendBlocking(message);
      }
      else
      {
         channel.send(message);
      }
   }

   public void expire(final long consumerID, final long messageID) throws HornetQException
   {
      checkClosed();

      // We don't send expiries for pre-ack since message will already have been acked on server
      if (!preAcknowledge)
      {
         SessionExpiredMessage message = new SessionExpiredMessage(consumerID, messageID);

         channel.send(message);
      }
   }

   public void addConsumer(final ClientConsumerInternal consumer)
   {
      consumers.put(consumer.getID(), consumer);
   }

   public void addProducer(final ClientProducerInternal producer)
   {
      producers.add(producer);
   }

   public void removeConsumer(final ClientConsumerInternal consumer) throws HornetQException
   {
      consumers.remove(consumer.getID());      
   }

   public void removeProducer(final ClientProducerInternal producer)
   {
      producers.remove(producer);
   }

   public void handleReceiveMessage(final long consumerID, final SessionReceiveMessage message) throws Exception
   {
      ClientConsumerInternal consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         ClientMessageInternal clMessage = message.getClientMessage();

         if (trace)
         {
            log.trace("Setting up flowControlSize to " + message.getRequiredBufferSize() + " on message = " + clMessage);
         }

         clMessage.setFlowControlSize(message.getRequiredBufferSize());

         workDone();

         consumer.handleMessage(message.getClientMessage());
      }
   }

   public void handleReceiveLargeMessage(final long consumerID, final SessionReceiveMessage message) throws Exception
   {
      ClientConsumerInternal consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         workDone();

         consumer.handleLargeMessage(message);
      }
   }

   public void handleReceiveContinuation(final long consumerID, final SessionReceiveContinuationMessage continuation) throws Exception
   {
      ClientConsumerInternal consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         workDone();

         consumer.handleLargeMessageContinuation(continuation);
      }
   }

   public void close() throws HornetQException
   {
      if (closed)
      {
         return;
      }

      try
      {
         closeChildren();

         closedSent = true;

         channel.sendBlocking(new SessionCloseMessage());
      }
      catch (Throwable ignore)
      {
         // Session close should always return without exception
      }

      doCleanup();
   }

   public synchronized void cleanUp() throws Exception
   {
      if (closed)
      {
         return;
      }

      cleanUpChildren();

      doCleanup();
   }

   public void setSendAcknowledgementHandler(final SendAcknowledgementHandler handler)
   {
      channel.setCommandConfirmationHandler(this);

      sendAckHandler = handler;
   }

    // Needs to be synchronized to prevent issues with occurring concurrently with close()
   
   //TODO - need to reenable
   public synchronized boolean handleReattach(final RemotingConnection backupConnection)
   {
      if (closed)
      {
         return true;
      }
      
      boolean ok = false;

      // We lock the channel to prevent any packets to be added to the resend
      // cache during the failover process
      channel.lock();

      try
      {
         channel.transferConnection(backupConnection);
         
         backupConnection.syncIDGeneratorSequence(remotingConnection.getIDGeneratorSequence());

         remotingConnection = backupConnection;

         Packet request = new ReattachSessionMessage(name, channel.getLastReceivedCommandID());

         Channel channel1 = backupConnection.getChannel(1, -1, false);

         ReattachSessionResponseMessage response = (ReattachSessionResponseMessage)channel1.sendBlocking(request);

         if (response.isSessionFound())
         {                        
            channel.replayCommands(response.getLastReceivedCommandID(), channel.getID());

            ok = true;
         }
         else
         {
            if (closedSent)
            {
               // a session re-attach may fail, if the session close was sent before failover started, hit the server,
               // processed, then before the response was received back, failover occurred, re-attach was attempted. in
               // this case it's ok - we don't want to call any failure listeners and we don't want to halt the rest of
               // the failover process.
               //
               // however if session re-attach fails and the session was not in a call to close, then we DO want to call
               // the session listeners so we return false
               //
               // Also session reattach will fail if the server is restarted - so the session is lost
               ok = true;
            }
            else
            {
               log.warn(System.identityHashCode(this) + " Session not found on server when attempting to re-attach");
            }

            channel.returnBlocking();
         }

      }
      catch (Throwable t)
      {
         log.error("Failed to handle failover", t);
      }
      finally
      {
         channel.unlock();
      }

      return ok;
   }

   public void workDone()
   {
      workDone = true;
   }

   // Needs to be synchronized to prevent issues with occurring concurrently with close()
   public synchronized boolean handleFailover(final RemotingConnection backupConnection)
   {
      if (closed)
      {
         return true;
      }
      
      boolean ok = false;
            
      // Need to stop all consumers outside the lock
      for (ClientConsumerInternal consumer : consumers.values())
      {
         try
         {
            consumer.stop();
         }
         catch (HornetQException e)
         {
            log.error("Failed to stop consumer", e);
         }

         consumer.clearAtFailover();
      }
      
      // We lock the channel to prevent any packets being sent during the failover process
      channel.lock();
      
      try
      {
         channel.transferConnection(backupConnection);
         
         remotingConnection = backupConnection;

         Packet request = new CreateSessionMessage(name,
                                                   channel.getID(),
                                                   version,
                                                   username,
                                                   password,
                                                   minLargeMessageSize,
                                                   xa,
                                                   autoCommitSends,
                                                   autoCommitAcks,
                                                   preAcknowledge,
                                                   producerWindowSize);

         Channel channel1 = backupConnection.getChannel(1, -1, false);

         CreateSessionResponseMessage response = (CreateSessionResponseMessage)channel1.sendBlocking(request);

         if (response.isCreated())
         {
            // Session was created ok

            // Now we need to recreate the consumers

            for (Map.Entry<Long, ClientConsumerInternal> entry : consumers.entrySet())
            {
               SessionCreateConsumerMessage createConsumerRequest = new SessionCreateConsumerMessage(entry.getKey(),
                                                                                                     entry.getValue().getQueueName(),
                                                                                                     entry.getValue().getFilterString(),
                                                                                                     entry.getValue().isBrowseOnly(),
                                                                                                     false);

               createConsumerRequest.setChannelID(channel.getID());

               Connection conn = channel.getConnection().getTransportConnection();

               HornetQBuffer buffer = conn.createBuffer(createConsumerRequest.getRequiredBufferSize());

               createConsumerRequest.encode(buffer);

               conn.write(buffer, false);
               
               int clientWindowSize = calcWindowSize(entry.getValue().getClientWindowSize());
                              
               if (clientWindowSize != 0)
               {
                  SessionConsumerFlowCreditMessage packet = new SessionConsumerFlowCreditMessage(entry.getKey(), clientWindowSize);
                  
                  packet.setChannelID(channel.getID());
                  
                  buffer = conn.createBuffer(packet.getRequiredBufferSize());

                  packet.encode(buffer);

                  conn.write(buffer, false);                  
               }
            }
            
            if ((!autoCommitAcks || !autoCommitSends) && workDone)
            {
               // Session is transacted - set for rollback only

               // FIXME - there is a race condition here - a commit could sneak in before this is set
               rollbackOnly = true;
            }

            // Now start the session if it was already started
            if (started)
            {
               for (ClientConsumerInternal consumer : consumers.values())
               {
                  consumer.start();
               }
               
               Packet packet = new PacketImpl(PacketImpl.SESS_START);
               
               packet.setChannelID(channel.getID());
               
               Connection conn = channel.getConnection().getTransportConnection();
               
               HornetQBuffer buffer = conn.createBuffer(packet.getRequiredBufferSize());

               packet.encode(buffer);

               conn.write(buffer, false);                              
            }

            ok = true;
         }
         else
         {
            // This means the server we failed onto is not ready to take new sessions - perhaps it hasn't actually
            // failed over
         }

         // We cause any blocking calls to return - since they won't get responses.
         channel.returnBlocking();
      }
      catch (Throwable t)
      {
         log.error("Failed to handle failover", t);
      }
      finally
      {
         channel.unlock();
      }
      
      return ok;
   }

   public void returnBlocking()
   {
      channel.returnBlocking();
   }

   public FailoverManager getConnectionManager()
   {
      return connectionManager;
   }

   // CommandConfirmationHandler implementation ------------------------------------

   public void commandConfirmed(final Packet packet)
   {
      if (packet.getType() == PacketImpl.SESS_SEND)
      {
         SessionSendMessage ssm = (SessionSendMessage)packet;

         sendAckHandler.sendAcknowledged(ssm.getClientMessage());
      }
   }

   // XAResource implementation
   // --------------------------------------------------------------------

   public void commit(final Xid xid, final boolean onePhase) throws XAException
   {
      checkXA();
      
      if (rollbackOnly)
      {
         throw new XAException(XAException.XA_RBOTHER);
      }

      // Note - don't need to flush acks since the previous end would have
      // done this

      SessionXACommitMessage packet = new SessionXACommitMessage(xid, onePhase);

      try
      {
         SessionXAResponseMessage response = (SessionXAResponseMessage)channel.sendBlocking(packet);

         workDone = false;
                  
         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
      }
      catch (HornetQException e)
      {
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public void end(final Xid xid, final int flags) throws XAException
   {
      checkXA();
      
      if (rollbackOnly)
      {
         throw new XAException(XAException.XA_RBOTHER);
      }
      
      try
      {
         Packet packet;

         if (flags == XAResource.TMSUSPEND)
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
         }
         else if (flags == XAResource.TMSUCCESS)
         {
            packet = new SessionXAEndMessage(xid, false);
         }
         else if (flags == XAResource.TMFAIL)
         {
            packet = new SessionXAEndMessage(xid, true);
         }
         else
         {
            throw new XAException(XAException.XAER_INVAL);
         }

         flushAcks();

         SessionXAResponseMessage response = (SessionXAResponseMessage)channel.sendBlocking(packet);

         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
      }
      catch (HornetQException e)
      {
         log.error("Caught jmsexecptione ", e);
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public void forget(final Xid xid) throws XAException
   {
      checkXA();
      try
      {
         SessionXAResponseMessage response = (SessionXAResponseMessage)channel.sendBlocking(new SessionXAForgetMessage(xid));

         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
      }
      catch (HornetQException e)
      {
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public int getTransactionTimeout() throws XAException
   {
      checkXA();

      try
      {
         SessionXAGetTimeoutResponseMessage response = (SessionXAGetTimeoutResponseMessage)channel.sendBlocking(new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT));

         return response.getTimeoutSeconds();
      }
      catch (HornetQException e)
      {
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public boolean isSameRM(final XAResource xares) throws XAException
   {
      checkXA();

      if (!(xares instanceof ClientSessionInternal))
      {
         return false;
      }

      if (forceNotSameRM)
      {
         return false;
      }

      ClientSessionInternal other = (ClientSessionInternal)xares;

      return connectionManager == other.getConnectionManager();
   }

   public int prepare(final Xid xid) throws XAException
   {
      checkXA();

      if (rollbackOnly)
      {
         throw new XAException(XAException.XA_RBOTHER);
      }

      // Note - don't need to flush acks since the previous end would have
      // done this

      SessionXAPrepareMessage packet = new SessionXAPrepareMessage(xid);

      try
      {
         SessionXAResponseMessage response = (SessionXAResponseMessage)channel.sendBlocking(packet);

         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
         else
         {
            return response.getResponseCode();
         }
      }
      catch (HornetQException e)
      {
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public Xid[] recover(final int flags) throws XAException
   {
      checkXA();

      if ((flags & XAResource.TMSTARTRSCAN) == XAResource.TMSTARTRSCAN)
      {
         try
         {
            SessionXAGetInDoubtXidsResponseMessage response = (SessionXAGetInDoubtXidsResponseMessage)channel.sendBlocking(new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS));

            List<Xid> xids = response.getXids();

            Xid[] xidArray = xids.toArray(new Xid[xids.size()]);

            return xidArray;
         }
         catch (HornetQException e)
         {
            // This should never occur
            throw new XAException(XAException.XAER_RMERR);
         }
      }
      else
      {
         return new Xid[0];
      }
   }

   public void rollback(final Xid xid) throws XAException
   {
      checkXA();

      try
      {
         boolean wasStarted = started;

         if (wasStarted)
         {
            stop();
         }

         // We need to make sure we don't get any inflight messages
         for (ClientConsumerInternal consumer : consumers.values())
         {
            consumer.clear();
         }

         flushAcks();

         SessionXARollbackMessage packet = new SessionXARollbackMessage(xid);

         SessionXAResponseMessage response = (SessionXAResponseMessage)channel.sendBlocking(packet);

         if (wasStarted)
         {
            start();
         }

         workDone = false;

         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
      }
      catch (HornetQException e)
      {
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public boolean setTransactionTimeout(final int seconds) throws XAException
   {
      checkXA();

      try
      {
         SessionXASetTimeoutResponseMessage response = (SessionXASetTimeoutResponseMessage)channel.sendBlocking(new SessionXASetTimeoutMessage(seconds));

         return response.isOK();
      }
      catch (HornetQException e)
      {
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public void start(final Xid xid, final int flags) throws XAException
   {
      checkXA();
      try
      {
         Packet packet;

         if (flags == XAResource.TMJOIN)
         {
            packet = new SessionXAJoinMessage(xid);
         }
         else if (flags == XAResource.TMRESUME)
         {
            packet = new SessionXAResumeMessage(xid);
         }
         else if (flags == XAResource.TMNOFLAGS)
         {
            // Don't need to flush since the previous end will have done this
            packet = new SessionXAStartMessage(xid);
         }
         else
         {
            throw new XAException(XAException.XAER_INVAL);
         }

         SessionXAResponseMessage response = (SessionXAResponseMessage)channel.sendBlocking(packet);

         if (response.isError())
         {
            log.error("XA operation failed " + response.getMessage() + " code:" + response.getResponseCode());
            throw new XAException(response.getResponseCode());
         }
      }
      catch (HornetQException e)
      {
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   // FailureListener implementation --------------------------------------------

   public void connectionFailed(final HornetQException me)
   {
      try
      {
         cleanUp();
      }
      catch (Exception e)
      {
         log.error("Failed to cleanup session");
      }
   }

   // Public
   // ----------------------------------------------------------------------------

   public void setForceNotSameRM(final boolean force)
   {
      forceNotSameRM = force;
   }

   public RemotingConnection getConnection()
   {
      return remotingConnection;
   }

   // Protected
   // ----------------------------------------------------------------------------

   // Package Private
   // ----------------------------------------------------------------------------

   // Private
   // ----------------------------------------------------------------------------

   private int calcWindowSize(final int windowSize)
   {
      int clientWindowSize;
      if (windowSize == -1)
      {
         // No flow control - buffer can increase without bound! Only use with
         // caution for very fast consumers
         clientWindowSize = -1;
      }
      else if (windowSize == 0)
      {
         // Slow consumer - no buffering
         clientWindowSize = 0;
      }
      else if (windowSize == 1)
      {
         // Slow consumer = buffer 1
         clientWindowSize = 1;
      }
      else if (windowSize > 1)
      {
         // Client window size is half server window size
         clientWindowSize = windowSize >> 1;
      }
      else
      {
         throw new IllegalArgumentException("Invalid window size " + windowSize);
      }
      
      return clientWindowSize;
   }
   
   /**
    * @param queueName
    * @param filterString
    * @param windowSize
    * @param browseOnly
    * @return
    * @throws HornetQException
    */
   private ClientConsumer internalCreateConsumer(final SimpleString queueName,
                                                 final SimpleString filterString,
                                                 final int windowSize,
                                                 final int maxRate,
                                                 final boolean browseOnly) throws HornetQException
   {
      checkClosed();
      
      long consumerID = idGenerator.generateID();

      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(consumerID, queueName, filterString, browseOnly, true);

      channel.sendBlocking(request);

      // The actual windows size that gets used is determined by the user since
      // could be overridden on the queue settings
      // The value we send is just a hint

      int clientWindowSize = calcWindowSize(windowSize);
      
      ClientConsumerInternal consumer = new ClientConsumerImpl(this,
                                                               consumerID,
                                                               queueName,
                                                               filterString,
                                                               browseOnly,
                                                               clientWindowSize,
                                                               ackBatchSize,
                                                               consumerMaxRate > 0 ? new TokenBucketLimiterImpl(maxRate,
                                                                                                                false)
                                                                                  : null,
                                                               executor,
                                                               channel);

      addConsumer(consumer);

      // Now we send window size credits to start the consumption
      // We even send it if windowSize == -1, since we need to start the
      // consumer

      if (windowSize != 0)
      {
         channel.send(new SessionConsumerFlowCreditMessage(consumerID, windowSize));
      }

      return consumer;
   }

   private ClientProducer internalCreateProducer(final SimpleString address,
                                                 final int maxRate,
                                                 final boolean blockOnNonPersistentSend,
                                                 final boolean blockOnPersistentSend) throws HornetQException
   {
      checkClosed();

      ClientProducerInternal producer = new ClientProducerImpl(this,
                                                               address,
                                                               maxRate == -1 ? null
                                                                            : new TokenBucketLimiterImpl(maxRate, false),
                                                               autoCommitSends && blockOnNonPersistentSend,
                                                               autoCommitSends && blockOnPersistentSend,
                                                               autoGroup,
                                                               minLargeMessageSize,
                                                               channel);

      addProducer(producer);

      return producer;
   }

   private void internalCreateQueue(final SimpleString address,
                                    final SimpleString queueName,
                                    final SimpleString filterString,
                                    final boolean durable,
                                    final boolean temp) throws HornetQException
   {
      checkClosed();

      if (durable && temp)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "Queue can not be both durable and temporay");
      }

      CreateQueueMessage request = new CreateQueueMessage(address, queueName, filterString, durable, temp);

      channel.sendBlocking(request);
   }

   private void checkXA() throws XAException
   {
      if (!xa)
      {
         log.error("Session is not XA");
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   private void checkClosed() throws HornetQException
   {
      if (closed)
      {
         throw new HornetQException(HornetQException.OBJECT_CLOSED, "Session is closed");
      }
   }

   private void doCleanup()
   {
      remotingConnection.removeFailureListener(this);

      synchronized (this)
      {
         closed = true;

         channel.close();
      }

      connectionManager.removeSession(this);
   }

   private void cleanUpChildren() throws Exception
   {
      Set<ClientConsumerInternal> consumersClone = new HashSet<ClientConsumerInternal>(consumers.values());

      for (ClientConsumerInternal consumer : consumersClone)
      {
         consumer.cleanUp();
      }

      Set<ClientProducerInternal> producersClone = new HashSet<ClientProducerInternal>(producers);

      for (ClientProducerInternal producer : producersClone)
      {
         producer.cleanUp();
      }
   }

   private void closeChildren() throws HornetQException
   {
      Set<ClientConsumer> consumersClone = new HashSet<ClientConsumer>(consumers.values());

      for (ClientConsumer consumer : consumersClone)
      {
         consumer.close();
      }

      Set<ClientProducer> producersClone = new HashSet<ClientProducer>(producers);

      for (ClientProducer producer : producersClone)
      {
         producer.close();
      }
   }

   private void flushAcks() throws HornetQException
   {
      for (ClientConsumerInternal consumer : consumers.values())
      {
         consumer.flushAcks();
      }
   }
}
