/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.messaging.core.client.impl;

import static org.jboss.messaging.utils.SimpleString.toSimpleString;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.SendAcknowledgementHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.CommandConfirmationHandler;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.RollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionExpiredMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionFailoverCompleteMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.ConcurrentHashSet;
import org.jboss.messaging.utils.IDGenerator;
import org.jboss.messaging.utils.OrderedExecutorFactory;
import org.jboss.messaging.utils.SimpleIDGenerator;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.TokenBucketLimiterImpl;

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

   private static final org.jboss.messaging.utils.ExecutorFactory executorFactory = new OrderedExecutorFactory(Executors.newCachedThreadPool(new org.jboss.messaging.utils.JBMThreadFactory("jbm-client-session-threads")));

   // Attributes ----------------------------------------------------------------------------

   private final ConnectionManager connectionManager;

   private final String name;

   private final boolean xa;

   private final Executor executor;

   private volatile RemotingConnection remotingConnection;

   private final Set<ClientProducerInternal> producers = new ConcurrentHashSet<ClientProducerInternal>();

   private final Map<Long, ClientConsumerInternal> consumers = new ConcurrentHashMap<Long, ClientConsumerInternal>();

   private volatile boolean closed;

   private final boolean autoCommitAcks;

   private final boolean preAcknowledge;

   private final boolean autoCommitSends;

   private final boolean blockOnAcknowledge;

   private final boolean autoGroup;

   private final int ackBatchSize;

   private final int consumerWindowSize;

   private final int consumerMaxRate;

   private final int producerMaxRate;

   private final boolean blockOnNonPersistentSend;

   private final boolean blockOnPersistentSend;

   private final int minLargeMessageSize;

   private final Channel channel;

   private final int version;

   // For testing only
   private boolean forceNotSameRM;

   private final IDGenerator idGenerator = new SimpleIDGenerator(0);

   private volatile boolean started;

   private SendAcknowledgementHandler sendAckHandler;

   private volatile boolean closedSent;

   // Constructors ----------------------------------------------------------------------------

   public ClientSessionImpl(final ConnectionManager connectionManager,
                            final String name,
                            final boolean xa,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean preAcknowledge,
                            final boolean blockOnAcknowledge,
                            final boolean autoGroup,
                            final int ackBatchSize,
                            final int consumerWindowSize,
                            final int consumerMaxRate,
                            final int producerMaxRate,
                            final boolean blockOnNonPersistentSend,
                            final boolean blockOnPersistentSend,
                            final int minLargeMessageSize,
                            final RemotingConnection remotingConnection,
                            final int version,
                            final Channel channel) throws MessagingException
   {
      this.connectionManager = connectionManager;

      this.name = name;

      this.remotingConnection = remotingConnection;

      executor = executorFactory.getExecutor();

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

      this.producerMaxRate = producerMaxRate;

      this.blockOnNonPersistentSend = blockOnNonPersistentSend;

      this.blockOnPersistentSend = blockOnPersistentSend;

      this.minLargeMessageSize = minLargeMessageSize;
   }

   // ClientSession implementation
   // -----------------------------------------------------------------

   public void createQueue(final SimpleString address, final SimpleString queueName, final boolean durable) throws MessagingException
   {
      internalCreateQueue(address, queueName, null, durable, false);
   }

   public void createQueue(final String address, final String queueName, final boolean durable) throws MessagingException
   {
      createQueue(toSimpleString(address), toSimpleString(queueName), durable);
   }

   public void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable) throws MessagingException
   {
      internalCreateQueue(address, queueName, filterString, durable, false);
   }

   public void createQueue(String address, String queueName, String filterString, boolean durable) throws MessagingException
   {
      createQueue(toSimpleString(address), toSimpleString(queueName), toSimpleString(filterString), durable);
   }

   public void createTemporaryQueue(SimpleString address, SimpleString queueName) throws MessagingException
   {
      internalCreateQueue(address, queueName, null, false, true);
   }

   public void createTemporaryQueue(String address, String queueName) throws MessagingException
   {
      internalCreateQueue(toSimpleString(address), toSimpleString(queueName), null, false, true);
   }

   public void createTemporaryQueue(SimpleString address, SimpleString queueName, SimpleString filter) throws MessagingException
   {
      internalCreateQueue(address, queueName, filter, false, true);
   }

   public void createTemporaryQueue(String address, String queueName, String filter) throws MessagingException
   {
      internalCreateQueue(toSimpleString(address), toSimpleString(queueName), toSimpleString(filter), false, true);
   }

   public void deleteQueue(final SimpleString queueName) throws MessagingException
   {
      checkClosed();

      channel.sendBlocking(new SessionDeleteQueueMessage(queueName));
   }

   public void deleteQueue(final String queueName) throws MessagingException
   {
      deleteQueue(toSimpleString(queueName));
   }

   public SessionQueueQueryResponseMessage queueQuery(final SimpleString queueName) throws MessagingException
   {
      checkClosed();

      SessionQueueQueryMessage request = new SessionQueueQueryMessage(queueName);

      SessionQueueQueryResponseMessage response = (SessionQueueQueryResponseMessage)channel.sendBlocking(request);

      return response;
   }

   public SessionBindingQueryResponseMessage bindingQuery(final SimpleString address) throws MessagingException
   {
      checkClosed();

      SessionBindingQueryMessage request = new SessionBindingQueryMessage(address);

      SessionBindingQueryResponseMessage response = (SessionBindingQueryResponseMessage)channel.sendBlocking(request);

      return response;
   }

   public ClientConsumer createConsumer(final SimpleString queueName) throws MessagingException
   {
      return createConsumer(queueName, null, false);
   }

   public ClientConsumer createConsumer(final String queueName) throws MessagingException
   {
      return createConsumer(toSimpleString(queueName));
   }

   public ClientConsumer createConsumer(final SimpleString queueName, final SimpleString filterString) throws MessagingException
   {
      return createConsumer(queueName, filterString, consumerWindowSize, consumerMaxRate, false);
   }

   public ClientConsumer createConsumer(final String queueName, final String filterString) throws MessagingException
   {
      return createConsumer(toSimpleString(queueName), toSimpleString(filterString));
   }

   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final boolean browseOnly) throws MessagingException
   {
      return createConsumer(queueName, filterString, consumerWindowSize, consumerMaxRate, browseOnly);
   }

   public ClientConsumer createConsumer(final String queueName, final String filterString, final boolean browseOnly) throws MessagingException
   {
      return createConsumer(toSimpleString(queueName), toSimpleString(filterString), browseOnly);
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
                                        final boolean browseOnly) throws MessagingException
   {
      return internalCreateConsumer(queueName, filterString, windowSize, maxRate, browseOnly, null);
   }

   public ClientConsumer createConsumer(final String queueName,
                                        final String filterString,
                                        final int windowSize,
                                        final int maxRate,
                                        final boolean browseOnly) throws MessagingException
   {
      return createConsumer(toSimpleString(queueName), toSimpleString(filterString), windowSize, maxRate, browseOnly);
   }

   public ClientProducer createProducer() throws MessagingException
   {
      return createProducer((SimpleString)null);
   }

   public ClientProducer createProducer(final SimpleString address) throws MessagingException
   {
      return createProducer(address, producerMaxRate);
   }

   public ClientProducer createProducer(final String address) throws MessagingException
   {
      return createProducer(toSimpleString(address));
   }

   public ClientProducer createProducer(final SimpleString address, final int maxRate) throws MessagingException
   {
      return createProducer(address, maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
   }

   public ClientProducer createProducer(final String address, final int rate) throws MessagingException
   {
      return createProducer(toSimpleString(address), rate);
   }

   public ClientProducer createProducer(final SimpleString address,
                                        final int maxRate,
                                        final boolean blockOnNonPersistentSend,
                                        final boolean blockOnPersistentSend) throws MessagingException
   {
      return internalCreateProducer(address, maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
   }

   public ClientProducer createProducer(final String address,
                                        final int maxRate,
                                        final boolean blockOnNonPersistentSend,
                                        final boolean blockOnPersistentSend) throws MessagingException
   {
      return createProducer(toSimpleString(address), maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
   }

   public XAResource getXAResource()
   {
      return this;
   }

   public void commit() throws MessagingException
   {
      checkClosed();

      flushAcks();

      channel.sendBlocking(new PacketImpl(PacketImpl.SESS_COMMIT));
   }

   public void rollback() throws MessagingException
   {
      rollback(false);
   }

   public void rollback(final boolean isLastMessageAsDelived) throws MessagingException
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

      channel.sendBlocking(new RollbackMessage(isLastMessageAsDelived));

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
      MessagingBuffer body = createBuffer(INITIAL_MESSAGE_BODY_SIZE);

      return new ClientMessageImpl(type, durable, expiration, timestamp, priority, body);
   }

   public ClientMessage createClientMessage(final byte type, final boolean durable)
   {
      MessagingBuffer body = remotingConnection.createBuffer(INITIAL_MESSAGE_BODY_SIZE);

      return new ClientMessageImpl(type, durable, body);
   }

   public ClientMessage createClientMessage(final boolean durable)
   {
      MessagingBuffer body = createBuffer(INITIAL_MESSAGE_BODY_SIZE);

      return new ClientMessageImpl(durable, body);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.client.impl.ClientSessionInternal#createBuffer(int)
    */
   public MessagingBuffer createBuffer(final int size)
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

   public void start() throws MessagingException
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

   public void stop() throws MessagingException
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
      remotingConnection.addFailureListener(listener);
   }

   public boolean removeFailureListener(final FailureListener listener)
   {
      return remotingConnection.removeFailureListener(listener);
   }

   public int getVersion()
   {
      return version;
   }

   // ClientSessionInternal implementation
   // ------------------------------------------------------------

   public String getName()
   {
      return name;
   }

   // This acknowledges all messages received by the consumer so far
   public void acknowledge(final long consumerID, final long messageID) throws MessagingException
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

   public void expire(final long consumerID, final long messageID) throws MessagingException
   {
      checkClosed();

      SessionExpiredMessage message = new SessionExpiredMessage(consumerID, messageID);

      channel.send(message);
   }

   public void addConsumer(final ClientConsumerInternal consumer)
   {
      consumers.put(consumer.getID(), consumer);
   }

   public void addProducer(final ClientProducerInternal producer)
   {
      producers.add(producer);
   }

   public void removeConsumer(final ClientConsumerInternal consumer) throws MessagingException
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

         clMessage.setFlowControlSize(clMessage.getEncodeSize());

         consumer.handleMessage(message.getClientMessage());
      }
   }

   public void handleReceiveLargeMessage(final long consumerID, final SessionReceiveMessage message) throws Exception
   {
      ClientConsumerInternal consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         consumer.handleLargeMessage(message);
      }
   }

   public void handleReceiveContinuation(final long consumerID, final SessionReceiveContinuationMessage continuation) throws Exception
   {
      ClientConsumerInternal consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         consumer.handleLargeMessageContinuation(continuation);
      }
   }

   public void close() throws MessagingException
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
   public synchronized boolean handleFailover(final RemotingConnection backupConnection)
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
         channel.transferConnection(backupConnection, channel.getID(), null);

         backupConnection.syncIDGeneratorSequence(remotingConnection.getIDGeneratorSequence());

         remotingConnection = backupConnection;

         Packet request = new ReattachSessionMessage(name, channel.getLastReceivedCommandID());

         Channel channel1 = backupConnection.getChannel(1, -1, false);

         ReattachSessionResponseMessage response = (ReattachSessionResponseMessage)channel1.sendBlocking(request);

         if (!response.isRemoved())
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

      channel.send(new SessionFailoverCompleteMessage(name));

      return ok;
   }

   public void returnBlocking()
   {
      channel.returnBlocking();
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

      // Note - don't need to flush acks since the previous end would have
      // done this

      SessionXACommitMessage packet = new SessionXACommitMessage(xid, onePhase);

      try
      {
         SessionXAResponseMessage response = (SessionXAResponseMessage)channel.sendBlocking(packet);

         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
      }
      catch (MessagingException e)
      {
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public void end(final Xid xid, final int flags) throws XAException
   {
      checkXA();
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
      catch (MessagingException e)
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
      catch (MessagingException e)
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
      catch (MessagingException e)
      {
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   public boolean isSameRM(final XAResource xares) throws XAException
   {
      checkXA();

      if (!(xares instanceof ClientSessionImpl))
      {
         return false;
      }

      if (forceNotSameRM)
      {
         return false;
      }

      ClientSessionImpl other = (ClientSessionImpl)xares;

      return connectionManager == other.connectionManager;
   }

   public int prepare(final Xid xid) throws XAException
   {
      checkXA();

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
      catch (MessagingException e)
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
         catch (MessagingException e)
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

         if (response.isError())
         {
            throw new XAException(response.getResponseCode());
         }
      }
      catch (MessagingException e)
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
      catch (MessagingException e)
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
      catch (MessagingException e)
      {
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   // FailureListener implementation --------------------------------------------

   public boolean connectionFailed(final MessagingException me)
   {
      try
      {
         cleanUp();
      }
      catch (Exception e)
      {
         log.error("Failed to cleanup session");
      }

      return true;
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

   /**
    * @param queueName
    * @param filterString
    * @param windowSize
    * @param browseOnly
    * @return
    * @throws MessagingException
    */
   private ClientConsumer internalCreateConsumer(final SimpleString queueName,
                                                 final SimpleString filterString,
                                                 final int windowSize,
                                                 final int maxRate,
                                                 final boolean browseOnly,
                                                 final File directory) throws MessagingException
   {
      checkClosed();

      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(queueName, filterString, browseOnly);

      channel.sendBlocking(request);

      // The actual windows size that gets used is determined by the user since
      // could be overridden on the queue settings
      // The value we send is just a hint

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

      long consumerID = idGenerator.generateID();

      ClientConsumerInternal consumer = new ClientConsumerImpl(this,
                                                               consumerID,
                                                               clientWindowSize,
                                                               ackBatchSize,
                                                               consumerMaxRate > 0 ? new TokenBucketLimiterImpl(maxRate,
                                                                                                                false)
                                                                                  : null,
                                                               executor,
                                                               channel,
                                                               directory);

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
                                                 final boolean blockOnPersistentSend) throws MessagingException
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
                                    final boolean temp) throws MessagingException
   {
      checkClosed();

      if (durable && temp)
      {
         throw new MessagingException(MessagingException.INTERNAL_ERROR, "Queue can not be both durable and temporay");
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

   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Session is closed");
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

   private void closeChildren() throws MessagingException
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

   private void flushAcks() throws MessagingException
   {
      for (ClientConsumerInternal consumer : consumers.values())
      {
         consumer.flushAcks();
      }
   }
}
