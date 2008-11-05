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

import java.util.HashMap;
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

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionFailoverCompleteMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendManagementMessage;
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
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.IDGenerator;
import org.jboss.messaging.util.JBMThreadFactory;
import org.jboss.messaging.util.OrderedExecutorFactory;
import org.jboss.messaging.util.SimpleIDGenerator;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TokenBucketLimiterImpl;
import org.jboss.messaging.util.TypedProperties;

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
public class ClientSessionImpl implements ClientSessionInternal, FailureListener
{
   // Constants ----------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientSessionImpl.class);

   private final boolean trace = log.isTraceEnabled();

   public static final int INITIAL_MESSAGE_BODY_SIZE = 1024;

   private static final ExecutorFactory executorFactory = new OrderedExecutorFactory(Executors.newCachedThreadPool(new JBMThreadFactory("jbm-client-session-threads")));

   // Attributes ----------------------------------------------------------------------------

   private final ClientSessionFactoryInternal sessionFactory;

   private final String name;

   private final boolean xa;

   private final boolean cacheProducers;

   private final Executor executor;

   private volatile RemotingConnection remotingConnection;

   private volatile RemotingConnection backupConnection;

   private final Map<Long, ClientProducerInternal> producers = new ConcurrentHashMap<Long, ClientProducerInternal>();

   private final Map<Long, ClientConsumerInternal> consumers = new ConcurrentHashMap<Long, ClientConsumerInternal>();

   private final Map<SimpleString, ClientProducerInternal> producerCache;

   private final ClientSessionFactory connectionFactory;

   private volatile boolean closed;

   private final boolean autoCommitAcks;

   private final boolean autoCommitSends;

   private final boolean blockOnAcknowledge;

   private final boolean autoGroupId;

   private final Channel channel;

   private final int version;

   // For testing only
   private boolean forceNotSameRM;

   private final IDGenerator idGenerator = new SimpleIDGenerator(0);

   private volatile boolean started;

   // Constructors ----------------------------------------------------------------------------

   public ClientSessionImpl(final ClientSessionFactoryInternal sessionFactory,
                            final String name,
                            final boolean xa,
                            final boolean cacheProducers,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean blockOnAcknowledge,
                            final boolean autoGroupId,
                            final RemotingConnection remotingConnection,
                            final RemotingConnection backupConnection,
                            final ClientSessionFactory connectionFactory,
                            final int version,
                            final Channel channel) throws MessagingException
   {
      this.sessionFactory = sessionFactory;

      this.name = name;

      this.remotingConnection = remotingConnection;

      this.backupConnection = backupConnection;

      this.connectionFactory = connectionFactory;

      this.cacheProducers = cacheProducers;

      executor = executorFactory.getExecutor();

      this.xa = xa;

      if (cacheProducers)
      {
         producerCache = new HashMap<SimpleString, ClientProducerInternal>();
      }
      else
      {
         producerCache = null;
      }

      this.autoCommitAcks = autoCommitAcks;

      this.autoCommitSends = autoCommitSends;

      this.blockOnAcknowledge = blockOnAcknowledge;

      this.autoGroupId = autoGroupId;

      this.channel = channel;

      this.version = version;
   }

   // ClientSession implementation
   // -----------------------------------------------------------------

   public void createQueue(final SimpleString address,
                           final SimpleString queueName,
                           final SimpleString filterString,
                           final boolean durable,
                           final boolean temp) throws MessagingException
   {
      checkClosed();

      SessionCreateQueueMessage request = new SessionCreateQueueMessage(address, queueName, filterString, durable, temp);

      channel.sendBlocking(request);
   }

   public void deleteQueue(final SimpleString queueName) throws MessagingException
   {
      checkClosed();

      channel.sendBlocking(new SessionDeleteQueueMessage(queueName));
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

   public void addDestination(final SimpleString address, final boolean durable, final boolean temp) throws MessagingException
   {
      checkClosed();

      SessionAddDestinationMessage request = new SessionAddDestinationMessage(address, durable, temp);

      channel.sendBlocking(request);
   }

   public void removeDestination(final SimpleString address, final boolean durable) throws MessagingException
   {
      checkClosed();

      SessionRemoveDestinationMessage request = new SessionRemoveDestinationMessage(address, durable);

      channel.sendBlocking(request);
   }

   public ClientConsumer createConsumer(final SimpleString queueName) throws MessagingException
   {
      checkClosed();

      return createConsumer(queueName, null, false);
   }

   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final boolean direct) throws MessagingException
   {
      checkClosed();

      return createConsumer(queueName,
                            filterString,
                            direct,
                            connectionFactory.getConsumerWindowSize(),
                            connectionFactory.getConsumerMaxRate(),
                            false);
   }

   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final boolean direct,
                                        final boolean browseOnly) throws MessagingException
   {
      return createConsumer(queueName,
                            filterString,
                            direct,
                            connectionFactory.getConsumerWindowSize(),
                            connectionFactory.getConsumerMaxRate(),
                            browseOnly);
   }

   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final boolean direct,
                                        final int windowSize,
                                        final int maxRate,
                                        final boolean browseOnly) throws MessagingException
   {
      checkClosed();
      
      if (direct && sessionFactory.getSendWindowSize() != -1)
      {
         //Direct consumers and send window blocking is incompatible.
         //If execute onMessage on same thread as remoting thread then if onMessage calls rollback() or other method
         //but has no credits it will block on the semaphore until credits arrive, but they will never arrive since the
         //remoting thread won't unwind.
         throw new IllegalArgumentException("Cannot create a direct consumer if send window is specified - since can lead to deadlock");
      }

      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(queueName,
                                                                              filterString,
                                                                              windowSize,
                                                                              maxRate,
                                                                              browseOnly);

      SessionCreateConsumerResponseMessage response = (SessionCreateConsumerResponseMessage)channel.sendBlocking(request);

      // The actual windows size that gets used is determined by the user since
      // could be overridden on the queue settings
      // The value we send is just a hint
      int actualWindowSize = response.getWindowSize();

      int clientWindowSize;
      if (actualWindowSize == -1)
      {
         // No flow control - buffer can increase without bound! Only use with
         // caution for very fast consumers
         clientWindowSize = 0;
      }
      else if (actualWindowSize == 1)
      {
         // Slow consumer - no buffering
         clientWindowSize = 1;
      }
      else if (actualWindowSize > 1)
      {
         // Client window size is half server window size
         clientWindowSize = actualWindowSize >> 1;
      }
      else
      {
         throw new IllegalArgumentException("Invalid window size " + actualWindowSize);
      }

      long consumerID = idGenerator.generateID();

      ClientConsumerInternal consumer = new ClientConsumerImpl(this,
                                                               consumerID,
                                                               clientWindowSize,
                                                               direct,
                                                               executor,
                                                               channel);

      addConsumer(consumer);

      // Now we send window size credits to start the consumption
      // We even send it if windowSize == -1, since we need to start the
      // consumer

      channel.send(new SessionConsumerFlowCreditMessage(consumerID, response.getWindowSize()));

      return consumer;
   }

   public ClientProducer createProducer(final SimpleString address) throws MessagingException
   {
      checkClosed();

      return createProducer(address, connectionFactory.getProducerMaxRate());
   }

   public ClientProducer createProducer(final SimpleString address, final int maxRate) throws MessagingException
   {
      return createProducer(address,
                            maxRate,
                            connectionFactory.isBlockOnNonPersistentSend(),
                            connectionFactory.isBlockOnPersistentSend());
   }

   public ClientProducer createProducer(final SimpleString address,
                                        final int maxRate,
                                        final boolean blockOnNonPersistentSend,
                                        final boolean blockOnPersistentSend) throws MessagingException
   {
      checkClosed();

      ClientProducerInternal producer = null;

      if (cacheProducers)
      {
         producer = producerCache.remove(address);
      }

      if (producer == null)
      {
         SessionCreateProducerMessage request = new SessionCreateProducerMessage(address, maxRate, autoGroupId);

         SessionCreateProducerResponseMessage response = (SessionCreateProducerResponseMessage)channel.sendBlocking(request);

         // maxRate and windowSize can be overridden by the server

         // If the producer is not auto-commit sends then messages are never
         // sent blocking - there is no point
         // since commit, prepare or rollback will flush any messages sent.

         producer = new ClientProducerImpl(this,
                                           idGenerator.generateID(),
                                           address,
                                           response.getMaxRate() == -1 ? null
                                                                      : new TokenBucketLimiterImpl(response.getMaxRate(),
                                                                                                   false),
                                           autoCommitSends && blockOnNonPersistentSend,
                                           autoCommitSends && blockOnPersistentSend,
                                           response.getAutoGroupId(),
                                           channel);
      }

      addProducer(producer);

      return producer;
   }

   public XAResource getXAResource()
   {
      return this;
   }

   public void commit() throws MessagingException
   {
      checkClosed();

      channel.sendBlocking(new PacketImpl(PacketImpl.SESS_COMMIT));
   }

   public void rollback() throws MessagingException
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

      channel.sendBlocking(new PacketImpl(PacketImpl.SESS_ROLLBACK));

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
      MessagingBuffer body = remotingConnection.createBuffer(INITIAL_MESSAGE_BODY_SIZE);

      return new ClientMessageImpl(type, durable, expiration, timestamp, priority, body);
   }

   public ClientMessage createClientMessage(final byte type, final boolean durable)
   {
      MessagingBuffer body = remotingConnection.createBuffer(INITIAL_MESSAGE_BODY_SIZE);

      return new ClientMessageImpl(type, durable, body);
   }

   public ClientMessage createClientMessage(final boolean durable)
   {
      MessagingBuffer body = remotingConnection.createBuffer(INITIAL_MESSAGE_BODY_SIZE);

      return new ClientMessageImpl(durable, body);
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

   public boolean isCacheProducers()
   {
      return cacheProducers;
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
         channel.send(new PacketImpl(PacketImpl.SESS_START));

         started = true;
      }
   }

   public void stop() throws MessagingException
   {
      checkClosed();

      if (started)
      {
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

   public void addConsumer(final ClientConsumerInternal consumer)
   {
      consumers.put(consumer.getID(), consumer);
   }

   public void addProducer(final ClientProducerInternal producer)
   {
      producers.put(producer.getID(), producer);
   }

   public void removeConsumer(final ClientConsumerInternal consumer) throws MessagingException
   {
      consumers.remove(consumer.getID());
   }

   public void removeProducer(final ClientProducerInternal producer)
   {
      producers.remove(producer.getID());

      if (cacheProducers && !producerCache.containsKey(producer.getAddress()))
      {
         producerCache.put(producer.getAddress(), producer);
      }
   }

   public Set<ClientProducerInternal> getProducers()
   {
      return new HashSet<ClientProducerInternal>(producers.values());
   }

   public Set<ClientConsumerInternal> getConsumers()
   {
      return new HashSet<ClientConsumerInternal>(consumers.values());
   }

   public Map<SimpleString, ClientProducerInternal> getProducerCache()
   {
      return new HashMap<SimpleString, ClientProducerInternal>(producerCache);
   }

   public void handleReceiveMessage(final long consumerID, final ClientMessage message) throws Exception
   {
      ClientConsumerInternal consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         consumer.handleMessage(message);
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

   // Needs to be synchronized to prevent issues with occurring concurrently with close()
   public synchronized void handleFailover()
   {
      if (closed)
      {
         return;
      }

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

         if (!response.isRemoved())
         {
            channel.replayCommands(response.getLastReceivedCommandID());
         }
         else
         {
            // There may be a close session call blocking - the response will never come because the session has been
            // closed on the server so we need to interrupt it
            channel.returnBlocking();
         }

         backupConnection = null;
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

      // Now we can add a failure listener since if a further failure occurs we cleanup since no backup any more
      remotingConnection.addFailureListener(this);
   }
   
   public void sendManagementMessage(final ClientMessage message)
   {
      Packet packet = new SessionSendManagementMessage(message);
      
      //Fill in the dest - it's not actually used
      message.setDestination(new SimpleString("JBM"));
      
      channel.send(packet);
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

      return remotingConnection == other.remotingConnection;
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

      // We need to make sure we don't get any inflight messages
      for (ClientConsumerInternal consumer : consumers.values())
      {
         consumer.clear();
      }

      SessionXARollbackMessage packet = new SessionXARollbackMessage(xid);

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

   public void connectionFailed(final MessagingException me)
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

   public RemotingConnection getBackupConnection()
   {
      return backupConnection;
   }

   // Protected
   // ----------------------------------------------------------------------------

   // Package Private
   // ----------------------------------------------------------------------------

   // Private
   // ----------------------------------------------------------------------------

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
      if (cacheProducers)
      {
         producerCache.clear();
      }

      remotingConnection.removeFailureListener(this);

      synchronized (this)
      {
         closed = true;

         channel.close();
      }

      sessionFactory.removeSession(this);
   }

   private void cleanUpChildren() throws Exception
   {
      Set<ClientConsumerInternal> consumersClone = new HashSet<ClientConsumerInternal>(consumers.values());

      for (ClientConsumerInternal consumer : consumersClone)
      {
         consumer.cleanUp();
      }

      Set<ClientProducerInternal> producersClone = new HashSet<ClientProducerInternal>(producers.values());

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

      Set<ClientProducer> producersClone = new HashSet<ClientProducer>(producers.values());

      for (ClientProducer producer : producersClone)
      {
         producer.close();
      }
   }
}
