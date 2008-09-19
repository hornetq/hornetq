/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package org.jboss.messaging.core.client.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CLOSE;

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

import org.jboss.messaging.core.client.ClientBrowser;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ConnectionRegistry;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.ConnectionRegistryImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
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
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TokenBucketLimiterImpl;

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
 * @version <tt>$Revision: 3603 $</tt>
 * 
 * $Id: ClientSessionImpl.java 3603 2008-01-21 18:49:20Z timfox $
 */
public class ClientSessionImpl implements ClientSessionInternal
{
   // Constants
   //----------------------------------------------------------------------------
   // --------

   private static final Logger log = Logger.getLogger(ClientSessionImpl.class);

   private final boolean trace = log.isTraceEnabled();

   public static final int INITIAL_MESSAGE_BODY_SIZE = 1024;

   private static final ExecutorFactory executorFactory = new OrderedExecutorFactory(Executors.newCachedThreadPool(new JBMThreadFactory("jbm-client-session-threads")));

   // Attributes
   //----------------------------------------------------------------------------
   // -------

   private final ClientSessionFactoryInternal sessionFactory;

   private final String name;

   private final boolean xa;

   private final int lazyAckBatchSize;

   private final boolean cacheProducers;

   private final Executor executor;

   private volatile RemotingConnection remotingConnection;

   private final Map<Long, ClientBrowser> browsers = new ConcurrentHashMap<Long, ClientBrowser>();

   private final Map<Long, ClientProducerInternal> producers = new ConcurrentHashMap<Long, ClientProducerInternal>();

   private final Map<Long, ClientConsumerInternal> consumers = new ConcurrentHashMap<Long, ClientConsumerInternal>();

   private final Map<SimpleString, ClientProducerInternal> producerCache;

   private final ClientSessionFactory connectionFactory;

   private volatile boolean closed;

   private boolean acked = true;

   private boolean broken;

   private long toAckCount;

   private long lastID = -1;

   private long deliverID;

   private boolean deliveryExpired;

   private long lastCommittedID = -1;

   private final boolean autoCommitAcks;

   private final boolean autoCommitSends;

   private final boolean blockOnAcknowledge;

   private final Channel channel;

   private final int version;

   private ConnectionRegistry connectionRegistry;

   // For testing only
   private boolean forceNotSameRM;

   private final IDGenerator idGenerator = new IDGenerator(0);

   // Constructors
   //----------------------------------------------------------------------------
   // -----

   public ClientSessionImpl(final ClientSessionFactoryInternal sessionFactory,
                            final String name,
                            final boolean xa,
                            final int lazyAckBatchSize,
                            final boolean cacheProducers,
                            final boolean autoCommitSends,
                            final boolean autoCommitAcks,
                            final boolean blockOnAcknowledge,
                            final RemotingConnection remotingConnection,
                            final ClientSessionFactory connectionFactory,
                            final int version,
                            final Channel channel) throws MessagingException
   {
      if (lazyAckBatchSize < -1 || lazyAckBatchSize == 0)
      {
         throw new IllegalArgumentException("Invalid lazyAckbatchSize, valid values are > 0 or -1 (infinite)");
      }

      this.sessionFactory = sessionFactory;

      this.name = name;

      this.remotingConnection = remotingConnection;

      this.connectionFactory = connectionFactory;

      this.cacheProducers = cacheProducers;

      executor = executorFactory.getExecutor();

      this.xa = xa;

      this.lazyAckBatchSize = lazyAckBatchSize;

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

      this.channel = channel;

      this.version = version;

      connectionRegistry = ConnectionRegistryImpl.instance;
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
                            connectionFactory.getConsumerMaxRate());
   }

   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final boolean direct,
                                        final int windowSize,
                                        final int maxRate) throws MessagingException
   {
      checkClosed();

      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(queueName,
                                                                              filterString,
                                                                              windowSize,
                                                                              maxRate);

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

   public ClientBrowser createBrowser(final SimpleString queueName) throws MessagingException
   {
      return createBrowser(queueName, null);
   }

   public ClientBrowser createBrowser(final SimpleString queueName, final SimpleString filterString) throws MessagingException
   {
      checkClosed();

      SessionCreateBrowserMessage request = new SessionCreateBrowserMessage(queueName, filterString);

      channel.sendBlocking(request);

      ClientBrowser browser = new ClientBrowserImpl(this, idGenerator.generateID(), channel);

      addBrowser(browser);

      return browser;
   }

   public ClientProducer createProducer(final SimpleString address) throws MessagingException
   {
      checkClosed();

      return createProducer(address, connectionFactory.getProducerWindowSize(), connectionFactory.getProducerMaxRate());
   }

   public ClientProducer createRateLimitedProducer(final SimpleString address, final int rate) throws MessagingException
   {
      checkClosed();

      return createProducer(address, -1, rate);
   }

   public ClientProducer createProducerWithWindowSize(final SimpleString address, final int windowSize) throws MessagingException
   {
      checkClosed();

      return createProducer(address, windowSize, -1);
   }

   private ClientProducer createProducer(final SimpleString address, final int windowSize, final int maxRate) throws MessagingException
   {
      return createProducer(address,
                            windowSize,
                            maxRate,
                            connectionFactory.isBlockOnNonPersistentSend(),
                            connectionFactory.isBlockOnPersistentSend());
   }

   public ClientProducer createProducer(final SimpleString address,
                                        final int windowSize,
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
         SessionCreateProducerMessage request = new SessionCreateProducerMessage(address, windowSize, maxRate);

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
                                           response.getInitialCredits(),
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

      // Flush any acks to the server
      acknowledgeInternal(false);

      channel.sendBlocking(new PacketImpl(PacketImpl.SESS_COMMIT));

      lastCommittedID = lastID;
   }

   public void rollback() throws MessagingException
   {
      checkClosed();

      // We tell each consumer to clear it's buffers and ignore any repeated
      // deliveries

      if (autoCommitAcks)
      {
         lastCommittedID = lastID;
      }

      for (ClientConsumerInternal consumer : consumers.values())
      {
         consumer.recover(lastCommittedID + 1);
      }

      // Flush any acks to the server
      acknowledgeInternal(false);

      toAckCount = 0;

      channel.sendBlocking(new PacketImpl(PacketImpl.SESS_ROLLBACK));
   }

   public void acknowledge() throws MessagingException
   {
      checkClosed();

      if (lastID + 1 != deliverID)
      {
         broken = true;
      }

      lastID = deliverID;

      toAckCount++;

      acked = false;

      if (deliveryExpired)
      {
         channel.send(new SessionCancelMessage(lastID, true));

         toAckCount = 0;

         acked = true;
      }
      else if (broken || toAckCount == lazyAckBatchSize)
      {
         acknowledgeInternal(blockOnAcknowledge);

         toAckCount = 0;

         if (autoCommitAcks)
         {
            lastCommittedID = lastID;
         }
      }
   }

   public synchronized void close() throws MessagingException
   {
      if (closed)
      {
         return;
      }

      try
      {
         closeChildren();

         // Flush any acks to the server
         acknowledgeInternal(false);

         channel.sendBlocking(new PacketImpl(SESS_CLOSE));
      }
      catch (Throwable ignore)
      {
         // Session close should always return without exception
      }

      doCleanup();
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

   public int getLazyAckBatchSize()
   {
      return lazyAckBatchSize;
   }

   public boolean isXA()
   {
      return xa;
   }

   public void start() throws MessagingException
   {
      checkClosed();

      channel.send(new PacketImpl(PacketImpl.SESS_START));
   }

   public void stop() throws MessagingException
   {
      checkClosed();

      channel.sendBlocking(new PacketImpl(PacketImpl.SESS_STOP));
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

   public void delivered(final long deliverID, final boolean expired)
   {
      this.deliverID = deliverID;

      deliveryExpired = expired;
   }

   public void addConsumer(final ClientConsumerInternal consumer)
   {
      consumers.put(consumer.getID(), consumer);
   }

   public void addProducer(final ClientProducerInternal producer)
   {
      producers.put(producer.getID(), producer);
   }

   public void addBrowser(final ClientBrowser browser)
   {
      browsers.put(browser.getID(), browser);
   }

   public void removeConsumer(final ClientConsumerInternal consumer) throws MessagingException
   {
      consumers.remove(consumer.getID());

      // 1. flush any unacked message to the server

      acknowledgeInternal(false);

      // 2. cancel all deliveries on server but not in tx

      channel.send(new SessionCancelMessage(-1, false));
   }

   public void removeProducer(final ClientProducerInternal producer)
   {
      producers.remove(producer.getID());

      if (cacheProducers && !producerCache.containsKey(producer.getAddress()))
      {
         producerCache.put(producer.getAddress(), producer);
      }
   }

   public void removeBrowser(final ClientBrowser browser)
   {
      browsers.remove(browser.getID());
   }

   public Set<ClientProducerInternal> getProducers()
   {
      return new HashSet<ClientProducerInternal>(producers.values());
   }

   public Set<ClientConsumerInternal> getConsumers()
   {
      return new HashSet<ClientConsumerInternal>(consumers.values());
   }

   public Set<ClientBrowser> getBrowsers()
   {
      return new HashSet<ClientBrowser>(browsers.values());
   }

   public Map<SimpleString, ClientProducerInternal> getProducerCache()
   {
      return new HashMap<SimpleString, ClientProducerInternal>(producerCache);
   }

   public synchronized void cleanUp() throws Exception
   {
      if (closed)
      {
         return;
      }

      try
      {
         cleanUpChildren();
      }
      finally
      {
         doCleanup();
      }
   }

   public void handleReceiveMessage(final long consumerID, final ClientMessage message) throws Exception
   {
      ClientConsumerInternal consumer = consumers.get(consumerID);

      if (consumer != null)
      {
         consumer.handleMessage(message);
      }
   }

   public void receiveProducerCredits(final long producerID, final int credits) throws Exception
   {
      ClientProducerInternal producer = producers.get(producerID);

      if (producer != null)
      {
         producer.receiveCredits(credits);
      }
   }

   public void handleFailover(final RemotingConnection backupConnection)
   {
      channel.lock();

      try
      {
         channel.transferConnection(backupConnection);

         remotingConnection = backupConnection;

         Packet request = new ReattachSessionMessage(name, channel.getLastReceivedCommandID());

         Channel channel1 = backupConnection.getChannel(1, false, -1);

         ReattachSessionResponseMessage response = (ReattachSessionResponseMessage)channel1.sendBlocking(request);

         channel.replayCommands(response.getLastReceivedCommandID());
      }
      catch (Throwable t)
      {
         log.error("Failed to handle failover", t);
      }
      finally
      {
         channel.unlock();
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

         // Need to flush any acks to server first
         acknowledgeInternal(false);

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
         // Need to flush any acks to server first
         acknowledgeInternal(false);

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

      // Note - don't need to flush acks since the previous end would have
      // done this

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
            // Need to flush any acks to server first
            acknowledgeInternal(false);

            packet = new SessionXAJoinMessage(xid);
         }
         else if (flags == XAResource.TMRESUME)
         {
            // Need to flush any acks to server first
            acknowledgeInternal(false);

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
         log.error("Caught jmsexecptione ", e);
         // This should never occur
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   // Public
   //----------------------------------------------------------------------------
   // -----------

   public void setForceNotSameRM(final boolean force)
   {
      forceNotSameRM = force;
   }

   public void setConnectionRegistry(final ConnectionRegistry registry)
   {
      connectionRegistry = registry;
   }

   public RemotingConnection getConnection()
   {
      return remotingConnection;
   }

   // Protected
   //----------------------------------------------------------------------------
   // --------

   // Package Private
   //----------------------------------------------------------------------------
   // --

   // Private
   //----------------------------------------------------------------------------
   // ----------

   private void checkXA() throws XAException
   {
      if (!xa)
      {
         log.error("Session is not XA");
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   private void acknowledgeInternal(final boolean block) throws MessagingException
   {
      if (acked)
      {
         return;
      }

      SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(lastID, !broken, block);

      if (block)
      {
         channel.sendBlocking(message);
      }
      else
      {
         channel.send(message);
      }

      acked = true;
   }

   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Session is closed");
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

      Set<ClientBrowser> browsersClone = new HashSet<ClientBrowser>(browsers.values());

      for (ClientBrowser browser : browsersClone)
      {
         browser.close();
      }
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

      Set<ClientBrowser> browsersClone = new HashSet<ClientBrowser>(browsers.values());

      for (ClientBrowser browser : browsersClone)
      {
         browser.cleanUp();
      }
   }

   private void doCleanup()
   {
      if (cacheProducers)
      {
         producerCache.clear();
      }

      channel.close();

      connectionRegistry.returnConnection(remotingConnection.getID());

      sessionFactory.removeSession(this);

      closed = true;
   }

   // Inner Classes
   //----------------------------------------------------------------------------
   // ----

}
