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

import java.util.Set;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.FailoverEventListener;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.ConcurrentHashSet;

/**
 * A DelegatingSession
 *
 * We wrap the real session, so we can add a finalizer on this and close the session
 * on GC if it has not already been closed
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class DelegatingSession implements ClientSessionInternal
{
   private final ClientSessionInternal session;

   private final Exception creationStack;

   private volatile boolean closed;

   private static Set<DelegatingSession> sessions = new ConcurrentHashSet<DelegatingSession>();

   public static volatile boolean debug;

   public static void dumpSessionCreationStacks()
   {
      HornetQClientLogger.LOGGER.dumpingSessionStacks();

      for (DelegatingSession session : DelegatingSession.sessions)
      {
         HornetQClientLogger.LOGGER.dumpingSessionStack(session.creationStack);
      }
   }

   @Override
   protected void finalize() throws Throwable
   {
      // In some scenarios we have seen the JDK finalizing the DelegatingSession while the call to session.close() was still in progress
      //
      if (!closed && !session.isClosed())
      {
         HornetQClientLogger.LOGGER.clientSessionNotClosed(creationStack, System.identityHashCode(this));

         close();
      }

      super.finalize();
   }

   public DelegatingSession(final ClientSessionInternal session)
   {
      this.session = session;

      creationStack = new Exception();

      if (DelegatingSession.debug)
      {
         DelegatingSession.sessions.add(this);
      }
   }

   public void acknowledge(final long consumerID, final long messageID) throws HornetQException
   {
      session.acknowledge(consumerID, messageID);
   }

   public void individualAcknowledge(final long consumerID, final long messageID) throws HornetQException
   {
      session.individualAcknowledge(consumerID, messageID);
   }

   public void addConsumer(final ClientConsumerInternal consumer)
   {
      session.addConsumer(consumer);
   }

   public void addFailureListener(final SessionFailureListener listener)
   {
      session.addFailureListener(listener);
   }

   public void addFailoverListener(FailoverEventListener listener)
   {
     session.addFailoverListener(listener);
   }

   public void addProducer(final ClientProducerInternal producer)
   {
      session.addProducer(producer);
   }

   public BindingQuery bindingQuery(final SimpleString address) throws HornetQException
   {
      return session.bindingQuery(address);
   }

   public void forceDelivery(final long consumerID, final long sequence) throws HornetQException
   {
      session.forceDelivery(consumerID, sequence);
   }


   public void cleanUp(boolean failingOver) throws HornetQException
   {
      session.cleanUp(failingOver);
   }

   public void close() throws HornetQException
   {
      closed = true;

      if (DelegatingSession.debug)
      {
         DelegatingSession.sessions.remove(this);
      }

      session.close();
   }

   public void commit() throws HornetQException
   {
      session.commit();
   }

   public void commit(final Xid xid, final boolean onePhase) throws XAException
   {
      session.commit(xid, onePhase);
   }

   public ClientMessage createMessage(final boolean durable)
   {
      return session.createMessage(durable);
   }

   public ClientMessage createMessage(final byte type,
                                            final boolean durable,
                                            final long expiration,
                                            final long timestamp,
                                            final byte priority)
   {
      return session.createMessage(type, durable, expiration, timestamp, priority);
   }

   public ClientMessage createMessage(final byte type, final boolean durable)
   {
      return session.createMessage(type, durable);
   }

   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, filterString, browseOnly);
   }

   public ClientConsumer createConsumer(final SimpleString queueName,
                                        final SimpleString filterString,
                                        final int windowSize,
                                        final int maxRate,
                                        final boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, filterString, windowSize, maxRate, browseOnly);
   }

   public ClientConsumer createConsumer(final SimpleString queueName, final SimpleString filterString) throws HornetQException
   {
      return session.createConsumer(queueName, filterString);
   }

   public ClientConsumer createConsumer(final SimpleString queueName) throws HornetQException
   {
      return session.createConsumer(queueName);
   }

   public ClientConsumer createConsumer(final String queueName, final String filterString, final boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, filterString, browseOnly);
   }

   public ClientConsumer createConsumer(final String queueName,
                                        final String filterString,
                                        final int windowSize,
                                        final int maxRate,
                                        final boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, filterString, windowSize, maxRate, browseOnly);
   }

   public ClientConsumer createConsumer(final String queueName, final String filterString) throws HornetQException
   {
      return session.createConsumer(queueName, filterString);
   }

   public ClientConsumer createConsumer(final String queueName) throws HornetQException
   {
      return session.createConsumer(queueName);
   }

   public ClientConsumer createConsumer(final SimpleString queueName, final boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, browseOnly);
   }

   public ClientConsumer createConsumer(final String queueName, final boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, browseOnly);
   }

   public ClientProducer createProducer() throws HornetQException
   {
      return session.createProducer();
   }

   public ClientProducer createProducer(final SimpleString address, final int rate) throws HornetQException
   {
      return session.createProducer(address, rate);
   }

   public ClientProducer createProducer(final SimpleString address) throws HornetQException
   {
      return session.createProducer(address);
   }

   public ClientProducer createProducer(final String address) throws HornetQException
   {
      return session.createProducer(address);
   }

   public void createQueue(final String address, final String queueName) throws HornetQException
   {
      session.createQueue(address, queueName);
   }

   public void createQueue(final SimpleString address, final SimpleString queueName) throws HornetQException
   {
      session.createQueue(address, queueName);
   }

   public void createQueue(final SimpleString address, final SimpleString queueName, final boolean durable) throws HornetQException
   {
      session.createQueue(address, queueName, durable);
   }

   @Override
   public void createTransientQueue(SimpleString address, SimpleString queueName) throws HornetQException
   {
      session.createTransientQueue(address, queueName);
   }

   @Override
   public void createTransientQueue(SimpleString address, SimpleString queueName, SimpleString filter) throws HornetQException
   {
      session.createTransientQueue(address, queueName, filter);
   }

   public void createQueue(final SimpleString address,
                           final SimpleString queueName,
                           final SimpleString filterString,
                           final boolean durable) throws HornetQException
   {
      session.createQueue(address, queueName, filterString, durable);
   }

   public void createQueue(final String address, final String queueName, final boolean durable) throws HornetQException
   {
      session.createQueue(address, queueName, durable);
   }

   public void createQueue(final String address,
                           final String queueName,
                           final String filterString,
                           final boolean durable) throws HornetQException
   {
      session.createQueue(address, queueName, filterString, durable);
   }

   public void createTemporaryQueue(final SimpleString address, final SimpleString queueName, final SimpleString filter) throws HornetQException
   {
      session.createTemporaryQueue(address, queueName, filter);
   }

   public void createTemporaryQueue(final SimpleString address, final SimpleString queueName) throws HornetQException
   {
      session.createTemporaryQueue(address, queueName);
   }

   public void createTemporaryQueue(final String address, final String queueName, final String filter) throws HornetQException
   {
      session.createTemporaryQueue(address, queueName, filter);
   }

   public void createTemporaryQueue(final String address, final String queueName) throws HornetQException
   {
      session.createTemporaryQueue(address, queueName);
   }

   public void deleteQueue(final SimpleString queueName) throws HornetQException
   {
      session.deleteQueue(queueName);
   }

   public void deleteQueue(final String queueName) throws HornetQException
   {
      session.deleteQueue(queueName);
   }

   public void end(final Xid xid, final int flags) throws XAException
   {
      session.end(xid, flags);
   }

   public void expire(final long consumerID, final long messageID) throws HornetQException
   {
      session.expire(consumerID, messageID);
   }

   public void forget(final Xid xid) throws XAException
   {
      session.forget(xid);
   }

   public RemotingConnection getConnection()
   {
      return session.getConnection();
   }

   public int getMinLargeMessageSize()
   {
      return session.getMinLargeMessageSize();
   }

   public String getName()
   {
      return session.getName();
   }

   public int getTransactionTimeout() throws XAException
   {
      return session.getTransactionTimeout();
   }

   public int getVersion()
   {
      return session.getVersion();
   }

   public XAResource getXAResource()
   {
      return session.getXAResource();
   }

   public void preHandleFailover(CoreRemotingConnection connection)
   {
      session.preHandleFailover(connection);
   }

   public void handleFailover(final CoreRemotingConnection backupConnection)
   {
      session.handleFailover(backupConnection);
   }

   public void handleReceiveContinuation(final long consumerID, final SessionReceiveContinuationMessage continuation) throws Exception
   {
      session.handleReceiveContinuation(consumerID, continuation);
   }

   public void handleReceiveLargeMessage(final long consumerID, final SessionReceiveLargeMessage message) throws Exception
   {
      session.handleReceiveLargeMessage(consumerID, message);
   }

   public void handleReceiveMessage(final long consumerID, final SessionReceiveMessage message) throws Exception
   {
      session.handleReceiveMessage(consumerID, message);
   }

   public boolean isAutoCommitAcks()
   {
      return session.isAutoCommitAcks();
   }

   public boolean isAutoCommitSends()
   {
      return session.isAutoCommitSends();
   }

   public boolean isBlockOnAcknowledge()
   {
      return session.isBlockOnAcknowledge();
   }

   public boolean isCacheLargeMessageClient()
   {
      return session.isCacheLargeMessageClient();
   }

   public boolean isClosed()
   {
      return session.isClosed();
   }

   public boolean isSameRM(final XAResource xares) throws XAException
   {
      return session.isSameRM(xares);
   }

   public boolean isXA()
   {
      return session.isXA();
   }

   public int prepare(final Xid xid) throws XAException
   {
      return session.prepare(xid);
   }

   public QueueQuery queueQuery(final SimpleString queueName) throws HornetQException
   {
      return session.queueQuery(queueName);
   }

   public Xid[] recover(final int flag) throws XAException
   {
      return session.recover(flag);
   }

   public void removeConsumer(final ClientConsumerInternal consumer) throws HornetQException
   {
      session.removeConsumer(consumer);
   }

   public boolean removeFailureListener(final SessionFailureListener listener)
   {
      return session.removeFailureListener(listener);
   }

   public boolean removeFailoverListener(FailoverEventListener listener)
   {
      return session.removeFailoverListener(listener);
   }

   public void removeProducer(final ClientProducerInternal producer)
   {
      session.removeProducer(producer);
   }

   public void returnBlocking()
   {
      session.returnBlocking();
   }

   public void rollback() throws HornetQException
   {
      session.rollback();
   }

   public boolean isRollbackOnly()
   {
      return session.isRollbackOnly();
   }

   public void rollback(final boolean considerLastMessageAsDelivered) throws HornetQException
   {
      session.rollback(considerLastMessageAsDelivered);
   }

   public void rollback(final Xid xid) throws XAException
   {
      session.rollback(xid);
   }

   public void setSendAcknowledgementHandler(final SendAcknowledgementHandler handler)
   {
      session.setSendAcknowledgementHandler(handler);
   }

   public boolean setTransactionTimeout(final int seconds) throws XAException
   {
      return session.setTransactionTimeout(seconds);
   }

   public void resetIfNeeded() throws HornetQException
   {
      session.resetIfNeeded();
   }

   public void start() throws HornetQException
   {
      session.start();
   }

   public void start(final Xid xid, final int flags) throws XAException
   {
      session.start(xid, flags);
   }

   public void stop() throws HornetQException
   {
      session.stop();
   }

   public ClientSessionFactoryInternal getSessionFactory()
   {
      return session.getSessionFactory();
   }

   public void setForceNotSameRM(final boolean force)
   {
      session.setForceNotSameRM(force);
   }

   public void workDone()
   {
      session.workDone();
   }

   public void sendProducerCreditsMessage(final int credits, final SimpleString address)
   {
      session.sendProducerCreditsMessage(credits, address);
   }

   public ClientProducerCredits getCredits(final SimpleString address, final boolean anon)
   {
      return session.getCredits(address, anon);
   }

   public void returnCredits(final SimpleString address)
   {
      session.returnCredits(address);
   }

   public void handleReceiveProducerCredits(final SimpleString address, final int credits)
   {
      session.handleReceiveProducerCredits(address, credits);
   }

   public void handleReceiveProducerFailCredits(final SimpleString address, final int credits)
   {
      session.handleReceiveProducerFailCredits(address, credits);
   }

   public ClientProducerCreditManager getProducerCreditManager()
   {
      return session.getProducerCreditManager();
   }

   public void setAddress(Message message, SimpleString address)
   {
      session.setAddress(message, address);
   }

   public void setPacketSize(int packetSize)
   {
      session.setPacketSize(packetSize);
   }

   public void addMetaData(String key, String data) throws HornetQException
   {
      session.addMetaData(key, data);
   }

   public boolean isCompressLargeMessages()
   {
      return session.isCompressLargeMessages();
   }

   @Override
   public String toString()
   {
      return "DelegatingSession [session=" + session + "]";
   }

   @Override
   public Channel getChannel()
   {
      return session.getChannel();
   }

   @Override
   public void addUniqueMetaData(String key, String data) throws HornetQException
   {
      session.addUniqueMetaData(key, data);

   }

   public void startCall()
   {
      session.startCall();
   }

   public void endCall()
   {
      session.endCall();
   }

   @Override
   public void setStopSignal()
   {
      session.setStopSignal();
   }
}
