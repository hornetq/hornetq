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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.SendAcknowledgementHandler;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.SimpleString;

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
   private static final Logger log = Logger.getLogger(DelegatingSession.class);

   private final ClientSessionInternal session;

   private Exception creationStack;
   
   private static Set<DelegatingSession> sessions = new ConcurrentHashSet<DelegatingSession>();

   public static volatile boolean debug;
   
   public static void dumpSessionCreationStacks()
   {
      log.info("**** Dumping session creation stacks ****");
      
      for (DelegatingSession session: sessions)
      {
         log.info("session created", session.creationStack);
      }
   }
   
   @Override
   protected void finalize() throws Throwable
   {
      if (!session.isClosed())
      {
         log.warn("I'm closing a core ClientSession you left open. Please make sure you close all ClientSessions explicitly " + "before letting them go out of scope!");

         log.warn("The ClientSession you didn't close was created here:", creationStack);

         close();
      }

      super.finalize();
   }

   public DelegatingSession(final ClientSessionInternal session)
   {
      this.session = session;

      this.creationStack = new Exception();
      
      if (debug)
      {
         sessions.add(this);
      }
   }

   public void acknowledge(long consumerID, long messageID) throws HornetQException
   {
      session.acknowledge(consumerID, messageID);
   }

   public void addConsumer(ClientConsumerInternal consumer)
   {
      session.addConsumer(consumer);
   }

   public void addFailureListener(FailureListener listener)
   {
      session.addFailureListener(listener);
   }

   public void addProducer(ClientProducerInternal producer)
   {
      session.addProducer(producer);
   }

   public SessionBindingQueryResponseMessage bindingQuery(SimpleString address) throws HornetQException
   {
      return session.bindingQuery(address);
   }

   public void cleanUp() throws Exception
   {
      session.cleanUp();
   }

   public void close() throws HornetQException
   {
      if (debug)
      {
         sessions.remove(this);
      }
      
      session.close();
   }

   public void commit() throws HornetQException
   {
      session.commit();
   }

   public void commit(Xid xid, boolean onePhase) throws XAException
   {
      session.commit(xid, onePhase);
   }

   public HornetQBuffer createBuffer(int size)
   {
      return session.createBuffer(size);
   }

   public ClientMessage createClientMessage(boolean durable)
   {
      return session.createClientMessage(durable);
   }

   public ClientMessage createClientMessage(byte type, boolean durable, long expiration, long timestamp, byte priority)
   {
      return session.createClientMessage(type, durable, expiration, timestamp, priority);
   }

   public ClientMessage createClientMessage(byte type, boolean durable)
   {
      return session.createClientMessage(type, durable);
   }

   public ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString, boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, filterString, browseOnly);
   }

   public ClientConsumer createConsumer(SimpleString queueName,
                                        SimpleString filterString,
                                        int windowSize,
                                        int maxRate,
                                        boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, filterString, windowSize, maxRate, browseOnly);
   }

   public ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString) throws HornetQException
   {
      return session.createConsumer(queueName, filterString);
   }

   public ClientConsumer createConsumer(SimpleString queueName) throws HornetQException
   {
      return session.createConsumer(queueName);
   }

   public ClientConsumer createConsumer(String queueName, String filterString, boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, filterString, browseOnly);
   }

   public ClientConsumer createConsumer(String queueName,
                                        String filterString,
                                        int windowSize,
                                        int maxRate,
                                        boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, filterString, windowSize, maxRate, browseOnly);
   }

   public ClientConsumer createConsumer(String queueName, String filterString) throws HornetQException
   {
      return session.createConsumer(queueName, filterString);
   }

   public ClientConsumer createConsumer(String queueName) throws HornetQException
   {
      return session.createConsumer(queueName);
   }
   
   public ClientConsumer createConsumer(SimpleString queueName, boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, browseOnly);
   }

   public ClientConsumer createConsumer(String queueName, boolean browseOnly) throws HornetQException
   {
      return session.createConsumer(queueName, browseOnly);
   }

   public ClientProducer createProducer() throws HornetQException
   {
      return session.createProducer();
   }

   public ClientProducer createProducer(SimpleString address,
                                        int maxRate,
                                        boolean blockOnNonPersistentSend,
                                        boolean blockOnPersistentSend) throws HornetQException
   {
      return session.createProducer(address, maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
   }

   public ClientProducer createProducer(SimpleString address, int rate) throws HornetQException
   {
      return session.createProducer(address, rate);
   }

   public ClientProducer createProducer(SimpleString address) throws HornetQException
   {
      return session.createProducer(address);
   }

   public ClientProducer createProducer(String address,
                                        int maxRate,
                                        boolean blockOnNonPersistentSend,
                                        boolean blockOnPersistentSend) throws HornetQException
   {
      return session.createProducer(address, maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
   }

   public ClientProducer createProducer(String address, int rate) throws HornetQException
   {
      return session.createProducer(address, rate);
   }

   public ClientProducer createProducer(String address) throws HornetQException
   {
      return session.createProducer(address);
   }

   public void createQueue(String address, String queueName) throws HornetQException
   {
      session.createQueue(address, queueName);
   }
   
   public void createQueue(SimpleString address, SimpleString queueName, boolean durable) throws HornetQException
   {
      session.createQueue(address, queueName, durable);
   }

   public void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable) throws HornetQException
   {
      session.createQueue(address, queueName, filterString, durable);
   }

   public void createQueue(String address, String queueName, boolean durable) throws HornetQException
   {
      session.createQueue(address, queueName, durable);
   }

   public void createQueue(String address, String queueName, String filterString, boolean durable) throws HornetQException
   {
      session.createQueue(address, queueName, filterString, durable);
   }

   public void createTemporaryQueue(SimpleString address, SimpleString queueName, SimpleString filter) throws HornetQException
   {
      session.createTemporaryQueue(address, queueName, filter);
   }

   public void createTemporaryQueue(SimpleString address, SimpleString queueName) throws HornetQException
   {
      session.createTemporaryQueue(address, queueName);
   }

   public void createTemporaryQueue(String address, String queueName, String filter) throws HornetQException
   {
      session.createTemporaryQueue(address, queueName, filter);
   }

   public void createTemporaryQueue(String address, String queueName) throws HornetQException
   {
      session.createTemporaryQueue(address, queueName);
   }

   public void deleteQueue(SimpleString queueName) throws HornetQException
   {
      session.deleteQueue(queueName);
   }

   public void deleteQueue(String queueName) throws HornetQException
   {
      session.deleteQueue(queueName);
   }

   public void end(Xid xid, int flags) throws XAException
   {
      session.end(xid, flags);
   }

   public void expire(long consumerID, long messageID) throws HornetQException
   {
      session.expire(consumerID, messageID);
   }

   public void forget(Xid xid) throws XAException
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

   public boolean handleFailover(RemotingConnection backupConnection)
   {
      return session.handleFailover(backupConnection);
   }
   
   public boolean handleReattach(RemotingConnection backupConnection)
   {
      return session.handleReattach(backupConnection);
   }

   public void handleReceiveContinuation(long consumerID, SessionReceiveContinuationMessage continuation) throws Exception
   {
      session.handleReceiveContinuation(consumerID, continuation);
   }

   public void handleReceiveLargeMessage(long consumerID, SessionReceiveMessage message) throws Exception
   {
      session.handleReceiveLargeMessage(consumerID, message);
   }

   public void handleReceiveMessage(long consumerID, SessionReceiveMessage message) throws Exception
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

   public boolean isSameRM(XAResource xares) throws XAException
   {
      return session.isSameRM(xares);
   }

   public boolean isXA()
   {
      return session.isXA();
   }

   public int prepare(Xid xid) throws XAException
   {
      return session.prepare(xid);
   }

   public SessionQueueQueryResponseMessage queueQuery(SimpleString queueName) throws HornetQException
   {
      return session.queueQuery(queueName);
   }

   public Xid[] recover(int flag) throws XAException
   {
      return session.recover(flag);
   }

   public void removeConsumer(ClientConsumerInternal consumer) throws HornetQException
   {
      session.removeConsumer(consumer);
   }

   public boolean removeFailureListener(FailureListener listener)
   {
      return session.removeFailureListener(listener);
   }

   public void removeProducer(ClientProducerInternal producer)
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

   public void rollback(boolean considerLastMessageAsDelivered) throws HornetQException
   {
      session.rollback(considerLastMessageAsDelivered);
   }

   public void rollback(Xid xid) throws XAException
   {
      session.rollback(xid);
   }

   public void setSendAcknowledgementHandler(SendAcknowledgementHandler handler)
   {
      session.setSendAcknowledgementHandler(handler);
   }

   public boolean setTransactionTimeout(int seconds) throws XAException
   {
      return session.setTransactionTimeout(seconds);
   }

   public void start() throws HornetQException
   {
      session.start();
   }

   public void start(Xid xid, int flags) throws XAException
   {
      session.start(xid, flags);
   }

   public void stop() throws HornetQException
   {
      session.stop();
   }

   public FailoverManager getConnectionManager()
   {
      return session.getConnectionManager();
   }

   public void setForceNotSameRM(boolean force)
   {
      session.setForceNotSameRM(force);
   }
   
   public void workDone()
   {
      session.workDone();
   }
}
