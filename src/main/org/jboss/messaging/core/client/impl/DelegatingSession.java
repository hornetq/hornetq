/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.SendAcknowledgementHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.SimpleString;

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
   
   @Override
   protected void finalize() throws Throwable
   {
      if (!session.isClosed())
      {
         log.warn("I'm closing a ClientSession you left open. Please make sure you close all ClientSessions explicitly " + "before letting them go out of scope!");
         
         close();
      }

      super.finalize();
   }

   
   public DelegatingSession(final ClientSessionInternal session)
   {
      this.session = session;
   }

   public void acknowledge(long consumerID, long messageID) throws MessagingException
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

   public SessionBindingQueryResponseMessage bindingQuery(SimpleString address) throws MessagingException
   {
      return session.bindingQuery(address);
   }

   public void cleanUp() throws Exception
   {
      session.cleanUp();
   }

   public void close() throws MessagingException
   {
      session.close();
   }

   public void commit() throws MessagingException
   {
      session.commit();
   }

   public void commit(Xid xid, boolean onePhase) throws XAException
   {
      session.commit(xid, onePhase);
   }

   public MessagingBuffer createBuffer(int size)
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

   public ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString, boolean browseOnly) throws MessagingException
   {
      return session.createConsumer(queueName, filterString, browseOnly);
   }

   public ClientConsumer createConsumer(SimpleString queueName,
                                        SimpleString filterString,
                                        int windowSize,
                                        int maxRate,
                                        boolean browseOnly) throws MessagingException
   {
      return session.createConsumer(queueName, filterString, windowSize, maxRate, browseOnly);
   }

   public ClientConsumer createConsumer(SimpleString queueName, SimpleString filterString) throws MessagingException
   {
      return session.createConsumer(queueName, filterString);
   }

   public ClientConsumer createConsumer(SimpleString queueName) throws MessagingException
   {
      return session.createConsumer(queueName);
   }

   public ClientConsumer createConsumer(String queueName, String filterString, boolean browseOnly) throws MessagingException
   {
      return session.createConsumer(queueName, filterString, browseOnly);
   }

   public ClientConsumer createConsumer(String queueName,
                                        String filterString,
                                        int windowSize,
                                        int maxRate,
                                        boolean browseOnly) throws MessagingException
   {
      return session.createConsumer(queueName, filterString, windowSize, maxRate, browseOnly);
   }

   public ClientConsumer createConsumer(String queueName, String filterString) throws MessagingException
   {
      return session.createConsumer(queueName, filterString);
   }

   public ClientConsumer createConsumer(String queueName) throws MessagingException
   {
      return session.createConsumer(queueName);
   }

   public ClientProducer createProducer() throws MessagingException
   {
      return session.createProducer();
   }

   public ClientProducer createProducer(SimpleString address,
                                        int maxRate,
                                        boolean blockOnNonPersistentSend,
                                        boolean blockOnPersistentSend) throws MessagingException
   {
      return session.createProducer(address, maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
   }

   public ClientProducer createProducer(SimpleString address, int rate) throws MessagingException
   {
      return session.createProducer(address, rate);
   }

   public ClientProducer createProducer(SimpleString address) throws MessagingException
   {
      return session.createProducer(address);
   }

   public ClientProducer createProducer(String address,
                                        int maxRate,
                                        boolean blockOnNonPersistentSend,
                                        boolean blockOnPersistentSend) throws MessagingException
   {
      return session.createProducer(address, maxRate, blockOnNonPersistentSend, blockOnPersistentSend);
   }

   public ClientProducer createProducer(String address, int rate) throws MessagingException
   {
      return session.createProducer(address, rate);
   }

   public ClientProducer createProducer(String address) throws MessagingException
   {
      return session.createProducer(address);
   }

   public void createQueue(SimpleString address, SimpleString queueName, boolean durable) throws MessagingException
   {
      session.createQueue(address, queueName, durable);
   }

   public void createQueue(SimpleString address, SimpleString queueName, SimpleString filterString, boolean durable) throws MessagingException
   {
      session.createQueue(address, queueName, filterString, durable);
   }

   public void createQueue(String address, String queueName, boolean durable) throws MessagingException
   {
      session.createQueue(address, queueName, durable);
   }

   public void createQueue(String address, String queueName, String filterString, boolean durable) throws MessagingException
   {
      session.createQueue(address, queueName, filterString, durable);
   }

   public void createTemporaryQueue(SimpleString address, SimpleString queueName, SimpleString filter) throws MessagingException
   {
      session.createTemporaryQueue(address, queueName, filter);
   }

   public void createTemporaryQueue(SimpleString address, SimpleString queueName) throws MessagingException
   {
      session.createTemporaryQueue(address, queueName);
   }

   public void createTemporaryQueue(String address, String queueName, String filter) throws MessagingException
   {
      session.createTemporaryQueue(address, queueName, filter);
   }

   public void createTemporaryQueue(String address, String queueName) throws MessagingException
   {
      session.createTemporaryQueue(address, queueName);
   }

   public void deleteQueue(SimpleString queueName) throws MessagingException
   {
      session.deleteQueue(queueName);
   }

   public void deleteQueue(String queueName) throws MessagingException
   {
      session.deleteQueue(queueName);
   }

   public void end(Xid xid, int flags) throws XAException
   {
      session.end(xid, flags);
   }

   public void expire(long consumerID, long messageID) throws MessagingException
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

   public SessionQueueQueryResponseMessage queueQuery(SimpleString queueName) throws MessagingException
   {
      return session.queueQuery(queueName);
   }

   public Xid[] recover(int flag) throws XAException
   {
      return session.recover(flag);
   }

   public void removeConsumer(ClientConsumerInternal consumer) throws MessagingException
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

   public void rollback() throws MessagingException
   {
      session.rollback();
   }

   public void rollback(boolean considerLastMessageAsDelivered) throws MessagingException
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

   public void start() throws MessagingException
   {
      session.start();
   }

   public void start(Xid xid, int flags) throws XAException
   {
      session.start(xid, flags);
   }

   public void stop() throws MessagingException
   {
      session.stop();
   }

   public ConnectionManager getConnectionManager()
   {
      return session.getConnectionManager();
   }

   public void setForceNotSameRM(boolean force)
   {
      session.setForceNotSameRM(force);
   }   
}
