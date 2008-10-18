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

package org.jboss.messaging.core.server;

import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendManagementMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.util.SimpleString;

import javax.transaction.xa.Xid;
import java.util.List;

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
   long getID();

   String getUsername();

   String getPassword();

   void removeConsumer(ServerConsumer consumer) throws Exception;

   void removeProducer(ServerProducer producer) throws Exception;

   void close() throws Exception;

   void setStarted(boolean started) throws Exception;

   void promptDelivery(Queue queue);

   void send(ServerMessage msg) throws Exception;

   void sendScheduled(ServerMessage serverMessage, long scheduledDeliveryTime) throws Exception;

   void acknowledge(final long consumerID, final long messageID) throws Exception;

   void rollback() throws Exception;

   void commit() throws Exception;

   SessionXAResponseMessage XACommit(boolean onePhase, Xid xid) throws Exception;

   SessionXAResponseMessage XAEnd(Xid xid, boolean failed) throws Exception;

   SessionXAResponseMessage XAForget(Xid xid);

   SessionXAResponseMessage XAJoin(Xid xid) throws Exception;

   SessionXAResponseMessage XAPrepare(Xid xid) throws Exception;

   SessionXAResponseMessage XAResume(Xid xid) throws Exception;

   SessionXAResponseMessage XARollback(Xid xid) throws Exception;

   SessionXAResponseMessage XAStart(Xid xid);

   SessionXAResponseMessage XASuspend() throws Exception;

   List<Xid> getInDoubtXids() throws Exception;

   int getXATimeout();

   boolean setXATimeout(int timeoutSeconds);

   void addDestination(SimpleString address, boolean durable, boolean temporary) throws Exception;

   void removeDestination(SimpleString address, boolean durable) throws Exception;

   void createQueue(SimpleString address,
                    SimpleString queueName,
                    SimpleString filterString,
                    boolean durable,
                    boolean temporary) throws Exception;
  
   void deleteQueue(SimpleString queueName) throws Exception;

   SessionCreateConsumerResponseMessage createConsumer(SimpleString queueName,
                                                       SimpleString filterString,
                                                       int windowSize,
                                                       int maxRate,
                                                       boolean browseOnly) throws Exception;

   SessionCreateProducerResponseMessage createProducer(SimpleString address,
                                                       int windowSize,
                                                       int maxRate,
                                                       boolean autoGroupId) throws Exception;

   SessionQueueQueryResponseMessage executeQueueQuery(SimpleString queueName) throws Exception;

   SessionBindingQueryResponseMessage executeBindingQuery(SimpleString address) throws Exception;

   void closeConsumer(long consumerID) throws Exception;

   void closeProducer(long producerID) throws Exception;

   void receiveConsumerCredits(long consumerID, int credits) throws Exception;

   void sendProducerMessage(long producerID, ServerMessage message) throws Exception;

   void sendScheduledProducerMessage(long producerID, ServerMessage serverMessage, long scheduledDeliveryTime) throws Exception;

   int transferConnection(RemotingConnection newConnection, int lastReceivedCommandID);

   void handleManagementMessage(SessionSendManagementMessage message) throws Exception;

   void failedOver() throws Exception;
   
   void handleReplicatedDelivery(long consumerID, long messageID) throws Exception;
}
