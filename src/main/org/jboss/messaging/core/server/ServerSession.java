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

import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionExpiredMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionProducerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReplicateDeliveryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;

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
   
   void promptDelivery(Queue queue);

   void send(ServerMessage msg) throws Exception;

   void handleAcknowledge(final SessionAcknowledgeMessage packet);
   
   void handleExpired(final SessionExpiredMessage packet);

   void handleRollback(Packet packet);

   void handleCommit(Packet packet);

   void handleXACommit(SessionXACommitMessage packet);

   void handleXAEnd(SessionXAEndMessage packet);

   void handleXAForget(SessionXAForgetMessage packet);

   void handleXAJoin(SessionXAJoinMessage packet);

   void handleXAPrepare(SessionXAPrepareMessage packet);

   void handleXAResume(SessionXAResumeMessage packet);

   void handleXARollback(SessionXARollbackMessage packet);

   void handleXAStart(SessionXAStartMessage packet);

   void handleXASuspend(Packet packet);

   void handleGetInDoubtXids(Packet packet);

   void handleGetXATimeout(Packet packet);

   void handleSetXATimeout(SessionXASetTimeoutMessage packet);

   void handleAddDestination(SessionAddDestinationMessage packet);
   
   void handleStart(Packet packet);
   
   void handleStop(Packet packet);

   void handleRemoveDestination(SessionRemoveDestinationMessage packet);

   void handleCreateQueue(SessionCreateQueueMessage packet);
  
   void handleDeleteQueue(SessionDeleteQueueMessage packet);

   void handleCreateConsumer(SessionCreateConsumerMessage packet);

   void handleCreateProducer(SessionCreateProducerMessage packet);

   void handleExecuteQueueQuery(SessionQueueQueryMessage packet);

   void handleExecuteBindingQuery(SessionBindingQueryMessage packet);

   void handleCloseConsumer(SessionConsumerCloseMessage packet);

   void handleCloseProducer(SessionProducerCloseMessage packet);

   void handleReceiveConsumerCredits(SessionConsumerFlowCreditMessage packet);

   void handleSendProducerMessage(SessionSendMessage packet);

   void handleFailedOver(Packet packet);
   
   void handleClose(Packet packet);
   
   void handleReplicatedDelivery(SessionReplicateDeliveryMessage packet);
   
   int transferConnection(RemotingConnection newConnection, int lastReceivedCommandID);
   
   Channel getChannel();
}
