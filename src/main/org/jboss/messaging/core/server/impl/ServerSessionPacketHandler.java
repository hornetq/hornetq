/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_ACKNOWLEDGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_ADD_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BINDINGQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CONSUMER_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATEQUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_DELETE_QUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_EXPIRED;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_FAILOVER_COMPLETE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_FLOWTOKEN;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_QUEUEQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_REMOVE_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_REPLICATE_DELIVERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_SEND;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_SEND_CONTINUATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_STOP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_END;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_FORGET;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_GET_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_INDOUBT_XIDS;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_JOIN;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_PREPARE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_RESUME;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SET_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_XA_SUSPEND;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.RollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionExpiredMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReplicateDeliveryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendContinuationMessage;
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
import org.jboss.messaging.core.server.ServerSession;

/**
 * A ServerSessionPacketHandler
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 */
public class ServerSessionPacketHandler implements ChannelHandler
{
   private static final Logger log = Logger.getLogger(ServerSessionPacketHandler.class);

   private final ServerSession session;

   private final Channel channel;

   public ServerSessionPacketHandler(final ServerSession session, final Channel channel)

   {
      this.session = session;

      this.channel = channel;
   }

   public long getID()
   {
      return session.getID();
   }

   public void handlePacket(final Packet packet)
   {
      byte type = packet.getType();

      try
      {
         switch (type)
         {
            case SESS_CREATECONSUMER:
            {
               SessionCreateConsumerMessage request = (SessionCreateConsumerMessage)packet;
               session.handleCreateConsumer(request);
               break;
            }
            case SESS_CREATEQUEUE:
            {
               SessionCreateQueueMessage request = (SessionCreateQueueMessage)packet;
               session.handleCreateQueue(request);
               break;
            }
            case SESS_DELETE_QUEUE:
            {
               SessionDeleteQueueMessage request = (SessionDeleteQueueMessage)packet;
               session.handleDeleteQueue(request);
               break;
            }
            case SESS_QUEUEQUERY:
            {
               SessionQueueQueryMessage request = (SessionQueueQueryMessage)packet;
               session.handleExecuteQueueQuery(request);
               break;
            }
            case SESS_BINDINGQUERY:
            {
               SessionBindingQueryMessage request = (SessionBindingQueryMessage)packet;
               session.handleExecuteBindingQuery(request);
               break;
            }
            case SESS_ACKNOWLEDGE:
            {
               SessionAcknowledgeMessage message = (SessionAcknowledgeMessage)packet;
               session.handleAcknowledge(message);
               break;
            }
            case SESS_EXPIRED:
            {
               SessionExpiredMessage message = (SessionExpiredMessage)packet;
               session.handleExpired(message);
               break;
            }
            case SESS_COMMIT:
            {
               session.handleCommit(packet);
               break;
            }
            case SESS_ROLLBACK:
            {
               session.handleRollback((RollbackMessage)packet);
               break;
            }
            case SESS_XA_COMMIT:
            {
               SessionXACommitMessage message = (SessionXACommitMessage)packet;
               session.handleXACommit(message);
               break;
            }
            case SESS_XA_END:
            {
               SessionXAEndMessage message = (SessionXAEndMessage)packet;
               session.handleXAEnd(message);
               break;
            }
            case SESS_XA_FORGET:
            {
               SessionXAForgetMessage message = (SessionXAForgetMessage)packet;
               session.handleXAForget(message);
               break;
            }
            case SESS_XA_JOIN:
            {
               SessionXAJoinMessage message = (SessionXAJoinMessage)packet;
               session.handleXAJoin(message);
               break;
            }
            case SESS_XA_RESUME:
            {
               SessionXAResumeMessage message = (SessionXAResumeMessage)packet;
               session.handleXAResume(message);
               break;
            }
            case SESS_XA_ROLLBACK:
            {
               SessionXARollbackMessage message = (SessionXARollbackMessage)packet;
               session.handleXARollback(message);
               break;
            }
            case SESS_XA_START:
            {
               SessionXAStartMessage message = (SessionXAStartMessage)packet;
               session.handleXAStart(message);
               break;
            }
            case SESS_XA_SUSPEND:
            {
               session.handleXASuspend(packet);
               break;
            }
            case SESS_XA_PREPARE:
            {
               SessionXAPrepareMessage message = (SessionXAPrepareMessage)packet;
               session.handleXAPrepare(message);
               break;
            }
            case SESS_XA_INDOUBT_XIDS:
            {
               session.handleGetInDoubtXids(packet);
               break;
            }
            case SESS_XA_GET_TIMEOUT:
            {
               session.handleGetXATimeout(packet);
               break;
            }
            case SESS_XA_SET_TIMEOUT:
            {
               SessionXASetTimeoutMessage message = (SessionXASetTimeoutMessage)packet;
               session.handleSetXATimeout(message);
               break;
            }
            case SESS_ADD_DESTINATION:
            {
               SessionAddDestinationMessage message = (SessionAddDestinationMessage)packet;
               session.handleAddDestination(message);
               break;
            }
            case SESS_REMOVE_DESTINATION:
            {
               SessionRemoveDestinationMessage message = (SessionRemoveDestinationMessage)packet;
               session.handleRemoveDestination(message);
               break;
            }
            case SESS_START:
            {
               session.handleStart(packet);
               break;
            }
            case SESS_FAILOVER_COMPLETE:
            {
               session.handleFailedOver(packet);
               break;
            }
            case SESS_STOP:
            {
               session.handleStop(packet);
               break;
            }
            case SESS_CLOSE:
            {
               session.handleClose(packet);
               break;
            }
            case SESS_CONSUMER_CLOSE:
            {
               SessionConsumerCloseMessage message = (SessionConsumerCloseMessage)packet;
               session.handleCloseConsumer(message);
               break;
            }
            case SESS_FLOWTOKEN:
            {
               SessionConsumerFlowCreditMessage message = (SessionConsumerFlowCreditMessage)packet;
               session.handleReceiveConsumerCredits(message);
               break;
            }
            case SESS_SEND:
            {
               SessionSendMessage message = (SessionSendMessage)packet;
               if (message.isLargeMessage())
               {
                  session.handleSendLargeMessage(message);
               }
               else
               {
                  session.handleSend(message);
               }
               break;
            }
            case SESS_SEND_CONTINUATION:
            {
               SessionSendContinuationMessage message = (SessionSendContinuationMessage)packet;
               session.handleSendContinuations(message);
               break;
            }
            case SESS_REPLICATE_DELIVERY:
            {
               SessionReplicateDeliveryMessage message = (SessionReplicateDeliveryMessage)packet;
               session.handleReplicatedDelivery(message);
               break;
            }
         }
      }
      catch (Throwable t)
      {
         log.error("Caught unexpected exception", t);
      }

      channel.replicateComplete();
   }
}
