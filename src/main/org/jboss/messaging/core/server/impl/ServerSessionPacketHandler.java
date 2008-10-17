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
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_RESET;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CONSUMER_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATEPRODUCER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_CREATEQUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_DELETE_QUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_FAILOVER_COMPLETE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_FLOWTOKEN;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_MANAGEMENT_SEND;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_PRODUCER_CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_QUEUEQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_REMOVE_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_SCHEDULED_SEND;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_SEND;
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

import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.DelayedResult;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.NullResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserResetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionProducerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReplicateDeliveryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionScheduledSendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendManagementMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.jboss.messaging.core.server.ServerMessage;
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

   private final StorageManager storageManager;
   
   public ServerSessionPacketHandler(final ServerSession session,
                                     final Channel channel,
                                     final StorageManager storageManager)

   {
      this.session = session;

      this.channel = channel;

      this.storageManager = storageManager;
   }

   public long getID()
   {
      return session.getID();
   }

   public void handlePacket(final Packet packet)
   {
      byte type = packet.getType();
      
      if (type == SESS_SEND || type == SESS_SCHEDULED_SEND)
      {
         SessionSendMessage send = (SessionSendMessage)packet;

         ServerMessage msg = send.getServerMessage();

         if (msg.getMessageID() == 0L)
         {
            // must generate message id here, so we know they are in sync on live and backup
            long id = storageManager.generateUniqueID();

            send.getServerMessage().setMessageID(id);
         }
      }

      Packet response = null;
      
      DelayedResult result = channel.replicatePacket(packet);
      
      try
      {
         switch (type)
         {
            case SESS_CREATECONSUMER:
            {
               SessionCreateConsumerMessage request = (SessionCreateConsumerMessage)packet;
               response = session.createConsumer(request.getQueueName(),
                                                 request.getFilterString(),
                                                 request.getWindowSize(),
                                                 request.getMaxRate());
               break;
            }
            case SESS_CREATEQUEUE:
            {
               SessionCreateQueueMessage request = (SessionCreateQueueMessage)packet;
               session.createQueue(request.getAddress(),
                                   request.getQueueName(),
                                   request.getFilterString(),
                                   request.isDurable(),
                                   request.isTemporary());
               response = new NullResponseMessage();
               break;
            }
            case SESS_DELETE_QUEUE:
            {
               SessionDeleteQueueMessage request = (SessionDeleteQueueMessage)packet;
               session.deleteQueue(request.getQueueName());
               response = new NullResponseMessage();
               break;
            }
            case SESS_QUEUEQUERY:
            {
               SessionQueueQueryMessage request = (SessionQueueQueryMessage)packet;
               response = session.executeQueueQuery(request.getQueueName());
               break;
            }
            case SESS_BINDINGQUERY:
            {
               SessionBindingQueryMessage request = (SessionBindingQueryMessage)packet;
               response = session.executeBindingQuery(request.getAddress());
               break;
            }
            case SESS_CREATEBROWSER:
            {
               SessionCreateBrowserMessage request = (SessionCreateBrowserMessage)packet;
               session.createBrowser(request.getQueueName(), request.getFilterString());
               response = new NullResponseMessage();
               break;
            }
            case SESS_CREATEPRODUCER:
            {
               SessionCreateProducerMessage request = (SessionCreateProducerMessage)packet;
               response = session.createProducer(request.getAddress(),
                                                 request.getWindowSize(),
                                                 request.getMaxRate(),
                                                 request.isAutoGroupId());
               break;
            }
            case SESS_ACKNOWLEDGE:
            {
               SessionAcknowledgeMessage message = (SessionAcknowledgeMessage)packet;
               session.acknowledge(message.getConsumerID(), message.getMessageID());
               if (message.isRequiresResponse())
               {
                  response = new NullResponseMessage();
               }
               break;
            }
            case SESS_COMMIT:
            {
               session.commit();
               response = new NullResponseMessage();
               break;
            }
            case SESS_ROLLBACK:
            {
               session.rollback();
               response = new NullResponseMessage();
               break;
            }
            case SESS_XA_COMMIT:
            {
               SessionXACommitMessage message = (SessionXACommitMessage)packet;
               response = session.XACommit(message.isOnePhase(), message.getXid());
               break;
            }
            case SESS_XA_END:
            {
               SessionXAEndMessage message = (SessionXAEndMessage)packet;
               response = session.XAEnd(message.getXid(), message.isFailed());
               break;
            }
            case SESS_XA_FORGET:
            {
               SessionXAForgetMessage message = (SessionXAForgetMessage)packet;
               response = session.XAForget(message.getXid());
               break;
            }
            case SESS_XA_JOIN:
            {
               SessionXAJoinMessage message = (SessionXAJoinMessage)packet;
               response = session.XAJoin(message.getXid());
               break;
            }
            case SESS_XA_RESUME:
            {
               SessionXAResumeMessage message = (SessionXAResumeMessage)packet;
               response = session.XAResume(message.getXid());
               break;
            }
            case SESS_XA_ROLLBACK:
            {
               SessionXARollbackMessage message = (SessionXARollbackMessage)packet;              
               response = session.XARollback(message.getXid());
               break;
            }
            case SESS_XA_START:
            {
               SessionXAStartMessage message = (SessionXAStartMessage)packet;
               response = session.XAStart(message.getXid());
               break;
            }
            case SESS_XA_SUSPEND:
            {
               response = session.XASuspend();
               break;
            }
            case SESS_XA_PREPARE:
            {
               SessionXAPrepareMessage message = (SessionXAPrepareMessage)packet;
               response = session.XAPrepare(message.getXid());
               break;
            }
            case SESS_XA_INDOUBT_XIDS:
            {
               List<Xid> xids = session.getInDoubtXids();
               response = new SessionXAGetInDoubtXidsResponseMessage(xids);
               break;
            }
            case SESS_XA_GET_TIMEOUT:
            {
               response = new SessionXAGetTimeoutResponseMessage(session.getXATimeout());
               break;
            }
            case SESS_XA_SET_TIMEOUT:
            {
               SessionXASetTimeoutMessage message = (SessionXASetTimeoutMessage)packet;
               response = new SessionXASetTimeoutResponseMessage(session.setXATimeout(message.getTimeoutSeconds()));
               break;
            }
            case SESS_ADD_DESTINATION:
            {
               SessionAddDestinationMessage message = (SessionAddDestinationMessage)packet;
               session.addDestination(message.getAddress(), message.isDurable(), message.isTemporary());
               response = new NullResponseMessage();
               break;
            }
            case SESS_REMOVE_DESTINATION:
            {
               SessionRemoveDestinationMessage message = (SessionRemoveDestinationMessage)packet;
               session.removeDestination(message.getAddress(), message.isDurable());
               response = new NullResponseMessage();
               break;
            }
            case SESS_START:
            {
               session.setStarted(true);
               break;
            }
            case SESS_FAILOVER_COMPLETE:
            {
               session.failedOver();
               break;
            }
            case SESS_STOP:
            {
               session.setStarted(false);
               response = new NullResponseMessage();
               break;
            }
            case SESS_CLOSE:
            {
               session.close();
               response = new NullResponseMessage();
               break;
            }
            case SESS_CONSUMER_CLOSE:
            {
               SessionConsumerCloseMessage message = (SessionConsumerCloseMessage)packet;
               session.closeConsumer(message.getConsumerID());
               response = new NullResponseMessage();
               break;
            }
            case SESS_PRODUCER_CLOSE:
            {
               SessionProducerCloseMessage message = (SessionProducerCloseMessage)packet;
               session.closeProducer(message.getProducerID());
               response = new NullResponseMessage();
               break;
            }
            case SESS_BROWSER_CLOSE:
            {
               SessionBrowserCloseMessage message = (SessionBrowserCloseMessage)packet;
               session.closeBrowser(message.getBrowserID());
               response = new NullResponseMessage();
               break;
            }
            case SESS_FLOWTOKEN:
            {
               SessionConsumerFlowCreditMessage message = (SessionConsumerFlowCreditMessage)packet;
               session.receiveConsumerCredits(message.getConsumerID(), message.getCredits());
               break;
            }
            case SESS_SEND:
            {
               SessionSendMessage message = (SessionSendMessage)packet;
               session.sendProducerMessage(message.getProducerID(), message.getServerMessage());
               if (message.isRequiresResponse())
               {
                  response = new NullResponseMessage();
               }
               break;
            }
            case SESS_SCHEDULED_SEND:
            {
               SessionScheduledSendMessage message = (SessionScheduledSendMessage)packet;
               session.sendScheduledProducerMessage(message.getProducerID(),
                                                    message.getServerMessage(),
                                                    message.getScheduledDeliveryTime());
               if (message.isRequiresResponse())
               {
                  response = new NullResponseMessage();
               }
               break;
            }
            case SESS_BROWSER_HASNEXTMESSAGE:
            {
               SessionBrowserHasNextMessageMessage message = (SessionBrowserHasNextMessageMessage)packet;
               response = new SessionBrowserHasNextMessageResponseMessage(session.browserHasNextMessage(message.getBrowserID()));
               break;
            }
            case SESS_BROWSER_NEXTMESSAGE:
            {
               SessionBrowserNextMessageMessage message = (SessionBrowserNextMessageMessage)packet;
               ServerMessage smsg = session.browserNextMessage(message.getBrowserID());
               response = new SessionBrowseMessage(smsg);
               break;
            }
            case SESS_BROWSER_RESET:
            {
               SessionBrowserResetMessage message = (SessionBrowserResetMessage)packet;
               session.browserReset(message.getBrowserID());
               response = new NullResponseMessage();
               break;
            }
            case SESS_MANAGEMENT_SEND:
            {
               SessionSendManagementMessage message = (SessionSendManagementMessage)packet;
               session.handleManagementMessage(message);
               break;
            }
            case PacketImpl.SESS_REPLICATE_DELIVERY:
            {
               SessionReplicateDeliveryMessage message = (SessionReplicateDeliveryMessage)packet;
               session.handleReplicatedDelivery(message.getConsumerID(), message.getMessageID());
               break;
            }
            default:
            {
               response = new MessagingExceptionMessage(new MessagingException(MessagingException.UNSUPPORTED_PACKET,
                                                                               "Unsupported packet " + type));
            }
         }
      }
      catch (Throwable t)
      {
         MessagingException me;

         log.error("Caught unexpected exception", t);

         if (t instanceof MessagingException)
         {
            me = (MessagingException)t;
         }
         else
         {
            me = new MessagingException(MessagingException.INTERNAL_ERROR);
         }

         response = new MessagingExceptionMessage(me);
      }

      if (response != null)
      {
         final boolean closeChannel = type == SESS_CLOSE;
         
         if (result == null)
         {
            // Not clustered - just send now
            channel.send(response);              
            
            if (closeChannel)
            {
               channel.close();
            }
         }
         else
         {
            final Packet theResponse = response;
            
            result.setResultRunner(new Runnable()
            {
               public void run()
               {
                  channel.send(theResponse);
                  
                  if (closeChannel)
                  {
                     channel.close();
                  }
               }
            });
         }
      }

      channel.replicateComplete();
   }
}
