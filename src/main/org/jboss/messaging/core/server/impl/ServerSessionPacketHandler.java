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

package org.jboss.messaging.core.server.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_BROWSER_RESET;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_REPLICATE_DELIVERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_REPLICATE_DELIVERY_RESP;

import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.BrowseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.SendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserResetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionNullResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionProducerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReplicateDeliveryMessage;
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
 *
 * A ServerSessionPacketHandler
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerSessionPacketHandler implements ChannelHandler
{
	private static final Logger log = Logger.getLogger(ServerSessionPacketHandler.class);

	private final ServerSession session;
	
	private final Channel channel;

	public ServerSessionPacketHandler(final ServerSession session,
	                                  final Channel channel)
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
      Packet response = null;

      byte type = packet.getType();
      
      try
      {
         switch (type)
         {
            case PacketImpl.SESS_CREATECONSUMER:
            {
               SessionCreateConsumerMessage request = (SessionCreateConsumerMessage) packet;
               
               response = session.createConsumer(request.getQueueName(), request.getFilterString(),            		                            
               		                            request.getWindowSize(), request.getMaxRate());
               break;
            }
            case PacketImpl.SESS_CREATEQUEUE:
            {
               SessionCreateQueueMessage request = (SessionCreateQueueMessage) packet;
               session.createQueue(request.getAddress(), request.getQueueName(), request
                     .getFilterString(), request.isDurable(), request.isTemporary());
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_DELETE_QUEUE:
            {
               SessionDeleteQueueMessage request = (SessionDeleteQueueMessage) packet;
               session.deleteQueue(request.getQueueName());
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_QUEUEQUERY:
            {
               SessionQueueQueryMessage request = (SessionQueueQueryMessage) packet;
               response = session.executeQueueQuery(request.getQueueName());
               break;
            }
            case PacketImpl.SESS_BINDINGQUERY:
            {
               SessionBindingQueryMessage request = (SessionBindingQueryMessage)packet;
               response = session.executeBindingQuery(request.getAddress());
               break;
            }
            case PacketImpl.SESS_CREATEBROWSER:
            {
               SessionCreateBrowserMessage request = (SessionCreateBrowserMessage) packet;
               session.createBrowser(request.getQueueName(), request.getFilterString());
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_CREATEPRODUCER:
            {
               SessionCreateProducerMessage request = (SessionCreateProducerMessage) packet;
               response = session.createProducer(request.getAddress(), request.getWindowSize(), request.getMaxRate());
               break;
            }
            case PacketImpl.SESS_CLOSE:
            {;
               session.close();
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_ACKNOWLEDGE:
            {
               SessionAcknowledgeMessage message = (SessionAcknowledgeMessage) packet;
               session.acknowledge(message.getDeliveryID(), message.isAllUpTo());
               if (message.isRequiresResponse())
               {
                  response = new SessionNullResponseMessage();
               }
               break;
            }
            case PacketImpl.SESS_COMMIT:
            {
               session.commit();
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_ROLLBACK:
            {
               session.rollback();
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_CANCEL:
            {
               SessionCancelMessage message = (SessionCancelMessage) packet;
               session.cancel(message.getDeliveryID(), message.isExpired());
               //one way
               break;
            }
            case PacketImpl.SESS_XA_COMMIT:
            {
               SessionXACommitMessage message = (SessionXACommitMessage) packet;
               response = session.XACommit(message.isOnePhase(), message.getXid());
               break;
            }
            case PacketImpl.SESS_XA_END:
            {
               SessionXAEndMessage message = (SessionXAEndMessage) packet;
               response = session.XAEnd(message.getXid(), message.isFailed());
               break;
            }
            case PacketImpl.SESS_XA_FORGET:
            {
               SessionXAForgetMessage message = (SessionXAForgetMessage) packet;
               response = session.XAForget(message.getXid());
               break;
            }
            case PacketImpl.SESS_XA_JOIN:
            {
               SessionXAJoinMessage message = (SessionXAJoinMessage) packet;
               response = session.XAJoin(message.getXid());
               break;
            }
            case PacketImpl.SESS_XA_RESUME:
            {
               SessionXAResumeMessage message = (SessionXAResumeMessage) packet;
               response = session.XAResume(message.getXid());
               break;
            }
            case PacketImpl.SESS_XA_ROLLBACK:
            {
               SessionXARollbackMessage message = (SessionXARollbackMessage) packet;
               response = session.XARollback(message.getXid());
               break;
            }
            case PacketImpl.SESS_XA_START:
            {
               SessionXAStartMessage message = (SessionXAStartMessage) packet;
               response = session.XAStart(message.getXid());
               break;
            }
            case PacketImpl.SESS_XA_SUSPEND:
            {
               response = session.XASuspend();
               break;
            }
            case PacketImpl.SESS_XA_PREPARE:
            {
               SessionXAPrepareMessage message = (SessionXAPrepareMessage) packet;
               response = session.XAPrepare(message.getXid());
               break;
            }
            case PacketImpl.SESS_XA_INDOUBT_XIDS:
            {
               List<Xid> xids = session.getInDoubtXids();
               response = new SessionXAGetInDoubtXidsResponseMessage(xids);
               break;
            }
            case PacketImpl.SESS_XA_GET_TIMEOUT:
            {
               response = new SessionXAGetTimeoutResponseMessage(session.getXATimeout());
               break;
            }
            case PacketImpl.SESS_XA_SET_TIMEOUT:
            {
               SessionXASetTimeoutMessage message = (SessionXASetTimeoutMessage) packet;
               response = new SessionXASetTimeoutResponseMessage(session.setXATimeout(message
                     .getTimeoutSeconds()));
               break;
            }
            case PacketImpl.SESS_ADD_DESTINATION:
            {
               SessionAddDestinationMessage message = (SessionAddDestinationMessage) packet;
               session.addDestination(message.getAddress(), message.isDurable(), message.isTemporary());
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_REMOVE_DESTINATION:
            {
               SessionRemoveDestinationMessage message = (SessionRemoveDestinationMessage) packet;
               session.removeDestination(message.getAddress(), message.isDurable());
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_START:
            {            
               session.setStarted(true);           
               break;
            }
            case PacketImpl.SESS_STOP:
            {            
               session.setStarted(false);       
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_CONSUMER_CLOSE:
            { 
               SessionConsumerCloseMessage message = (SessionConsumerCloseMessage)packet;
               session.closeConsumer(message.getConsumerID());
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_PRODUCER_CLOSE:
            { 
               SessionProducerCloseMessage message = (SessionProducerCloseMessage)packet;
               session.closeProducer(message.getProducerID());
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_BROWSER_CLOSE:
            { 
               SessionBrowserCloseMessage message = (SessionBrowserCloseMessage)packet;
               session.closeBrowser(message.getBrowserID());
               response = new SessionNullResponseMessage();
               break;
            }
            case PacketImpl.SESS_FLOWTOKEN:
            {
               SessionFlowCreditMessage message = (SessionFlowCreditMessage)packet;
               session.receiveConsumerCredits(message.getConsumerID(), message.getCredits());
               break;
            }
            case PacketImpl.SESS_SEND:
            {
               SendMessage message = (SendMessage) packet;
               session.sendProducerMessage(message.getProducerID(), message.getServerMessage());               
               if (message.isRequiresResponse())
               {
                  response = new SessionNullResponseMessage();
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
               response = new BrowseMessage(smsg);
               break;
            }
            case SESS_BROWSER_RESET:
            {
               SessionBrowserResetMessage message = (SessionBrowserResetMessage)packet;
               session.browserReset(message.getBrowserID());
               response = new SessionNullResponseMessage();
               break;
            }
            case SESS_REPLICATE_DELIVERY:
            {
               SessionReplicateDeliveryMessage message = (SessionReplicateDeliveryMessage)packet;
               session.handleReplicateDelivery(message.getMessageID(), message.getConsumerID());
               break;
            }
            case SESS_REPLICATE_DELIVERY_RESP:
            {
               session.handleDeferredDelivery();
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
         channel.send(response);   
      }
   }
}
