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

import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.CommandManager;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
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
import org.jboss.messaging.core.server.ServerSession;

/**
 *
 * A ServerSessionPacketHandler
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerSessionPacketHandler implements PacketHandler
{
	private static final Logger log = Logger.getLogger(ServerSessionPacketHandler.class);

	private final ServerSession session;

	private final CommandManager commandManager;

	public ServerSessionPacketHandler(final ServerSession session,
	                                  final CommandManager commandManager)
   {
		this.session = session;

		this.commandManager = commandManager;
   }

   public long getID()
   {
      return session.getID();
   }

   public void handle(final Object remotingConnectionID, final Packet packet)
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
               
               response = session.createConsumer(request.getClientTargetID(), request.getQueueName(), request.getFilterString(),            		                            
               		                            request.getWindowSize(), request.getMaxRate());
               break;
            }
            case PacketImpl.SESS_CREATEQUEUE:
            {
               SessionCreateQueueMessage request = (SessionCreateQueueMessage) packet;
               session.createQueue(request.getAddress(), request.getQueueName(), request
                     .getFilterString(), request.isDurable(), request.isTemporary());
               response = new PacketImpl(PacketImpl.NULL);
               break;
            }
            case PacketImpl.SESS_DELETE_QUEUE:
            {
               SessionDeleteQueueMessage request = (SessionDeleteQueueMessage) packet;
               session.deleteQueue(request.getQueueName());
               response = new PacketImpl(PacketImpl.NULL);
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
               response = session.createBrowser(request.getQueueName(), request
                     .getFilterString());
               break;
            }
            case PacketImpl.SESS_CREATEPRODUCER:
            {
               SessionCreateProducerMessage request = (SessionCreateProducerMessage) packet;
               response = session.createProducer(request.getClientTargetID(), request.getAddress(), request.getWindowSize(), request.getMaxRate());
               break;
            }
            case PacketImpl.CLOSE:
            {
               session.close();
               response = new PacketImpl(PacketImpl.NULL);
               break;
            }
            case PacketImpl.SESS_ACKNOWLEDGE:
            {
               SessionAcknowledgeMessage message = (SessionAcknowledgeMessage) packet;
               session.acknowledge(message.getDeliveryID(), message.isAllUpTo());
               if (packet.getResponseTargetID() != PacketImpl.NO_ID_SET)
               {
                  response = new PacketImpl(PacketImpl.NULL);
               }
               break;
            }
            case PacketImpl.SESS_COMMIT:
               session.commit();
               response = new PacketImpl(PacketImpl.NULL);
               break;
            case PacketImpl.SESS_ROLLBACK:
               session.rollback();
               response = new PacketImpl(PacketImpl.NULL);
               break;
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
               response = new PacketImpl(PacketImpl.NULL);
               break;
            }
            case PacketImpl.SESS_REMOVE_DESTINATION:
            {
               SessionRemoveDestinationMessage message = (SessionRemoveDestinationMessage) packet;
               session.removeDestination(message.getAddress(), message.isDurable());
               response = new PacketImpl(PacketImpl.NULL);
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
               response = new PacketImpl(PacketImpl.NULL);
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
         
         if (packet.getResponseTargetID() != PacketImpl.NO_ID_SET)
         {
            response = new MessagingExceptionMessage(me);
         }  
      }

      if (response != null)
      {
         commandManager.sendCommandOneway(packet.getResponseTargetID(), response);   
      }
      
      commandManager.packetProcessed(packet);
   }
}
