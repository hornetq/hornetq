/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.server.endpoint;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_ACKNOWLEDGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BINDINGQUERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CANCEL;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_COMMIT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATEPRODUCER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATEQUEUE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_DELETE_QUEUE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_QUEUEQUERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_ROLLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_COMMIT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_END;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_FORGET;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_GET_TIMEOUT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_INDOUBT_XIDS;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_JOIN;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_PREPARE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_RESUME;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_ROLLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_SET_TIMEOUT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_START;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_SUSPEND;

import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionAddAddressMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRemoveAddressMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAStartMessage;
import org.jboss.messaging.util.MessagingException;

/**
 * 
 * A ServerSessionPacketHandler
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerSessionPacketHandler extends ServerPacketHandlerSupport
{
	private final ServerSession session;
	
	private final int prefetchSize;
	
	public ServerSessionPacketHandler(final ServerSession session, final int prefetchSize)
   {
		this.session = session;
		
		this.prefetchSize = prefetchSize;
   }

   public String getID()
   {
      return session.getID();
   }

   public Packet doHandle(final Packet packet, final PacketSender sender) throws Exception
   {
      Packet response = null;

      PacketType type = packet.getType();

      // TODO use a switch for this
      if (type == SESS_CREATECONSUMER)
      {
         SessionCreateConsumerMessage request = (SessionCreateConsumerMessage) packet;

         response = session.createConsumer(request.getQueueName(), request
               .getFilterString(), request.isNoLocal(), request.isAutoDeleteQueue(), prefetchSize);
      }
      else if (type == SESS_CREATEQUEUE)
      {
         SessionCreateQueueMessage request = (SessionCreateQueueMessage) packet;

         session.createQueue(request.getAddress(), request.getQueueName(), request
               .getFilterString(), request.isDurable(), request
               .isTemporary());
      }
      else if (type == SESS_DELETE_QUEUE)
      {
         SessionDeleteQueueMessage request = (SessionDeleteQueueMessage) packet;

         session.deleteQueue(request.getQueueName());
      }
      else if (type == SESS_QUEUEQUERY)
      {
         SessionQueueQueryMessage request = (SessionQueueQueryMessage) packet;

         response = session.executeQueueQuery(request);
      }
      else if (type == SESS_BINDINGQUERY)
      {
         SessionBindingQueryMessage request = (SessionBindingQueryMessage)packet;

         response = session.executeBindingQuery(request);
      }
      else if (type == SESS_CREATEBROWSER)
      {
         SessionCreateBrowserMessage request = (SessionCreateBrowserMessage) packet;

         response = session.createBrowser(request.getQueueName(), request
               .getFilterString());
      }
      else if (type == SESS_CREATEPRODUCER)
      {
         SessionCreateProducerMessage request = (SessionCreateProducerMessage) packet;

         response = session.createProducer(request.getAddress());
      }
      else if (type == CLOSE)
      {
      	session.close();
      }
      else if (type == SESS_ACKNOWLEDGE)
      {
         SessionAcknowledgeMessage message = (SessionAcknowledgeMessage) packet;

         session.acknowledge(message.getDeliveryID(), message.isAllUpTo());
      }
      else if (type == SESS_COMMIT)
      {
      	session.commit();
      }
      else if (type == SESS_ROLLBACK)
      {
      	session.rollback();
      }
      else if (type == SESS_CANCEL)
      {
         SessionCancelMessage message = (SessionCancelMessage) packet;

         session.cancel(message.getDeliveryID(), message.isExpired());
      }
      else if (type == SESS_XA_COMMIT)
      {
         SessionXACommitMessage message = (SessionXACommitMessage) packet;

         response = session.XACommit(message.isOnePhase(), message.getXid());
      }
      else if (type == SESS_XA_END)
      {
         SessionXAEndMessage message = (SessionXAEndMessage) packet;

         response = session.XAEnd(message.getXid(), message.isFailed());
      }
      else if (type == SESS_XA_FORGET)
      {
         SessionXAForgetMessage message = (SessionXAForgetMessage) packet;

         response = session.XAForget(message.getXid());
      }
      else if (type == SESS_XA_JOIN)
      {
         SessionXAJoinMessage message = (SessionXAJoinMessage) packet;

         response = session.XAJoin(message.getXid());
      }
      else if (type == SESS_XA_RESUME)
      {
         SessionXAResumeMessage message = (SessionXAResumeMessage) packet;

         response = session.XAResume(message.getXid());
      }
      else if (type == SESS_XA_ROLLBACK)
      {
         SessionXARollbackMessage message = (SessionXARollbackMessage) packet;

         response = session.XARollback(message.getXid());
      }
      else if (type == SESS_XA_START)
      {
         SessionXAStartMessage message = (SessionXAStartMessage) packet;

         response = session.XAStart(message.getXid());
      }
      else if (type == SESS_XA_SUSPEND)
      {
         response = session.XASuspend();
      }
      else if (type == SESS_XA_PREPARE)
      {
         SessionXAPrepareMessage message = (SessionXAPrepareMessage) packet;

         response = session.XAPrepare(message.getXid());
      }
      else if (type == SESS_XA_INDOUBT_XIDS)
      {
         List<Xid> xids = session.getInDoubtXids();

         response = new SessionXAGetInDoubtXidsResponseMessage(xids);
      }
      else if (type == SESS_XA_GET_TIMEOUT)
      {
         response = new SessionXAGetTimeoutResponseMessage(session.getXATimeout());
      }
      else if (type == SESS_XA_SET_TIMEOUT)
      {
         SessionXASetTimeoutMessage message = (SessionXASetTimeoutMessage) packet;

         response = new SessionXASetTimeoutResponseMessage(session.setXATimeout(message
               .getTimeoutSeconds()));
      }
      else if (type == PacketType.SESS_ADD_ADDRESS)
      {
         SessionAddAddressMessage message = (SessionAddAddressMessage) packet;

         session.addAddress(message.getAddress());
      }
      else if (type == PacketType.SESS_REMOVE_ADDRESS)
      {
         SessionRemoveAddressMessage message = (SessionRemoveAddressMessage) packet;

         session.removeAddress(message.getAddress());
      }
      else
      {
         throw new MessagingException(MessagingException.UNSUPPORTED_PACKET, "Unsupported packet " + type);
      }

      // reply if necessary
      if (response == null && packet.isOneWay() == false)
      {
         response = new NullPacket();
      }

      return response;
   }

   @Override
   public String toString()
   {
      return "ServerSessionPacketHandler[id=" + session.getID() + "]";
   }
}
