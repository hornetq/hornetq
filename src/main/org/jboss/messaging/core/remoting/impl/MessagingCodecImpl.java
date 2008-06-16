/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.MessagingCodec;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.*;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.*;
import static org.jboss.messaging.util.DataConstants.SIZE_INT;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * A MessagingCodec
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 */
public class MessagingCodecImpl implements MessagingCodec
{
   private static final Logger log = Logger.getLogger(MessagingCodecImpl.class);

   // MessagingCodec implementation ------------------------------------------

   public void encode(final MessagingBuffer buffer, Object message) throws Exception
   {
      Packet packet = (Packet) message;
      packet.encode(buffer);
   }


   public Packet decode(final MessagingBuffer in) throws Exception
   {
      int start = in.position();

      if (in.remaining() <= SIZE_INT)
      {
         return null;
      }

      int length = in.getInt();

      if (in.remaining() < length)
      {
         in.position(start);

         return null;
      }

      byte packetType = in.getByte();

      Packet packet;


      switch (packetType)
      {
         case NULL:
         {
            packet = new PacketImpl(PacketImpl.NULL);
            break;
         }
         case PING:
         {
            packet = new Ping();
            break;
         }
         case PONG:
         {
            packet = new Pong();
            break;
         }
         case EXCEPTION:
         {
            packet = new MessagingExceptionMessage();
            break;
         }
         case CLOSE:
         {
            packet = new PacketImpl(PacketImpl.CLOSE);
            break;
         }
         case CREATECONNECTION:
         {
            packet = new CreateConnectionRequest();
            break;
         }
         case CREATECONNECTION_RESP:
         {
            packet = new CreateConnectionResponse();
            break;
         }
         case PacketImpl.CONN_CREATESESSION:
         {
            packet = new ConnectionCreateSessionMessage();
            break;
         }
         case PacketImpl.CONN_CREATESESSION_RESP:
         {
            packet = new ConnectionCreateSessionResponseMessage();
            break;
         }
         case PacketImpl.CONN_START:
         {
            packet = new PacketImpl(PacketImpl.CONN_START);
            break;
         }
         case PacketImpl.CONN_STOP:
         {
            packet = new PacketImpl(PacketImpl.CONN_STOP);
            break;
         }
         case PacketImpl.SESS_CREATECONSUMER:
         {
            packet = new SessionCreateConsumerMessage();
            break;
         }
         case PacketImpl.SESS_CREATECONSUMER_RESP:
         {
            packet = new SessionCreateConsumerResponseMessage();
            break;
         }
         case PacketImpl.SESS_CREATEPRODUCER:
         {
            packet = new SessionCreateProducerMessage();
            break;
         }
         case PacketImpl.SESS_CREATEPRODUCER_RESP:
         {
            packet = new SessionCreateProducerResponseMessage();
            break;
         }
         case PacketImpl.SESS_CREATEBROWSER:
         {
            packet = new SessionCreateBrowserMessage();
            break;
         }
         case PacketImpl.SESS_CREATEBROWSER_RESP:
         {
            packet = new SessionCreateBrowserResponseMessage();
            break;
         }
         case PacketImpl.SESS_ACKNOWLEDGE:
         {
            packet = new SessionAcknowledgeMessage();
            break;
         }
         case PacketImpl.SESS_RECOVER:
         {
            packet = new PacketImpl(PacketImpl.SESS_RECOVER);
            break;
         }
         case PacketImpl.SESS_COMMIT:
         {
            packet = new PacketImpl(PacketImpl.SESS_COMMIT);
            break;
         }
         case PacketImpl.SESS_ROLLBACK:
         {
            packet = new PacketImpl(PacketImpl.SESS_ROLLBACK);
            break;
         }
         case PacketImpl.SESS_CANCEL:
         {
            packet = new SessionCancelMessage();
            break;
         }
         case PacketImpl.SESS_QUEUEQUERY:
         {
            packet = new SessionQueueQueryMessage();
            break;
         }
         case PacketImpl.SESS_QUEUEQUERY_RESP:
         {
            packet = new SessionQueueQueryResponseMessage();
            break;
         }
         case PacketImpl.SESS_CREATEQUEUE:
         {
            packet = new SessionCreateQueueMessage();
            break;
         }
         case PacketImpl.SESS_DELETE_QUEUE:
         {
            packet = new SessionDeleteQueueMessage();
            break;
         }
         case PacketImpl.SESS_ADD_DESTINATION:
         {
            packet = new SessionAddDestinationMessage();
            break;
         }
         case PacketImpl.SESS_REMOVE_DESTINATION:
         {
            packet = new SessionRemoveDestinationMessage();
            break;
         }
         case PacketImpl.SESS_BINDINGQUERY:
         {
            packet = new SessionBindingQueryMessage();
            break;
         }
         case PacketImpl.SESS_BINDINGQUERY_RESP:
         {
            packet = new SessionBindingQueryResponseMessage();
            break;
         }
         case PacketImpl.SESS_BROWSER_RESET:
         {
            packet = new PacketImpl(PacketImpl.SESS_BROWSER_RESET);
            break;
         }
         case PacketImpl.SESS_BROWSER_HASNEXTMESSAGE:
         {
            packet = new PacketImpl(PacketImpl.SESS_BROWSER_HASNEXTMESSAGE);
            break;
         }
         case PacketImpl.SESS_BROWSER_HASNEXTMESSAGE_RESP:
         {
            packet = new SessionBrowserHasNextMessageResponseMessage();
            break;
         }
         case PacketImpl.SESS_BROWSER_NEXTMESSAGE:
         {
            packet = new PacketImpl(PacketImpl.SESS_BROWSER_NEXTMESSAGE);
            break;
         }
         case PacketImpl.SESS_XA_START:
         {
            packet = new SessionXAStartMessage();
            break;
         }
         case PacketImpl.SESS_XA_END:
         {
            packet = new SessionXAEndMessage();
            break;
         }
         case PacketImpl.SESS_XA_COMMIT:
         {
            packet = new SessionXACommitMessage();
            break;
         }
         case PacketImpl.SESS_XA_PREPARE:
         {
            packet = new SessionXAPrepareMessage();
            break;
         }
         case PacketImpl.SESS_XA_RESP:
         {
            packet = new SessionXAResponseMessage();
            break;
         }
         case PacketImpl.SESS_XA_ROLLBACK:
         {
            packet = new SessionXARollbackMessage();
            break;
         }
         case PacketImpl.SESS_XA_JOIN:
         {
            packet = new SessionXAJoinMessage();
            break;
         }
         case PacketImpl.SESS_XA_SUSPEND:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_SUSPEND);
            break;
         }
         case PacketImpl.SESS_XA_RESUME:
         {
            packet = new SessionXAResumeMessage();
            break;
         }
         case PacketImpl.SESS_XA_FORGET:
         {
            packet = new SessionXAForgetMessage();
            break;
         }
         case PacketImpl.SESS_XA_INDOUBT_XIDS:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_INDOUBT_XIDS);
            break;
         }
         case PacketImpl.SESS_XA_INDOUBT_XIDS_RESP:
         {
            packet = new SessionXAGetInDoubtXidsResponseMessage();
            break;
         }
         case PacketImpl.SESS_XA_SET_TIMEOUT:
         {
            packet = new SessionXASetTimeoutMessage();
            break;
         }
         case PacketImpl.SESS_XA_SET_TIMEOUT_RESP:
         {
            packet = new SessionXASetTimeoutResponseMessage();
            break;
         }
         case PacketImpl.SESS_XA_GET_TIMEOUT:
         {
            packet = new PacketImpl(PacketImpl.SESS_XA_GET_TIMEOUT);
            break;
         }
         case PacketImpl.SESS_XA_GET_TIMEOUT_RESP:
         {
            packet = new SessionXAGetTimeoutResponseMessage();
            break;
         }
         case PacketImpl.CONS_FLOWTOKEN:
         {
            packet = new ConsumerFlowCreditMessage();
            break;
         }
         case PacketImpl.PROD_SEND:
         {
            packet = new ProducerSendMessage();
            break;
         }
         case PacketImpl.PROD_RECEIVETOKENS:
         {
            packet = new ProducerFlowCreditMessage();
            break;
         }
         case PacketImpl.RECEIVE_MSG:
         {
            packet = new ReceiveMessage();
            break;
         }
         default:
         {
            throw new IllegalArgumentException("Invalid type: " + packetType);
         }
      }

      packet.decode(in);

      return packet;

   }

}
