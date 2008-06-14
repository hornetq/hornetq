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
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.*;
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
            packet = new EmptyPacket(EmptyPacket.NULL);
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
            packet = new EmptyPacket(EmptyPacket.CLOSE);
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
         case EmptyPacket.CONN_CREATESESSION:
         {
            packet = new ConnectionCreateSessionMessage();
            break;
         }
         case EmptyPacket.CONN_CREATESESSION_RESP:
         {
            packet = new ConnectionCreateSessionResponseMessage();
            break;
         }
         case EmptyPacket.CONN_START:
         {
            packet = new EmptyPacket(EmptyPacket.CONN_START);
            break;
         }
         case EmptyPacket.CONN_STOP:
         {
            packet = new EmptyPacket(EmptyPacket.CONN_STOP);
            break;
         }
         case EmptyPacket.SESS_CREATECONSUMER:
         {
            packet = new SessionCreateConsumerMessage();
            break;
         }
         case EmptyPacket.SESS_CREATECONSUMER_RESP:
         {
            packet = new SessionCreateConsumerResponseMessage();
            break;
         }
         case EmptyPacket.SESS_CREATEPRODUCER:
         {
            packet = new SessionCreateProducerMessage();
            break;
         }
         case EmptyPacket.SESS_CREATEPRODUCER_RESP:
         {
            packet = new SessionCreateProducerResponseMessage();
            break;
         }
         case EmptyPacket.SESS_CREATEBROWSER:
         {
            packet = new SessionCreateBrowserMessage();
            break;
         }
         case EmptyPacket.SESS_CREATEBROWSER_RESP:
         {
            packet = new SessionCreateBrowserResponseMessage();
            break;
         }
         case EmptyPacket.SESS_ACKNOWLEDGE:
         {
            packet = new SessionAcknowledgeMessage();
            break;
         }
         case EmptyPacket.SESS_RECOVER:
         {
            packet = new EmptyPacket(EmptyPacket.SESS_RECOVER);
            break;
         }
         case EmptyPacket.SESS_COMMIT:
         {
            packet = new EmptyPacket(EmptyPacket.SESS_COMMIT);
            break;
         }
         case EmptyPacket.SESS_ROLLBACK:
         {
            packet = new EmptyPacket(EmptyPacket.SESS_ROLLBACK);
            break;
         }
         case EmptyPacket.SESS_CANCEL:
         {
            packet = new SessionCancelMessage();
            break;
         }
         case EmptyPacket.SESS_QUEUEQUERY:
         {
            packet = new SessionQueueQueryMessage();
            break;
         }
         case EmptyPacket.SESS_QUEUEQUERY_RESP:
         {
            packet = new SessionQueueQueryResponseMessage();
            break;
         }
         case EmptyPacket.SESS_CREATEQUEUE:
         {
            packet = new SessionCreateQueueMessage();
            break;
         }
         case EmptyPacket.SESS_DELETE_QUEUE:
         {
            packet = new SessionDeleteQueueMessage();
            break;
         }
         case EmptyPacket.SESS_ADD_DESTINATION:
         {
            packet = new SessionAddDestinationMessage();
            break;
         }
         case EmptyPacket.SESS_REMOVE_DESTINATION:
         {
            packet = new SessionRemoveDestinationMessage();
            break;
         }
         case EmptyPacket.SESS_BINDINGQUERY:
         {
            packet = new SessionBindingQueryMessage();
            break;
         }
         case EmptyPacket.SESS_BINDINGQUERY_RESP:
         {
            packet = new SessionBindingQueryResponseMessage();
            break;
         }
         case EmptyPacket.SESS_BROWSER_RESET:
         {
            packet = new EmptyPacket(EmptyPacket.SESS_BROWSER_RESET);
            break;
         }
         case EmptyPacket.SESS_BROWSER_HASNEXTMESSAGE:
         {
            packet = new EmptyPacket(EmptyPacket.SESS_BROWSER_HASNEXTMESSAGE);
            break;
         }
         case EmptyPacket.SESS_BROWSER_HASNEXTMESSAGE_RESP:
         {
            packet = new SessionBrowserHasNextMessageResponseMessage();
            break;
         }
         case EmptyPacket.SESS_BROWSER_NEXTMESSAGE:
         {
            packet = new EmptyPacket(EmptyPacket.SESS_BROWSER_NEXTMESSAGE);
            break;
         }
         case EmptyPacket.SESS_XA_START:
         {
            packet = new SessionXAStartMessage();
            break;
         }
         case EmptyPacket.SESS_XA_END:
         {
            packet = new SessionXAEndMessage();
            break;
         }
         case EmptyPacket.SESS_XA_COMMIT:
         {
            packet = new SessionXACommitMessage();
            break;
         }
         case EmptyPacket.SESS_XA_PREPARE:
         {
            packet = new SessionXAPrepareMessage();
            break;
         }
         case EmptyPacket.SESS_XA_RESP:
         {
            packet = new SessionXAResponseMessage();
            break;
         }
         case EmptyPacket.SESS_XA_ROLLBACK:
         {
            packet = new SessionXARollbackMessage();
            break;
         }
         case EmptyPacket.SESS_XA_JOIN:
         {
            packet = new SessionXAJoinMessage();
            break;
         }
         case EmptyPacket.SESS_XA_SUSPEND:
         {
            packet = new EmptyPacket(EmptyPacket.SESS_XA_SUSPEND);
            break;
         }
         case EmptyPacket.SESS_XA_RESUME:
         {
            packet = new SessionXAResumeMessage();
            break;
         }
         case EmptyPacket.SESS_XA_FORGET:
         {
            packet = new SessionXAForgetMessage();
            break;
         }
         case EmptyPacket.SESS_XA_INDOUBT_XIDS:
         {
            packet = new EmptyPacket(EmptyPacket.SESS_XA_INDOUBT_XIDS);
            break;
         }
         case EmptyPacket.SESS_XA_INDOUBT_XIDS_RESP:
         {
            packet = new SessionXAGetInDoubtXidsResponseMessage();
            break;
         }
         case EmptyPacket.SESS_XA_SET_TIMEOUT:
         {
            packet = new SessionXASetTimeoutMessage();
            break;
         }
         case EmptyPacket.SESS_XA_SET_TIMEOUT_RESP:
         {
            packet = new SessionXASetTimeoutResponseMessage();
            break;
         }
         case EmptyPacket.SESS_XA_GET_TIMEOUT:
         {
            packet = new EmptyPacket(EmptyPacket.SESS_XA_GET_TIMEOUT);
            break;
         }
         case EmptyPacket.SESS_XA_GET_TIMEOUT_RESP:
         {
            packet = new SessionXAGetTimeoutResponseMessage();
            break;
         }
         case EmptyPacket.CONS_FLOWTOKEN:
         {
            packet = new ConsumerFlowCreditMessage();
            break;
         }
         case EmptyPacket.PROD_SEND:
         {
            packet = new ProducerSendMessage();
            break;
         }
         case EmptyPacket.PROD_RECEIVETOKENS:
         {
            packet = new ProducerFlowCreditMessage();
            break;
         }
         case EmptyPacket.RECEIVE_MSG:
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
