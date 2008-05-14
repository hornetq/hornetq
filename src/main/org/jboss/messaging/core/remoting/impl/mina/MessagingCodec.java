/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CREATECONNECTION;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CREATECONNECTION_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.EXCEPTION;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.NULL;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.PING;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.PONG;
import static org.jboss.messaging.util.DataConstants.SIZE_INT;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerReceiveTokensMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * 
 * A MessagingCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 */
public class MessagingCodec extends CumulativeProtocolDecoder
   implements ProtocolEncoder, ProtocolCodecFactory
{
   private static final Logger log = Logger.getLogger(MessagingCodec.class);

   // ProtocolCodecFactory implementation
   // -----------------------------------------------------------------------------------

   public ProtocolDecoder getDecoder(final IoSession session)
   {
      return this;
   }

   public ProtocolEncoder getEncoder(final IoSession session)
   {
      return this;
   }

   
   // ProtocolEncoder implementation ------------------------------------------

   public void dispose(final IoSession session) throws Exception
   {
   }

   public void encode(final IoSession session, final Object message,
         final ProtocolEncoderOutput out) throws Exception
   {
      Packet packet = (Packet) message;
      
      IoBuffer iobuf = IoBuffer.allocate(1024, false);
      
      iobuf.setAutoExpand(true);
      
      MessagingBuffer buffer = new BufferWrapper(iobuf);

      packet.encode(buffer);
      
      out.write(iobuf);
   }
      
   // CumulativeProtocolDecoder overrides
   // -------------------------------------------------------------------------------------

   public boolean doDecode(final IoSession session, final IoBuffer in, final ProtocolDecoderOutput out) throws Exception
   {
      int start = in.position();

      if (in.remaining() <= SIZE_INT)
      {
         return false;
      }

      int length = in.getInt();
      
      if (in.remaining() < length)
      {
         in.position(start);
         
         return false;
      }

      int limit = in.limit();
      in.limit(in.position() + length);
      
      byte packetType = in.get();
      
      Packet packet;
      
      try
      {
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
               packet = new ConsumerFlowTokenMessage();
               break;
            }
            case EmptyPacket.PROD_SEND:
            {
               packet = new ProducerSendMessage();
               break;
            }
            case EmptyPacket.PROD_RECEIVETOKENS:
            {
               packet = new ProducerReceiveTokensMessage();
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
         
         MessagingBuffer buff = new BufferWrapper(in.slice());
         
         packet.decode(buff);

         out.write(packet);
         
         return true;
      }
      finally
      {
         in.position(in.limit());
         
         in.limit(limit);
      }
   }
}
