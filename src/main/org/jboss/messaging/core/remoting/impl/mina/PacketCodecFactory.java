/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.NULL;

import org.apache.mina.filter.codec.demux.DemuxingProtocolCodecFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.BytesPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConnectionCreateSessionMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConnectionCreateSessionResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConsumerDeliverMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConsumerFlowTokenMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.CreateConnectionMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.CreateConnectionResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.MessagingExceptionMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.PingCodec;
import org.jboss.messaging.core.remoting.impl.codec.PongCodec;
import org.jboss.messaging.core.remoting.impl.codec.ProducerReceiveTokensMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ProducerSendMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.RemotingBuffer;
import org.jboss.messaging.core.remoting.impl.codec.SessionAcknowledgeMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionAddAddressMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBindingQueryMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBindingQueryResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBrowserHasNextMessageResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBrowserNextMessageBlockMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBrowserNextMessageBlockResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBrowserNextMessageResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionCancelMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionCreateBrowserMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionCreateBrowserResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionCreateConsumerMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionCreateConsumerResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionCreateProducerMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionCreateProducerResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionCreateQueueMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionDeleteQueueMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionQueueQueryMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionQueueQueryResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionRemoveAddressMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXACommitMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXAEndMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXAForgetMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXAGetInDoubtXidsResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXAGetTimeoutResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXAJoinMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXAPrepareMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXAResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXAResumeMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXARollbackMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXASetTimeoutMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXASetTimeoutResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionXAStartMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.TextPacketCodec;
import org.jboss.messaging.core.remoting.impl.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.BytesPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionStartMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionStopMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerDeliverMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerReceiveTokensMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddAddressMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageBlockMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageBlockResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserResetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCommitMessage;
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
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRecoverMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveAddressMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionRollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAStartMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXASuspendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.TextPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class PacketCodecFactory extends DemuxingProtocolCodecFactory
{
   // Constants -----------------------------------------------------

   private final Logger log = Logger.getLogger(PacketCodecFactory.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // FIXME: split encoder/decoder required only on client and/or server sides
   public PacketCodecFactory()
   {
      addCodecForEmptyPacket(NULL, NullPacket.class);

      // TextPacket are for testing purpose only!
      addCodec(TextPacket.class, TextPacketCodec.class);
      addCodec(BytesPacket.class, BytesPacketCodec.class);

      addCodec(Ping.class, PingCodec.class);
      addCodec(Pong.class, PongCodec.class);
      
      addCodec(MessagingExceptionMessage.class, MessagingExceptionMessageCodec.class);

      addCodec(CreateConnectionRequest.class,
            CreateConnectionMessageCodec.class);

      addCodec(CreateConnectionResponse.class,
            CreateConnectionResponseMessageCodec.class);

      addCodec(ConnectionCreateSessionMessage.class, ConnectionCreateSessionMessageCodec.class);

      addCodec(ConnectionCreateSessionResponseMessage.class, ConnectionCreateSessionResponseMessageCodec.class);

      addCodec(SessionCreateConsumerMessage.class, SessionCreateConsumerMessageCodec.class);

      addCodec(SessionCreateConsumerResponseMessage.class, SessionCreateConsumerResponseMessageCodec.class);
      
      addCodec(SessionCreateProducerMessage.class, SessionCreateProducerMessageCodec.class);

      addCodec(SessionCreateProducerResponseMessage.class, SessionCreateProducerResponseMessageCodec.class);

      addCodec(SessionCreateBrowserMessage.class, SessionCreateBrowserMessageCodec.class);

      addCodec(SessionCreateBrowserResponseMessage.class, SessionCreateBrowserResponseMessageCodec.class);

      addCodecForEmptyPacket(PacketType.CONN_START,
            ConnectionStartMessage.class);

      addCodecForEmptyPacket(PacketType.CONN_STOP,
            ConnectionStopMessage.class);

      addCodec(ConsumerFlowTokenMessage.class, ConsumerFlowTokenMessageCodec.class);

      addCodec(ConsumerDeliverMessage.class, ConsumerDeliverMessageCodec.class);

      addCodec(SessionAcknowledgeMessage.class,
            SessionAcknowledgeMessageCodec.class);
      
      addCodec(SessionCancelMessage.class,
            SessionCancelMessageCodec.class);
      
      addCodecForEmptyPacket(PacketType.SESS_COMMIT, SessionCommitMessage.class);
      
      addCodecForEmptyPacket(PacketType.SESS_ROLLBACK, SessionRollbackMessage.class);

      addCodecForEmptyPacket(PacketType.CLOSE, CloseMessage.class);
      
      addCodecForEmptyPacket(PacketType.SESS_RECOVER, SessionRecoverMessage.class);
            
      addCodecForEmptyPacket(PacketType.SESS_BROWSER_RESET,
            SessionBrowserResetMessage.class);

      addCodecForEmptyPacket(PacketType.SESS_BROWSER_HASNEXTMESSAGE,
            SessionBrowserHasNextMessageMessage.class);

      addCodec(SessionBrowserHasNextMessageResponseMessage.class,
            SessionBrowserHasNextMessageResponseMessageCodec.class);

      addCodecForEmptyPacket(PacketType.SESS_BROWSER_NEXTMESSAGE,
            SessionBrowserNextMessageMessage.class);

      addCodec(SessionBrowserNextMessageResponseMessage.class,
            SessionBrowserNextMessageResponseMessageCodec.class);

      addCodec(SessionBrowserNextMessageBlockMessage.class,
            SessionBrowserNextMessageBlockMessageCodec.class);

      addCodec(SessionBrowserNextMessageBlockResponseMessage.class,
            SessionBrowserNextMessageBlockResponseMessageCodec.class);

      addCodec(SessionXACommitMessage.class, SessionXACommitMessageCodec.class);
      
      addCodec(SessionXAEndMessage.class, SessionXAEndMessageCodec.class);
      
      addCodec(SessionXAForgetMessage.class, SessionXAForgetMessageCodec.class);
      
      addCodecForEmptyPacket(PacketType.SESS_XA_INDOUBT_XIDS,
            SessionXAGetInDoubtXidsMessage.class);
      
      addCodec(SessionXAGetInDoubtXidsResponseMessage.class, SessionXAGetInDoubtXidsResponseMessageCodec.class);
      
      addCodecForEmptyPacket(PacketType.SESS_XA_GET_TIMEOUT, SessionXAGetTimeoutMessage.class);
      
      addCodec(SessionXAGetTimeoutResponseMessage.class, SessionXAGetTimeoutResponseMessageCodec.class);
      
      addCodec(SessionXAJoinMessage.class, SessionXAJoinMessageCodec.class);
      
      addCodec(SessionXAPrepareMessage.class, SessionXAPrepareMessageCodec.class);
      
      addCodec(SessionXAResponseMessage.class, SessionXAResponseMessageCodec.class);
      
      addCodec(SessionXAResumeMessage.class, SessionXAResumeMessageCodec.class);
      
      addCodec(SessionXARollbackMessage.class, SessionXARollbackMessageCodec.class);
      
      addCodec(SessionXASetTimeoutMessage.class, SessionXASetTimeoutMessageCodec.class);
      
      addCodec(SessionXASetTimeoutResponseMessage.class, SessionXASetTimeoutResponseMessageCodec.class);
      
      addCodec(SessionXAStartMessage.class, SessionXAStartMessageCodec.class);
      
      addCodecForEmptyPacket(PacketType.SESS_XA_SUSPEND, SessionXASuspendMessage.class);
      
      addCodec(SessionRemoveAddressMessage.class, SessionRemoveAddressMessageCodec.class);
      
      addCodec(SessionCreateQueueMessage.class, SessionCreateQueueMessageCodec.class);
      
      addCodec(SessionQueueQueryMessage.class, SessionQueueQueryMessageCodec.class);
      
      addCodec(SessionQueueQueryResponseMessage.class, SessionQueueQueryResponseMessageCodec.class);
      
      addCodec(SessionAddAddressMessage.class, SessionAddAddressMessageCodec.class);
      
      addCodec(SessionBindingQueryMessage.class, SessionBindingQueryMessageCodec.class);
      
      addCodec(SessionBindingQueryResponseMessage.class, SessionBindingQueryResponseMessageCodec.class);
      
      addCodec(SessionDeleteQueueMessage.class, SessionDeleteQueueMessageCodec.class);
      
      addCodec(ProducerSendMessage.class, ProducerSendMessageCodec.class);
      
      addCodec(ProducerReceiveTokensMessage.class, ProducerReceiveTokensMessageCodec.class);

   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // FIXME generics definition should be in term of <P>...
   private void addCodec(
         Class<? extends AbstractPacket> packetClass,
         Class<? extends AbstractPacketCodec<? extends AbstractPacket>> codecClass)
   {
      try
      {
         AbstractPacketCodec<? extends AbstractPacket> codec = codecClass.newInstance();
         MinaPacketCodec<AbstractPacket> minaCodec = new MinaPacketCodec(codec);
         super.addMessageDecoder(minaCodec);
         super.addMessageEncoder(packetClass, minaCodec);
      } catch (Exception e)
      {
         log.error("Unable to add codec for packet class " + packetClass.getName(), e);
      }
   }

   private void addCodecForEmptyPacket(PacketType type,
         Class<? extends AbstractPacket> packetClass)
   {
      AbstractPacketCodec<AbstractPacket> codec = createCodecForEmptyPacket(
            type, packetClass);
      MinaPacketCodec<AbstractPacket> minaCodec = new MinaPacketCodec<AbstractPacket>(
            codec);
      super.addMessageDecoder(minaCodec);
      super.addMessageEncoder(packetClass, minaCodec);
   }

   public static AbstractPacketCodec<AbstractPacket> createCodecForEmptyPacket(
         PacketType type, final Class<? extends AbstractPacket> clazz)
   {
      return new CodecForEmptyPacket<AbstractPacket>(type)
      {
         @Override
         protected AbstractPacket newPacket()
         {
            try
            {
               return (AbstractPacket) clazz.newInstance();
            } catch (Throwable t)
            {
               return null;
            }
         }
      };
   }

   // Inner classes -------------------------------------------------

   abstract static class CodecForEmptyPacket<P extends AbstractPacket> extends
         AbstractPacketCodec<P>
   {

      public CodecForEmptyPacket(PacketType type)
      {
         super(type);
      }

      @Override
      protected void encodeBody(P packet, RemotingBuffer out) throws Exception
      {
         // no body
         out.putInt(0);
      }

      @Override
      protected P decodeBody(RemotingBuffer in) throws Exception
      {
         in.getInt(); // skip body length
         return newPacket();
      }

      protected abstract P newPacket();
   }
}
