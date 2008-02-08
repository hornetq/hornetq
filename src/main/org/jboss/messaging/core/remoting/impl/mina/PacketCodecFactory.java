/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.NULL;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.PING;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.PONG;

import org.apache.mina.filter.codec.demux.DemuxingProtocolCodecFactory;
import org.jboss.messaging.core.remoting.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.codec.BytesPacketCodec;
import org.jboss.messaging.core.remoting.codec.ConnectionCreateSessionMessageCodec;
import org.jboss.messaging.core.remoting.codec.ConnectionCreateSessionResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.ConsumerFlowTokenMessageCodec;
import org.jboss.messaging.core.remoting.codec.CreateConnectionMessageCodec;
import org.jboss.messaging.core.remoting.codec.CreateConnectionResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.DeliverMessageCodec;
import org.jboss.messaging.core.remoting.codec.MessagingExceptionMessageCodec;
import org.jboss.messaging.core.remoting.codec.RemotingBuffer;
import org.jboss.messaging.core.remoting.codec.SessionAcknowledgeMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionAddAddressMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBindingQueryMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBindingQueryResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBrowserHasNextMessageResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBrowserNextMessageBlockMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBrowserNextMessageBlockResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBrowserNextMessageResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCancelMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCreateBrowserMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCreateBrowserResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCreateConsumerMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCreateConsumerResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCreateQueueMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionDeleteQueueMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionQueueQueryMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionQueueQueryResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionRemoveAddressMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionSendMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionSetIDMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXACommitMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXAEndMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXAForgetMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXAGetInDoubtXidsResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXAGetTimeoutResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXAJoinMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXAPrepareMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXAResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXAResumeMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXARollbackMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXASetTimeoutMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXASetTimeoutResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionXAStartMessageCodec;
import org.jboss.messaging.core.remoting.codec.TextPacketCodec;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.BytesPacket;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionStartMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionStopMessage;
import org.jboss.messaging.core.remoting.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.core.remoting.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.Ping;
import org.jboss.messaging.core.remoting.wireformat.Pong;
import org.jboss.messaging.core.remoting.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionAddAddressMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserHasNextMessageMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageBlockMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageBlockResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserResetMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCommitMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRecoverMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRemoveAddressMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRollbackMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionSetIDMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXACommitMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAEndMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAForgetMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetInDoubtXidsMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetTimeoutMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAJoinMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAPrepareMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAResumeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXARollbackMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASetTimeoutResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXAStartMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionXASuspendMessage;
import org.jboss.messaging.core.remoting.wireformat.TextPacket;
import org.jboss.messaging.util.Logger;

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

      addCodecForEmptyPacket(PING, Ping.class);
      addCodecForEmptyPacket(PONG, Pong.class);
      addCodec(SessionSetIDMessage.class, SessionSetIDMessageCodec.class);
      
      addCodec(MessagingExceptionMessage.class, MessagingExceptionMessageCodec.class);

      addCodec(CreateConnectionRequest.class,
            CreateConnectionMessageCodec.class);

      addCodec(CreateConnectionResponse.class,
            CreateConnectionResponseMessageCodec.class);

      addCodec(ConnectionCreateSessionMessage.class, ConnectionCreateSessionMessageCodec.class);

      addCodec(ConnectionCreateSessionResponseMessage.class, ConnectionCreateSessionResponseMessageCodec.class);

      addCodec(SessionSendMessage.class, SessionSendMessageCodec.class);

      addCodec(SessionCreateConsumerMessage.class, SessionCreateConsumerMessageCodec.class);

      addCodec(SessionCreateConsumerResponseMessage.class, SessionCreateConsumerResponseMessageCodec.class);

      addCodec(SessionCreateBrowserMessage.class, SessionCreateBrowserMessageCodec.class);

      addCodec(SessionCreateBrowserResponseMessage.class, SessionCreateBrowserResponseMessageCodec.class);

      addCodecForEmptyPacket(PacketType.CONN_START,
            ConnectionStartMessage.class);

      addCodecForEmptyPacket(PacketType.CONN_STOP,
            ConnectionStopMessage.class);

      addCodec(ConsumerFlowTokenMessage.class, ConsumerFlowTokenMessageCodec.class);

      addCodec(DeliverMessage.class, DeliverMessageCodec.class);

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
