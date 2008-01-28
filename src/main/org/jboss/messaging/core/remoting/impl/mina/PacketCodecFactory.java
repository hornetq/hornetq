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
import org.jboss.messaging.core.remoting.codec.AddTemporaryDestinationMessageCodec;
import org.jboss.messaging.core.remoting.codec.BrowserHasNextMessageResponseCodec;
import org.jboss.messaging.core.remoting.codec.BrowserNextMessageBlockRequestCodec;
import org.jboss.messaging.core.remoting.codec.BrowserNextMessageBlockResponseCodec;
import org.jboss.messaging.core.remoting.codec.BrowserNextMessageResponseCodec;
import org.jboss.messaging.core.remoting.codec.BytesPacketCodec;
import org.jboss.messaging.core.remoting.codec.ConnectionFactoryCreateConnectionRequestCodec;
import org.jboss.messaging.core.remoting.codec.ConnectionFactoryCreateConnectionResponseCodec;
import org.jboss.messaging.core.remoting.codec.ConsumerChangeRateMessageCodec;
import org.jboss.messaging.core.remoting.codec.CreateBrowserRequestCodec;
import org.jboss.messaging.core.remoting.codec.CreateBrowserResponseCodec;
import org.jboss.messaging.core.remoting.codec.CreateConsumerRequestCodec;
import org.jboss.messaging.core.remoting.codec.CreateConsumerResponseCodec;
import org.jboss.messaging.core.remoting.codec.CreateDestinationRequestCodec;
import org.jboss.messaging.core.remoting.codec.CreateDestinationResponseCodec;
import org.jboss.messaging.core.remoting.codec.CreateSessionRequestCodec;
import org.jboss.messaging.core.remoting.codec.CreateSessionResponseCodec;
import org.jboss.messaging.core.remoting.codec.DeleteTemporaryDestinationMessageCodec;
import org.jboss.messaging.core.remoting.codec.DeliverMessageCodec;
import org.jboss.messaging.core.remoting.codec.GetClientIDResponseCodec;
import org.jboss.messaging.core.remoting.codec.JMSExceptionMessageCodec;
import org.jboss.messaging.core.remoting.codec.RemotingBuffer;
import org.jboss.messaging.core.remoting.codec.SessionAcknowledgeMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCancelMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionSendMessageCodec;
import org.jboss.messaging.core.remoting.codec.SetClientIDMessageCodec;
import org.jboss.messaging.core.remoting.codec.SetSessionIDMessageCodec;
import org.jboss.messaging.core.remoting.codec.TextPacketCodec;
import org.jboss.messaging.core.remoting.codec.UnsubscribeMessageCodec;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.AddTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.BrowserHasNextMessageRequest;
import org.jboss.messaging.core.remoting.wireformat.BrowserHasNextMessageResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageBlockRequest;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageBlockResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageRequest;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserResetMessage;
import org.jboss.messaging.core.remoting.wireformat.BytesPacket;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingMessage;
import org.jboss.messaging.core.remoting.wireformat.ConsumerChangeRateMessage;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionResponse;
import org.jboss.messaging.core.remoting.wireformat.DeleteTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDRequest;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDResponse;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.Ping;
import org.jboss.messaging.core.remoting.wireformat.Pong;
import org.jboss.messaging.core.remoting.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCommitMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRecoverMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRollbackMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionSendMessage;
import org.jboss.messaging.core.remoting.wireformat.SetClientIDMessage;
import org.jboss.messaging.core.remoting.wireformat.SetSessionIDMessage;
import org.jboss.messaging.core.remoting.wireformat.StartConnectionMessage;
import org.jboss.messaging.core.remoting.wireformat.StopConnectionMessage;
import org.jboss.messaging.core.remoting.wireformat.TextPacket;
import org.jboss.messaging.core.remoting.wireformat.UnsubscribeMessage;
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

      addCodec(JMSExceptionMessage.class, JMSExceptionMessageCodec.class);

      // TextPacket are for testing purpose only!
      addCodec(TextPacket.class, TextPacketCodec.class);
      addCodec(BytesPacket.class, BytesPacketCodec.class);

      addCodecForEmptyPacket(PING, Ping.class);
      addCodecForEmptyPacket(PONG, Pong.class);
      addCodec(SetSessionIDMessage.class, SetSessionIDMessageCodec.class);

      addCodec(CreateConnectionRequest.class,
            ConnectionFactoryCreateConnectionRequestCodec.class);

      addCodec(CreateConnectionResponse.class,
            ConnectionFactoryCreateConnectionResponseCodec.class);

      addCodec(CreateSessionRequest.class, CreateSessionRequestCodec.class);

      addCodec(CreateSessionResponse.class, CreateSessionResponseCodec.class);

      addCodecForEmptyPacket(PacketType.REQ_GETCLIENTID,
            GetClientIDRequest.class);

      addCodec(GetClientIDResponse.class, GetClientIDResponseCodec.class);

      addCodec(SetClientIDMessage.class, SetClientIDMessageCodec.class);

      addCodec(SessionSendMessage.class, SessionSendMessageCodec.class);

      addCodec(CreateConsumerRequest.class, CreateConsumerRequestCodec.class);

      addCodec(CreateDestinationRequest.class,
            CreateDestinationRequestCodec.class);

      addCodec(CreateDestinationResponse.class,
            CreateDestinationResponseCodec.class);

      addCodec(CreateConsumerResponse.class, CreateConsumerResponseCodec.class);

      addCodec(CreateBrowserRequest.class, CreateBrowserRequestCodec.class);

      addCodec(CreateBrowserResponse.class, CreateBrowserResponseCodec.class);

      addCodecForEmptyPacket(PacketType.MSG_STARTCONNECTION,
            StartConnectionMessage.class);

      addCodecForEmptyPacket(PacketType.MSG_STOPCONNECTION,
            StopConnectionMessage.class);

      addCodec(ConsumerChangeRateMessage.class, ConsumerChangeRateMessageCodec.class);

      addCodec(DeliverMessage.class, DeliverMessageCodec.class);

      addCodec(SessionAcknowledgeMessage.class,
            SessionAcknowledgeMessageCodec.class);
      
      addCodec(SessionCancelMessage.class,
            SessionCancelMessageCodec.class);
      
      addCodecForEmptyPacket(PacketType.MSG_COMMIT, SessionCommitMessage.class);
      
      addCodecForEmptyPacket(PacketType.MSG_COMMIT, SessionRollbackMessage.class);

      addCodecForEmptyPacket(PacketType.MSG_CLOSE, CloseMessage.class);
      
      addCodecForEmptyPacket(PacketType.MSG_CLOSING, ClosingMessage.class);
      
      addCodecForEmptyPacket(PacketType.MSG_RECOVER, SessionRecoverMessage.class);
      
      
      addCodecForEmptyPacket(PacketType.MSG_BROWSER_RESET,
            BrowserResetMessage.class);

      addCodecForEmptyPacket(PacketType.REQ_BROWSER_HASNEXTMESSAGE,
            BrowserHasNextMessageRequest.class);

      addCodec(BrowserHasNextMessageResponse.class,
            BrowserHasNextMessageResponseCodec.class);

      addCodecForEmptyPacket(PacketType.REQ_BROWSER_NEXTMESSAGE,
            BrowserNextMessageRequest.class);

      addCodec(BrowserNextMessageResponse.class,
            BrowserNextMessageResponseCodec.class);

      addCodec(BrowserNextMessageBlockRequest.class,
            BrowserNextMessageBlockRequestCodec.class);

      addCodec(BrowserNextMessageBlockResponse.class,
            BrowserNextMessageBlockResponseCodec.class);

      addCodec(UnsubscribeMessage.class, UnsubscribeMessageCodec.class);

      addCodec(AddTemporaryDestinationMessage.class,
            AddTemporaryDestinationMessageCodec.class);

      addCodec(DeleteTemporaryDestinationMessage.class,
            DeleteTemporaryDestinationMessageCodec.class);
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
