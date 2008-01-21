/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.NULL;

import org.apache.mina.filter.codec.demux.DemuxingProtocolCodecFactory;
import org.jboss.messaging.core.remoting.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.codec.AcknowledgeDeliveriesRequestCodec;
import org.jboss.messaging.core.remoting.codec.AcknowledgeDeliveryRequestCodec;
import org.jboss.messaging.core.remoting.codec.AcknowledgeDeliveryResponseCodec;
import org.jboss.messaging.core.remoting.codec.AddTemporaryDestinationMessageCodec;
import org.jboss.messaging.core.remoting.codec.BrowserHasNextMessageResponseCodec;
import org.jboss.messaging.core.remoting.codec.BrowserNextMessageBlockRequestCodec;
import org.jboss.messaging.core.remoting.codec.BrowserNextMessageBlockResponseCodec;
import org.jboss.messaging.core.remoting.codec.BrowserNextMessageResponseCodec;
import org.jboss.messaging.core.remoting.codec.BytesPacketCodec;
import org.jboss.messaging.core.remoting.codec.CancelDeliveriesMessageCodec;
import org.jboss.messaging.core.remoting.codec.CancelDeliveryMessageCodec;
import org.jboss.messaging.core.remoting.codec.ChangeRateMessageCodec;
import org.jboss.messaging.core.remoting.codec.ClosingRequestCodec;
import org.jboss.messaging.core.remoting.codec.ClosingResponseCodec;
import org.jboss.messaging.core.remoting.codec.ConnectionFactoryCreateConnectionRequestCodec;
import org.jboss.messaging.core.remoting.codec.ConnectionFactoryCreateConnectionResponseCodec;
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
import org.jboss.messaging.core.remoting.codec.GetPreparedTransactionsResponseCodec;
import org.jboss.messaging.core.remoting.codec.JMSExceptionMessageCodec;
import org.jboss.messaging.core.remoting.codec.RemotingBuffer;
import org.jboss.messaging.core.remoting.codec.SendMessageCodec;
import org.jboss.messaging.core.remoting.codec.SendTransactionMessageCodec;
import org.jboss.messaging.core.remoting.codec.SetClientIDMessageCodec;
import org.jboss.messaging.core.remoting.codec.TextPacketCodec;
import org.jboss.messaging.core.remoting.codec.UnsubscribeMessageCodec;
import org.jboss.messaging.core.remoting.codec.UpdateCallbackMessageCodec;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveryRequest;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveryResponse;
import org.jboss.messaging.core.remoting.wireformat.AddTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.BrowserHasNextMessageRequest;
import org.jboss.messaging.core.remoting.wireformat.BrowserHasNextMessageResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageBlockRequest;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageBlockResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageRequest;
import org.jboss.messaging.core.remoting.wireformat.BrowserNextMessageResponse;
import org.jboss.messaging.core.remoting.wireformat.BrowserResetMessage;
import org.jboss.messaging.core.remoting.wireformat.BytesPacket;
import org.jboss.messaging.core.remoting.wireformat.CancelDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.CancelDeliveryMessage;
import org.jboss.messaging.core.remoting.wireformat.ChangeRateMessage;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;
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
import org.jboss.messaging.core.remoting.wireformat.GetPreparedTransactionsRequest;
import org.jboss.messaging.core.remoting.wireformat.GetPreparedTransactionsResponse;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SendMessage;
import org.jboss.messaging.core.remoting.wireformat.SendTransactionMessage;
import org.jboss.messaging.core.remoting.wireformat.SetClientIDMessage;
import org.jboss.messaging.core.remoting.wireformat.StartConnectionMessage;
import org.jboss.messaging.core.remoting.wireformat.StopConnectionMessage;
import org.jboss.messaging.core.remoting.wireformat.TextPacket;
import org.jboss.messaging.core.remoting.wireformat.UnsubscribeMessage;
import org.jboss.messaging.core.remoting.wireformat.UpdateCallbackMessage;
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

      addCodec(CreateConnectionRequest.class,
            ConnectionFactoryCreateConnectionRequestCodec.class);

      addCodec(CreateConnectionResponse.class,
            ConnectionFactoryCreateConnectionResponseCodec.class);

      addCodec(UpdateCallbackMessage.class, UpdateCallbackMessageCodec.class);

      addCodec(CreateSessionRequest.class, CreateSessionRequestCodec.class);

      addCodec(CreateSessionResponse.class, CreateSessionResponseCodec.class);

      addCodecForEmptyPacket(PacketType.REQ_GETCLIENTID,
            GetClientIDRequest.class);

      addCodec(GetClientIDResponse.class, GetClientIDResponseCodec.class);

      addCodec(SetClientIDMessage.class, SetClientIDMessageCodec.class);

      addCodec(SendMessage.class, SendMessageCodec.class);

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

      addCodec(ChangeRateMessage.class, ChangeRateMessageCodec.class);

      addCodec(DeliverMessage.class, DeliverMessageCodec.class);

      addCodec(AcknowledgeDeliveryRequest.class,
            AcknowledgeDeliveryRequestCodec.class);

      addCodec(AcknowledgeDeliveryResponse.class,
            AcknowledgeDeliveryResponseCodec.class);

      addCodec(AcknowledgeDeliveriesMessage.class,
            AcknowledgeDeliveriesRequestCodec.class);

      addCodec(CancelDeliveryMessage.class, CancelDeliveryMessageCodec.class);

      addCodec(CancelDeliveriesMessage.class,
            CancelDeliveriesMessageCodec.class);

      addCodec(ClosingRequest.class, ClosingRequestCodec.class);

      addCodec(ClosingResponse.class, ClosingResponseCodec.class);

      addCodecForEmptyPacket(PacketType.MSG_CLOSE, CloseMessage.class);

      addCodec(SendTransactionMessage.class, SendTransactionMessageCodec.class);

      addCodecForEmptyPacket(PacketType.REQ_GETPREPAREDTRANSACTIONS,
            GetPreparedTransactionsRequest.class);

      addCodec(GetPreparedTransactionsResponse.class,
            GetPreparedTransactionsResponseCodec.class);

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
