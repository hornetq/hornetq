/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.BYTES;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONN_CREATESESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONN_CREATESESSION_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONN_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONN_STOP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONS_DELIVER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONS_FLOWTOKEN;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CREATECONNECTION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CREATECONNECTION_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.EXCEPTION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.NULL;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PING;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PONG;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_RECEIVETOKENS;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_SEND;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_ACKNOWLEDGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_ADD_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BINDINGQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BINDINGQUERY_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_HASNEXTMESSAGE_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGEBLOCK;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGEBLOCK_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGE_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_RESET;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CANCEL;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEBROWSER_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATECONSUMER_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEPRODUCER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEPRODUCER_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEQUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_DELETE_QUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_QUEUEQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_QUEUEQUERY_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_RECOVER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_REMOVE_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_END;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_FORGET;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_GET_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_GET_TIMEOUT_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_INDOUBT_XIDS;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_INDOUBT_XIDS_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_JOIN;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_PREPARE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_RESUME;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_SET_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_SET_TIMEOUT_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_XA_SUSPEND;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.TEXT;

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
import org.jboss.messaging.core.remoting.impl.codec.SessionAddDestinationMessageCodec;
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
import org.jboss.messaging.core.remoting.impl.codec.SessionRemoveDestinationMessageCodec;
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
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class PacketCodecFactory extends DemuxingProtocolCodecFactory
{
   // Constants -----------------------------------------------------

   private final Logger log = Logger.getLogger(PacketCodecFactory.class);
   private final MinaEncoder encoder;
   private final MinaDecoder decoder;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // FIXME: split encoder/decoder required only on client and/or server sides
   public PacketCodecFactory()
   {

       decoder = new MinaDecoder();
       encoder = new MinaEncoder();
       addMessageDecoder(decoder);
       addMessageEncoder(Packet.class, encoder);
      
      addCodecForEmptyPacket(encoder, decoder, NULL);
      addCodec(encoder, decoder, PROD_SEND,
            new ProducerSendMessageCodec());
      addCodec(encoder, decoder, CONS_DELIVER,
            new ConsumerDeliverMessageCodec());

      // TextPacket are for testing purpose only!
      addCodec(encoder, decoder, TEXT, new TextPacketCodec());
      addCodec(encoder, decoder, BYTES, new BytesPacketCodec());

      addCodec(encoder, decoder, PING, new PingCodec());
      addCodec(encoder, decoder, PONG, new PongCodec());

      addCodec(encoder, decoder, EXCEPTION,
            new MessagingExceptionMessageCodec());

      addCodec(encoder, decoder, CREATECONNECTION,
            new CreateConnectionMessageCodec());

      addCodec(encoder, decoder, CREATECONNECTION_RESP,
            new CreateConnectionResponseMessageCodec());

      addCodec(encoder, decoder, CONN_CREATESESSION,
            new ConnectionCreateSessionMessageCodec());

      addCodec(encoder, decoder, CONN_CREATESESSION_RESP,
            new ConnectionCreateSessionResponseMessageCodec());

      addCodec(encoder, decoder, SESS_CREATECONSUMER,
            new SessionCreateConsumerMessageCodec());

      addCodec(encoder, decoder, SESS_CREATECONSUMER_RESP,
            new SessionCreateConsumerResponseMessageCodec());

      addCodec(encoder, decoder, SESS_CREATEPRODUCER,
            new SessionCreateProducerMessageCodec());

      addCodec(encoder, decoder, SESS_CREATEPRODUCER_RESP,
            new SessionCreateProducerResponseMessageCodec());

      addCodec(encoder, decoder, SESS_CREATEBROWSER,
            new SessionCreateBrowserMessageCodec());

      addCodec(encoder, decoder, SESS_CREATEBROWSER_RESP,
            new SessionCreateBrowserResponseMessageCodec());

      addCodecForEmptyPacket(encoder, decoder, CONN_START);

      addCodecForEmptyPacket(encoder, decoder, CONN_STOP);

      addCodec(encoder, decoder, CONS_FLOWTOKEN,
            new ConsumerFlowTokenMessageCodec());

      addCodec(encoder, decoder, SESS_ACKNOWLEDGE,
            new SessionAcknowledgeMessageCodec());

      addCodec(encoder, decoder, SESS_CANCEL, new SessionCancelMessageCodec());

      addCodecForEmptyPacket(encoder, decoder, SESS_COMMIT);

      addCodecForEmptyPacket(encoder, decoder, SESS_ROLLBACK);

      addCodecForEmptyPacket(encoder, decoder, CLOSE);

      addCodecForEmptyPacket(encoder, decoder, SESS_RECOVER);

      addCodecForEmptyPacket(encoder, decoder, SESS_BROWSER_RESET);

      addCodecForEmptyPacket(encoder, decoder, SESS_BROWSER_HASNEXTMESSAGE);

      addCodec(encoder, decoder, SESS_BROWSER_HASNEXTMESSAGE_RESP,
            new SessionBrowserHasNextMessageResponseMessageCodec());

      addCodecForEmptyPacket(encoder, decoder, SESS_BROWSER_NEXTMESSAGE);

      addCodec(encoder, decoder, SESS_BROWSER_NEXTMESSAGE_RESP,
            new SessionBrowserNextMessageResponseMessageCodec());

      addCodec(encoder, decoder, SESS_BROWSER_NEXTMESSAGEBLOCK,
            new SessionBrowserNextMessageBlockMessageCodec());

      addCodec(encoder, decoder, SESS_BROWSER_NEXTMESSAGEBLOCK_RESP,
            new SessionBrowserNextMessageBlockResponseMessageCodec());

      addCodec(encoder, decoder, SESS_XA_COMMIT,
            new SessionXACommitMessageCodec());

      addCodec(encoder, decoder, SESS_XA_END, new SessionXAEndMessageCodec());

      addCodec(encoder, decoder, SESS_XA_FORGET,
            new SessionXAForgetMessageCodec());

      addCodecForEmptyPacket(encoder, decoder, SESS_XA_INDOUBT_XIDS);

      addCodec(encoder, decoder, SESS_XA_INDOUBT_XIDS_RESP,
            new SessionXAGetInDoubtXidsResponseMessageCodec());

      addCodecForEmptyPacket(encoder, decoder, SESS_XA_GET_TIMEOUT);

      addCodec(encoder, decoder, SESS_XA_GET_TIMEOUT_RESP,
            new SessionXAGetTimeoutResponseMessageCodec());

      addCodec(encoder, decoder, SESS_XA_JOIN, new SessionXAJoinMessageCodec());

      addCodec(encoder, decoder, SESS_XA_PREPARE,
            new SessionXAPrepareMessageCodec());

      addCodec(encoder, decoder, SESS_XA_RESP,
            new SessionXAResponseMessageCodec());

      addCodec(encoder, decoder, SESS_XA_RESUME,
            new SessionXAResumeMessageCodec());

      addCodec(encoder, decoder, SESS_XA_ROLLBACK,
            new SessionXARollbackMessageCodec());

      addCodec(encoder, decoder, SESS_XA_SET_TIMEOUT,
            new SessionXASetTimeoutMessageCodec());

      addCodec(encoder, decoder, SESS_XA_SET_TIMEOUT_RESP,
            new SessionXASetTimeoutResponseMessageCodec());

      addCodec(encoder, decoder, SESS_XA_START,
            new SessionXAStartMessageCodec());

      addCodecForEmptyPacket(encoder, decoder, SESS_XA_SUSPEND);

      addCodec(encoder, decoder, SESS_REMOVE_DESTINATION,
            new SessionRemoveDestinationMessageCodec());

      addCodec(encoder, decoder, SESS_CREATEQUEUE,
            new SessionCreateQueueMessageCodec());

      addCodec(encoder, decoder, SESS_QUEUEQUERY,
            new SessionQueueQueryMessageCodec());

      addCodec(encoder, decoder, SESS_QUEUEQUERY_RESP,
            new SessionQueueQueryResponseMessageCodec());

      addCodec(encoder, decoder, SESS_ADD_DESTINATION,
            new SessionAddDestinationMessageCodec());

      addCodec(encoder, decoder, SESS_BINDINGQUERY,
            new SessionBindingQueryMessageCodec());

      addCodec(encoder, decoder, SESS_BINDINGQUERY_RESP,
            new SessionBindingQueryResponseMessageCodec());

      addCodec(encoder, decoder, SESS_DELETE_QUEUE,
            new SessionDeleteQueueMessageCodec());

      addCodec(encoder, decoder, PROD_RECEIVETOKENS,
            new ProducerReceiveTokensMessageCodec());

   }

   // Public --------------------------------------------------------

   public static AbstractPacketCodec<Packet> createCodecForEmptyPacket(
         final PacketType type)
   {
      return new CodecForEmptyPacket<Packet>(type);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // FIXME generics definition should be in term of <P>...
   private void addCodec(MinaEncoder encoder, MinaDecoder decoder,
         PacketType type, AbstractPacketCodec<? extends Packet> codec)
   {
      try
      {
         decoder.put(type, codec);
         encoder.put(type, codec);
      } catch (Exception e)
      {
         log.error("Unable to add codec for packet " + type, e);
      }
   }
   
   private void addCodecForEmptyPacket(MinaEncoder encoder,
         MinaDecoder decoder, PacketType type)
   {
      AbstractPacketCodec<Packet> codec = createCodecForEmptyPacket(
            type);
      addCodec(encoder, decoder, type, codec);
   }

   // Inner classes -------------------------------------------------

   static class CodecForEmptyPacket<P extends Packet> extends
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
      protected Packet decodeBody(RemotingBuffer in) throws Exception
      {
         in.getInt(); // skip body length
         return new PacketImpl(type);
      }
   }
}
