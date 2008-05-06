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

import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.BytesPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConnectionCreateSessionMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConnectionCreateSessionResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConsumerDeliverMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConsumerFlowTokenMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.CreateConnectionMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.CreateConnectionResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.EmptyPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.MessagingExceptionMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.PingCodec;
import org.jboss.messaging.core.remoting.impl.codec.PongCodec;
import org.jboss.messaging.core.remoting.impl.codec.ProducerReceiveTokensMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ProducerSendMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionAcknowledgeMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionAddDestinationMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBindingQueryMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBindingQueryResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBrowserHasNextMessageResponseMessageCodec;
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
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class PacketCodecFactory implements ProtocolCodecFactory
{
   // Constants -----------------------------------------------------

	private final Logger log = Logger.getLogger(PacketCodecFactory.class);
	
   private final MessagingCodec codec;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // FIXME: split encoder/decoder required only on client and/or server sides
   public PacketCodecFactory()
   {
      codec = new MessagingCodec();

      addCodecForEmptyPacket(NULL);
      
      addCodec(PROD_SEND, new ProducerSendMessageCodec());
      
      addCodec(CONS_DELIVER, new ConsumerDeliverMessageCodec());

      // TextPacket are for testing purpose only!
      addCodec(TEXT, new TextPacketCodec());
      addCodec(BYTES, new BytesPacketCodec());

      addCodec(PING, new PingCodec());
      addCodec(PONG, new PongCodec());

      addCodec(EXCEPTION, new MessagingExceptionMessageCodec());

      addCodec(CREATECONNECTION, new CreateConnectionMessageCodec());

      addCodec(CREATECONNECTION_RESP, new CreateConnectionResponseMessageCodec());

      addCodec(CONN_CREATESESSION,  new ConnectionCreateSessionMessageCodec());

      addCodec(CONN_CREATESESSION_RESP, new ConnectionCreateSessionResponseMessageCodec());

      addCodec(SESS_CREATECONSUMER, new SessionCreateConsumerMessageCodec());

      addCodec(SESS_CREATECONSUMER_RESP, new SessionCreateConsumerResponseMessageCodec());

      addCodec(SESS_CREATEPRODUCER,  new SessionCreateProducerMessageCodec());

      addCodec(SESS_CREATEPRODUCER_RESP, new SessionCreateProducerResponseMessageCodec());

      addCodec(SESS_CREATEBROWSER, new SessionCreateBrowserMessageCodec());

      addCodec(SESS_CREATEBROWSER_RESP, new SessionCreateBrowserResponseMessageCodec());

      addCodecForEmptyPacket(CONN_START);

      addCodecForEmptyPacket(CONN_STOP);

      addCodec(CONS_FLOWTOKEN, new ConsumerFlowTokenMessageCodec());

      addCodec(SESS_ACKNOWLEDGE, new SessionAcknowledgeMessageCodec());

      addCodec(SESS_CANCEL, new SessionCancelMessageCodec());

      addCodecForEmptyPacket(SESS_COMMIT);

      addCodecForEmptyPacket(SESS_ROLLBACK);

      addCodecForEmptyPacket(CLOSE);

      addCodecForEmptyPacket(SESS_RECOVER);

      addCodecForEmptyPacket(SESS_BROWSER_RESET);

      addCodecForEmptyPacket(SESS_BROWSER_HASNEXTMESSAGE);

      addCodec(SESS_BROWSER_HASNEXTMESSAGE_RESP, new SessionBrowserHasNextMessageResponseMessageCodec());

      addCodecForEmptyPacket(SESS_BROWSER_NEXTMESSAGE);

      addCodec(SESS_BROWSER_NEXTMESSAGE_RESP, new SessionBrowserNextMessageResponseMessageCodec());

      addCodec(SESS_XA_COMMIT, new SessionXACommitMessageCodec());

      addCodec(SESS_XA_END, new SessionXAEndMessageCodec());

      addCodec(SESS_XA_FORGET, new SessionXAForgetMessageCodec());

      addCodecForEmptyPacket(SESS_XA_INDOUBT_XIDS);

      addCodec(SESS_XA_INDOUBT_XIDS_RESP, new SessionXAGetInDoubtXidsResponseMessageCodec());

      addCodecForEmptyPacket(SESS_XA_GET_TIMEOUT);

      addCodec(SESS_XA_GET_TIMEOUT_RESP, new SessionXAGetTimeoutResponseMessageCodec());

      addCodec(SESS_XA_JOIN, new SessionXAJoinMessageCodec());

      addCodec(SESS_XA_PREPARE, new SessionXAPrepareMessageCodec());

      addCodec(SESS_XA_RESP, new SessionXAResponseMessageCodec());

      addCodec(SESS_XA_RESUME, new SessionXAResumeMessageCodec());

      addCodec(SESS_XA_ROLLBACK, new SessionXARollbackMessageCodec());

      addCodec(SESS_XA_SET_TIMEOUT, new SessionXASetTimeoutMessageCodec());

      addCodec(SESS_XA_SET_TIMEOUT_RESP, new SessionXASetTimeoutResponseMessageCodec());

      addCodec(SESS_XA_START, new SessionXAStartMessageCodec());

      addCodecForEmptyPacket(SESS_XA_SUSPEND);

      addCodec(SESS_REMOVE_DESTINATION, new SessionRemoveDestinationMessageCodec());

      addCodec(SESS_CREATEQUEUE, new SessionCreateQueueMessageCodec());

      addCodec(SESS_QUEUEQUERY,  new SessionQueueQueryMessageCodec());

      addCodec(SESS_QUEUEQUERY_RESP, new SessionQueueQueryResponseMessageCodec());

      addCodec(SESS_ADD_DESTINATION, new SessionAddDestinationMessageCodec());

      addCodec(SESS_BINDINGQUERY, new SessionBindingQueryMessageCodec());

      addCodec(SESS_BINDINGQUERY_RESP, new SessionBindingQueryResponseMessageCodec());

      addCodec(SESS_DELETE_QUEUE, new SessionDeleteQueueMessageCodec());

      addCodec(PROD_RECEIVETOKENS, new ProducerReceiveTokensMessageCodec());
   }
   
   public ProtocolDecoder getDecoder(IoSession session) throws Exception
	{
		return codec;
	}

	public ProtocolEncoder getEncoder(IoSession session) throws Exception
	{
		return codec;
	}

   // Public --------------------------------------------------------

   public static AbstractPacketCodec<?> createCodecForEmptyPacket(final PacketType type)
   {
      return new EmptyPacketCodec(type);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void addCodec(final PacketType type, final AbstractPacketCodec<? extends Packet> c)
   {
      codec.put(type, c);
   }
   
   private void addCodecForEmptyPacket(final PacketType type)
   {
      AbstractPacketCodec<?> c = createCodecForEmptyPacket(type);
      addCodec(type, c);
   }

}
