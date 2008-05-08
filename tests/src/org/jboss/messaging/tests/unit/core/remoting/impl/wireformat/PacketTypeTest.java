/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.HEADER_LENGTH;
import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.encodeXid;
import static org.jboss.messaging.core.remoting.impl.mina.PacketCodecFactory.createCodecForEmptyPacket;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.BYTES;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONN_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONN_STOP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONS_FLOWTOKEN;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CREATECONNECTION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.NULL;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PING;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PONG;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_RECEIVETOKENS;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.RECEIVE_MSG;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_ACKNOWLEDGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_ADD_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BINDINGQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BINDINGQUERY_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_HASNEXTMESSAGE_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_RESET;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CANCEL;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEBROWSER_RESP;
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
import static org.jboss.messaging.tests.unit.core.remoting.impl.wireformat.CodecAssert.assertEqualsByteArrays;
import static org.jboss.messaging.tests.unit.core.remoting.impl.wireformat.CodecAssert.assertSameXids;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomBytes;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import static org.jboss.messaging.tests.util.RandomUtil.randomXid;
import static org.jboss.messaging.util.DataConstants.SIZE_LONG;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.WriteFuture;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.BytesPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConnectionCreateSessionMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConnectionCreateSessionResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConsumerFlowTokenMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.CreateConnectionMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.CreateConnectionResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.PingCodec;
import org.jboss.messaging.core.remoting.impl.codec.PongCodec;
import org.jboss.messaging.core.remoting.impl.codec.ProducerReceiveTokensMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ProducerSendMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ReceiveMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionAcknowledgeMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionAddDestinationMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBindingQueryMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBindingQueryResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.SessionBrowserHasNextMessageResponseMessageCodec;
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
import org.jboss.messaging.core.remoting.impl.mina.BufferWrapper;
import org.jboss.messaging.core.remoting.impl.mina.PacketCodecFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.BytesPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
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
import org.jboss.messaging.core.remoting.impl.wireformat.TextPacket;
import org.jboss.messaging.core.version.impl.VersionImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * FIXME - tidy up tests so test names match the actual packets
 * 
 * @version <tt>$Revision$</tt>
 */
public class PacketTypeTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PacketTypeTest.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static MessagingBuffer encode(int length, Object... args)
         throws Exception
   {
      BufferWrapper buffer = new BufferWrapper(IoBuffer.allocate(length));
      for (Object arg : args)
      {
         if (arg instanceof Byte)
            buffer.putByte(((Byte) arg).byteValue());
         else if (arg instanceof Boolean)
         {
            Boolean bool = (Boolean) arg;
            buffer.putBoolean(bool);
         } else if (arg instanceof Integer)
            buffer.putInt(((Integer) arg).intValue());
         else if (arg instanceof Long)
            buffer.putLong(((Long) arg).longValue());
         else if (arg instanceof Float)
            buffer.putFloat(((Float) arg).floatValue());
         else if (arg instanceof String)
            buffer.putNullableString((String) arg);
         else if (arg instanceof SimpleString)
            buffer.putSimpleString((SimpleString)arg);
         else if (arg instanceof NullableStringHolder)
            buffer.putNullableSimpleString(((NullableStringHolder)arg).str);
         else if (arg == null)
            buffer.putNullableString(null);
         else if (arg instanceof byte[])
         {
            byte[] b = (byte[]) arg;
            buffer.putInt(b.length);
            buffer.putBytes(b);
         } else if (arg instanceof long[])
         {
            long[] longs = (long[]) arg;
            for (long l : longs)
            {
               buffer.putLong(l);
            }
         } else if (arg instanceof List)
         {
            List argsInList = (List) arg;
            buffer.putInt(argsInList.size());
            for (Object argInList : argsInList)
            {
               if (argInList instanceof SimpleString)
                  buffer.putSimpleString((SimpleString) argInList);
               else if (argInList instanceof Xid)
                  encodeXid((Xid)argInList, buffer);
               else
                  fail ("no encoding defined for " + arg + " in List");
            }
         } else if (arg instanceof Xid)
         {
            Xid xid = (Xid) arg;
            encodeXid(xid, buffer);
         } else
         {
            fail("no encoding defined for " + arg);
         }
      }
      buffer.flip();
      return buffer;
   }

   private static void checkHeader(final MessagingBuffer buffer,
         final Packet packet, final int bodyLength) throws Exception
   {
      buffer.rewind();
      int messageLength = buffer.getInt();
      assertEquals(AbstractPacketCodec.HEADER_LENGTH + bodyLength,
            messageLength);

      assertEquals(buffer.getByte(), packet.getType().byteValue());

      long responseTargetID = buffer.getLong();
      long targetID = buffer.getLong();
      long executorID = buffer.getLong();

      assertEquals(packet.getResponseTargetID(), responseTargetID);
      assertEquals(packet.getTargetID(), targetID);
      assertEquals(packet.getExecutorID(), executorID);
   }

   private static void checkBody(MessagingBuffer buffer, int bodyLength,
         Object... bodyObjects) throws Exception
   {
      byte[] actualBody = new byte[bodyLength];
      buffer.getBytes(actualBody);
      MessagingBuffer expectedBody = encode(actualBody.length, bodyObjects);
      assertEqualsByteArrays(expectedBody.array(), actualBody);
      // check the buffer has been wholly read
      assertEquals(0, buffer.remaining());
   }

   private static Packet encodeAndCheckBytesAndDecode(Packet packet,
         AbstractPacketCodec codec, Object... bodyObjects) throws Exception
   {
      MessagingBuffer buffer = encode(packet, codec);
      int bodyLength = buffer.position();
      checkHeader(buffer, packet, bodyLength);
      checkBody(buffer, bodyLength, bodyObjects);
      buffer.rewind();

      Packet decodedPacket = decode(buffer, codec, bodyLength);
      
      return decodedPacket;
   }

   private static MessagingBuffer encode(final Packet packet,
         final AbstractPacketCodec<Packet> codec) throws Exception
   {
      SimpleProtocolEncoderOutput out = new SimpleProtocolEncoderOutput();

      codec.encode(packet, out);

      Object encodedMessage = out.getEncodedMessage();

      assertNotNull(encodedMessage);

      assertTrue(encodedMessage instanceof IoBuffer);

      MessagingBuffer buff = new BufferWrapper((IoBuffer) encodedMessage);

      return buff;
   }

   private static Packet decode(final MessagingBuffer buffer,
         final AbstractPacketCodec<Packet> codec, final int len)
         throws Exception
   {
      SimpleProtocolDencoderOutput out = new SimpleProtocolDencoderOutput();

      int length = buffer.getInt();

      assertEquals(len + HEADER_LENGTH, length);

      byte type = buffer.getByte();

      assertEquals(codec.getType().byteValue(), type);

      codec.decode(buffer, out);

      Object message = out.getMessage();

      assertNotNull(message);

      assertTrue(message instanceof Packet);

      return (Packet) message;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
//
//   public void testNullPacket() throws Exception
//   {
//      Packet packet = new PacketImpl(NULL);
//      long cid = randomLong();
//      packet.setResponseTargetID(cid);
//      packet.setTargetID(randomLong());
//      packet.setExecutorID(randomLong());
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(NULL);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet, codec);
//      assertTrue(decodedPacket instanceof PacketImpl);
//
//      assertEquals(NULL, decodedPacket.getType());
//      assertEquals(packet.getResponseTargetID(), decodedPacket.getResponseTargetID());
//      assertEquals(packet.getTargetID(), decodedPacket.getTargetID());
//      assertEquals(packet.getExecutorID(), decodedPacket.getExecutorID());
//   }
//
//   public void testPing() throws Exception
//   {
//      Ping ping = new Ping(randomLong());
//      AbstractPacketCodec<Ping> codec = new PingCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(ping, codec, ping
//            .getSessionID());
//
//      assertTrue(decodedPacket instanceof Ping);
//      Ping decodedPing = (Ping) decodedPacket;
//      assertEquals(PING, decodedPing.getType());
//      assertEquals(ping.getResponseTargetID(), decodedPacket.getResponseTargetID());
//      assertEquals(ping.getTargetID(), decodedPacket.getTargetID());
//      assertEquals(ping.getExecutorID(), decodedPacket.getExecutorID());
//   }
//
//   public void testPong() throws Exception
//   {
//      Pong pong = new Pong(randomLong(), true);
//      AbstractPacketCodec<Pong> codec = new PongCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(pong, codec, pong
//            .getSessionID(), pong.isSessionFailed());
//
//      assertTrue(decodedPacket instanceof Pong);
//      Pong decodedPong = (Pong) decodedPacket;
//      assertEquals(PONG, decodedPong.getType());
//      assertEquals(pong.getSessionID(), decodedPong.getSessionID());
//      assertEquals(pong.isSessionFailed(), decodedPong.isSessionFailed());
//   }
//
//   public void testTextPacket() throws Exception
//   {
//      TextPacket packet = new TextPacket("testTextPacket");
//      AbstractPacketCodec<TextPacket> codec = new TextPacketCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet, codec, packet
//            .getText());
//
//      assertTrue(decodedPacket instanceof TextPacket);
//      TextPacket p = (TextPacket) decodedPacket;
//
//      assertEquals(TEXT, p.getType());
//      assertEquals(packet.getText(), p.getText());
//   }
//
//   public void testBytesPacket() throws Exception
//   {
//      BytesPacket packet = new BytesPacket(randomBytes());
//      AbstractPacketCodec<BytesPacket> codec = new BytesPacketCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet, codec, packet
//            .getBytes());
//
//      assertTrue(decodedPacket instanceof BytesPacket);
//      BytesPacket p = (BytesPacket) decodedPacket;
//
//      assertEquals(BYTES, p.getType());
//      assertEqualsByteArrays(packet.getBytes(), p.getBytes());
//   }
//
//   public void testCreateConnectionRequest() throws Exception
//   {
//      int version = randomInt();
//      long remotingSessionID = randomLong();
//      String username = null;
//      String password = null;
//      CreateConnectionRequest request = new CreateConnectionRequest(version,
//            remotingSessionID, username, password);
//      AbstractPacketCodec<CreateConnectionRequest> codec = new CreateConnectionMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, codec,
//            version, remotingSessionID, username, password);
//
//      assertTrue(decodedPacket instanceof CreateConnectionRequest);
//      CreateConnectionRequest decodedRequest = (CreateConnectionRequest) decodedPacket;
//
//      assertEquals(CREATECONNECTION, decodedPacket.getType());
//      assertEquals(request.getVersion(), decodedRequest.getVersion());
//      assertEquals(request.getRemotingSessionID(), decodedRequest
//            .getRemotingSessionID());
//      assertEquals(request.getUsername(), decodedRequest.getUsername());
//      assertEquals(request.getPassword(), decodedRequest.getPassword());
//   }
//
//   public void testCreateConnectionResponse() throws Exception
//   {
//      CreateConnectionResponse response = new CreateConnectionResponse(
//            randomLong(), new VersionImpl("test", 1,2,3,4,"xxx"));
//      AbstractPacketCodec<CreateConnectionResponse> codec = new CreateConnectionResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, codec,
//            response.getConnectionTargetID(),
//              response.getServerVersion().getVersionName(),
//              response.getServerVersion().getMajorVersion(),
//              response.getServerVersion().getMinorVersion(),
//              response.getServerVersion().getMicroVersion(),
//              response.getServerVersion().getIncrementingVersion(),
//              response.getServerVersion().getVersionSuffix());
//
//      assertTrue(decodedPacket instanceof CreateConnectionResponse);
//      CreateConnectionResponse decodedResponse = (CreateConnectionResponse) decodedPacket;
//      assertEquals(PacketType.CREATECONNECTION_RESP, decodedResponse.getType());
//      assertEquals(response.getConnectionTargetID(), decodedResponse
//            .getConnectionTargetID());
//      assertEquals(response.getServerVersion().getFullVersion(), decodedResponse.getServerVersion().getFullVersion());
//   }
//
//   public void testConnectionCreateSessionMessage() throws Exception
//   {
//      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(
//            randomBoolean(), randomBoolean(), randomBoolean());
//      AbstractPacketCodec<ConnectionCreateSessionMessage> codec = new ConnectionCreateSessionMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, codec,
//            request.isXA(), request.isAutoCommitSends(), request
//                  .isAutoCommitAcks());
//
//      assertTrue(decodedPacket instanceof ConnectionCreateSessionMessage);
//      ConnectionCreateSessionMessage decodedRequest = (ConnectionCreateSessionMessage) decodedPacket;
//      assertEquals(PacketType.CONN_CREATESESSION, decodedRequest.getType());
//      assertEquals(request.isXA(), decodedRequest.isXA());
//      assertEquals(request.isAutoCommitSends(), decodedRequest
//            .isAutoCommitSends());
//      assertEquals(request.isAutoCommitAcks(), decodedRequest
//            .isAutoCommitAcks());
//   }
//
//   public void testConnectionCreateSessionResponseMessage() throws Exception
//   {
//      ConnectionCreateSessionResponseMessage response = new ConnectionCreateSessionResponseMessage(
//            randomLong());
//      AbstractPacketCodec<ConnectionCreateSessionResponseMessage> codec = new ConnectionCreateSessionResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, codec,
//            response.getSessionID());
//
//      assertTrue(decodedPacket instanceof ConnectionCreateSessionResponseMessage);
//      ConnectionCreateSessionResponseMessage decodedResponse = (ConnectionCreateSessionResponseMessage) decodedPacket;
//      assertEquals(PacketType.CONN_CREATESESSION_RESP, decodedResponse
//            .getType());
//      assertEquals(response.getSessionID(), decodedResponse.getSessionID());
//   }
//
//   public void testProducerSendMessage() throws Exception
//   {
//      Message msg = new MessageImpl((byte)1, false, 1212212L, 761276712L, (byte)1);
//      msg.setDestination(new SimpleString("blah"));
//      ProducerSendMessage packet = new ProducerSendMessage(msg);
//      MessagingBuffer buff = packet.getMessage().encode();
//      
//      Message msg2 = new MessageImpl();
//      msg2.decode(buff);
//      
//      
//      byte[] messageBytes = buff.array();
//      byte[] data = new byte[buff.limit()];
//      System.arraycopy(messageBytes, 0, data, 0, buff.limit());
//      AbstractPacketCodec codec = new ProducerSendMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet, codec, data);
//
//      assertTrue(decodedPacket instanceof ProducerSendMessage);
//      ProducerSendMessage decodedMessage = (ProducerSendMessage) decodedPacket;
//      assertEquals(PacketType.PROD_SEND, decodedPacket.getType());
//      assertEquals(packet.getMessage().getMessageID(), decodedMessage
//            .getMessage().getMessageID());
//   }
//
//   public void testSessionCreateConsumerMessage() throws Exception
//   {
//      SimpleString destination = new SimpleString("queue.SessionCreateConsumerMessage");
//      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(randomLong(),
//            destination, new SimpleString("color = 'red'"), false, false, randomInt(),
//            randomInt());
//      AbstractPacketCodec codec = new SessionCreateConsumerMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, codec, request.getClientTargetID(),
//            request.getQueueName(), new NullableStringHolder(request.getFilterString()), request
//                  .isNoLocal(), request.isAutoDeleteQueue(), request
//                  .getWindowSize(), request.getMaxRate());
//
//      assertTrue(decodedPacket instanceof SessionCreateConsumerMessage);
//      SessionCreateConsumerMessage decodedRequest = (SessionCreateConsumerMessage) decodedPacket;
//      assertEquals(PacketType.SESS_CREATECONSUMER, decodedRequest.getType());
//      assertEquals(request.getClientTargetID(), decodedRequest.getClientTargetID());
//      assertEquals(request.getQueueName(), decodedRequest.getQueueName());
//      assertEquals(request.getFilterString(), decodedRequest.getFilterString());
//      assertEquals(request.isNoLocal(), decodedRequest.isNoLocal());
//      assertEquals(request.isAutoDeleteQueue(), decodedRequest
//            .isAutoDeleteQueue());
//      assertEquals(request.getWindowSize(), decodedRequest.getWindowSize());
//      assertEquals(request.getMaxRate(), decodedRequest.getMaxRate());
//   }
//   
//  
//
//   public void testSessionCreateConsumerResponseMessage() throws Exception
//   {
//      SessionCreateConsumerResponseMessage response = new SessionCreateConsumerResponseMessage(
//            randomLong(), randomInt());
//      AbstractPacketCodec codec = new SessionCreateConsumerResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, codec,
//            response.getConsumerTargetID(), response.getWindowSize());
//
//      assertTrue(decodedPacket instanceof SessionCreateConsumerResponseMessage);
//      SessionCreateConsumerResponseMessage decodedResponse = (SessionCreateConsumerResponseMessage) decodedPacket;
//      assertEquals(SESS_CREATECONSUMER_RESP, decodedResponse.getType());
//
//      assertEquals(response.getConsumerTargetID(), decodedResponse
//            .getConsumerTargetID());
//      assertEquals(response.getWindowSize(), decodedResponse.getWindowSize());
//   }
//
//   public void testSessionCreateProducerMessage() throws Exception
//   {
//      SimpleString destination = new SimpleString("queue.testSessionCreateProducerMessage");
//      int windowSize = randomInt();
//      int maxRate = randomInt();
//      SessionCreateProducerMessage request = new SessionCreateProducerMessage(randomLong(),
//            destination, windowSize, maxRate);
//      AbstractPacketCodec codec = new SessionCreateProducerMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, codec, request.getClientTargetID(),
//            new NullableStringHolder(request.getAddress()), request.getWindowSize(), request.getMaxRate());
//
//      assertTrue(decodedPacket instanceof SessionCreateProducerMessage);
//      SessionCreateProducerMessage decodedRequest = (SessionCreateProducerMessage) decodedPacket;
//      assertEquals(SESS_CREATEPRODUCER, decodedRequest.getType());
//      assertEquals(request.getClientTargetID(), decodedRequest.getClientTargetID());
//      assertEquals(request.getAddress(), decodedRequest.getAddress());
//      assertEquals(request.getWindowSize(), decodedRequest.getWindowSize());
//      assertEquals(request.getMaxRate(), decodedRequest.getMaxRate());
//   }
//
//   public void testSessionCreateProducerResponseMessage() throws Exception
//   {
//      SessionCreateProducerResponseMessage response = new SessionCreateProducerResponseMessage(
//            randomLong(), randomInt(), randomInt());
//      AbstractPacketCodec codec = new SessionCreateProducerResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, codec,
//            response.getProducerTargetID(), response.getWindowSize(), response
//                  .getMaxRate());
//
//      assertTrue(decodedPacket instanceof SessionCreateProducerResponseMessage);
//      SessionCreateProducerResponseMessage decodedResponse = (SessionCreateProducerResponseMessage) decodedPacket;
//      assertEquals(SESS_CREATEPRODUCER_RESP, decodedResponse.getType());
//      assertEquals(response.getProducerTargetID(), decodedResponse
//            .getProducerTargetID());
//      assertEquals(response.getWindowSize(), decodedResponse.getWindowSize());
//      assertEquals(response.getMaxRate(), decodedResponse.getMaxRate());
//   }
//
//   public void testStartConnectionMessage() throws Exception
//   {
//      Packet packet = new PacketImpl(CONN_START);
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(CONN_START);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet, codec);
//
//      assertEquals(CONN_START, decodedPacket.getType());
//   }
//
//   public void testStopConnectionMessage() throws Exception
//   {
//      Packet packet = new PacketImpl(CONN_STOP);
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(CONN_STOP);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet, codec);
//
//      assertEquals(CONN_STOP, decodedPacket.getType());
//   }
//
//   public void testConsumerFlowTokenMessage() throws Exception
//   {
//      ConsumerFlowTokenMessage message = new ConsumerFlowTokenMessage(
//            randomInt());
//      AbstractPacketCodec codec = new ConsumerFlowTokenMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getTokens());
//
//      assertTrue(decodedPacket instanceof ConsumerFlowTokenMessage);
//      ConsumerFlowTokenMessage decodedMessage = (ConsumerFlowTokenMessage) decodedPacket;
//      assertEquals(CONS_FLOWTOKEN, decodedMessage.getType());
//      assertEquals(message.getTokens(), decodedMessage.getTokens());
//   }
//
//   public void testProducerReceiveTokensMessage() throws Exception
//   {
//      ProducerReceiveTokensMessage message = new ProducerReceiveTokensMessage(
//            randomInt());
//      AbstractPacketCodec codec = new ProducerReceiveTokensMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getTokens());
//
//      assertTrue(decodedPacket instanceof ProducerReceiveTokensMessage);
//      ProducerReceiveTokensMessage decodedMessage = (ProducerReceiveTokensMessage) decodedPacket;
//      assertEquals(PROD_RECEIVETOKENS, decodedMessage.getType());
//      assertEquals(message.getTokens(), decodedMessage.getTokens());
//   }
//
//   public void testReceiveMessage() throws Exception
//   {
//      Message msg = new MessageImpl();
//      ReceiveMessage message = new ReceiveMessage(msg);
//      AbstractPacketCodec codec = new ReceiveMessageCodec();
//      
//      byte[] messageBytes = message.getMessage().encode().array();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec, messageBytes);
//
//      assertTrue(decodedPacket instanceof ReceiveMessage);
//      ReceiveMessage decodedMessage = (ReceiveMessage) decodedPacket;
//      assertEquals(RECEIVE_MSG, decodedMessage.getType());
//      assertEquals(message.getMessage().getMessageID(), decodedMessage
//            .getMessage().getMessageID());
//   }
//
//   public void testSessionAcknowledgeMessage() throws Exception
//   {
//      SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(
//            randomLong(), randomBoolean());
//      AbstractPacketCodec codec = new SessionAcknowledgeMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getDeliveryID(), message.isAllUpTo());
//
//      assertTrue(decodedPacket instanceof SessionAcknowledgeMessage);
//      SessionAcknowledgeMessage decodedMessage = (SessionAcknowledgeMessage) decodedPacket;
//      assertEquals(SESS_ACKNOWLEDGE, decodedMessage.getType());
//      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
//      assertEquals(message.isAllUpTo(), decodedMessage.isAllUpTo());
//   }
//
//   public void testSessionCancelMessage() throws Exception
//   {
//      SessionCancelMessage message = new SessionCancelMessage(randomLong(),
//            randomBoolean());
//      AbstractPacketCodec codec = new SessionCancelMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getDeliveryID(), message.isExpired());
//
//      assertTrue(decodedPacket instanceof SessionCancelMessage);
//      SessionCancelMessage decodedMessage = (SessionCancelMessage) decodedPacket;
//      assertEquals(SESS_CANCEL, decodedMessage.getType());
//      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
//      assertEquals(message.isExpired(), decodedMessage.isExpired());
//   }
//
//   public void testSessionCommitMessage() throws Exception
//   {
//      Packet message = new PacketImpl(SESS_COMMIT);
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(SESS_COMMIT);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec);
//
//      assertEquals(SESS_COMMIT, decodedPacket.getType());
//   }
//
//   public void testSessionRollbackMessage() throws Exception
//   {
//      Packet message = new PacketImpl(SESS_ROLLBACK);
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(SESS_ROLLBACK);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec);
//
//      assertEquals(SESS_ROLLBACK, decodedPacket.getType());
//   }
//
//   public void testSessionRecoverMessage() throws Exception
//   {
//      Packet message = new PacketImpl(SESS_RECOVER);
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(SESS_RECOVER);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec);
//
//      assertEquals(SESS_RECOVER, decodedPacket.getType());
//   }
//
//   public void testCloseMessage() throws Exception
//   {
//      Packet message = new PacketImpl(CLOSE);
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(CLOSE);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec);
//
//      assertEquals(CLOSE, decodedPacket.getType());
//   }
//
//   public void testSessionCreateBrowserMessage() throws Exception
//   {
//      SimpleString destination = new SimpleString("queue.testCreateBrowserRequest");
//      SessionCreateBrowserMessage request = new SessionCreateBrowserMessage(
//            destination, new SimpleString("color = 'red'"));
//      AbstractPacketCodec codec = new SessionCreateBrowserMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, codec,
//            request.getQueueName(), new NullableStringHolder(request.getFilterString()));
//
//      assertTrue(decodedPacket instanceof SessionCreateBrowserMessage);
//      SessionCreateBrowserMessage decodedRequest = (SessionCreateBrowserMessage) decodedPacket;
//      assertEquals(SESS_CREATEBROWSER, decodedRequest.getType());
//      assertEquals(request.getQueueName(), decodedRequest.getQueueName());
//      assertEquals(request.getFilterString(), decodedRequest.getFilterString());
//   }
//
//   public void testSessionCreateBrowserResponseMessage() throws Exception
//   {
//      SessionCreateBrowserResponseMessage response = new SessionCreateBrowserResponseMessage(
//            randomLong());
//      AbstractPacketCodec codec = new SessionCreateBrowserResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, codec,
//            response.getBrowserTargetID());
//
//      assertTrue(decodedPacket instanceof SessionCreateBrowserResponseMessage);
//      SessionCreateBrowserResponseMessage decodedResponse = (SessionCreateBrowserResponseMessage) decodedPacket;
//      assertEquals(SESS_CREATEBROWSER_RESP, decodedResponse.getType());
//      assertEquals(response.getBrowserTargetID(), decodedResponse
//            .getBrowserTargetID());
//   }
//
//   public void testBrowserResetMessage() throws Exception
//   {
//      Packet message = new PacketImpl(SESS_BROWSER_RESET);
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(SESS_BROWSER_RESET);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec);
//
//      assertEquals(SESS_BROWSER_RESET, decodedPacket.getType());
//   }
//
//   public void testBrowserHasNextMessageRequest() throws Exception
//   {
//      Packet request = new PacketImpl(SESS_BROWSER_HASNEXTMESSAGE);
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(SESS_BROWSER_HASNEXTMESSAGE);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, codec);
//
//      assertEquals(SESS_BROWSER_HASNEXTMESSAGE, decodedPacket.getType());
//   }
//
//   public void testSessionBrowserHasNextMessageResponseMessage()
//         throws Exception
//   {
//      SessionBrowserHasNextMessageResponseMessage response = new SessionBrowserHasNextMessageResponseMessage(
//            randomBoolean());
//      AbstractPacketCodec codec = new SessionBrowserHasNextMessageResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, codec,
//            response.hasNext());
//
//      assertTrue(decodedPacket instanceof SessionBrowserHasNextMessageResponseMessage);
//      SessionBrowserHasNextMessageResponseMessage decodedResponse = (SessionBrowserHasNextMessageResponseMessage) decodedPacket;
//      assertEquals(SESS_BROWSER_HASNEXTMESSAGE_RESP, decodedResponse.getType());
//      assertEquals(response.hasNext(), decodedResponse.hasNext());
//   }
//
//   public void testBrowserNextMessageRequest() throws Exception
//   {
//      Packet request = new PacketImpl(SESS_BROWSER_NEXTMESSAGE);
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(SESS_BROWSER_NEXTMESSAGE);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, codec);
//
//      assertEquals(SESS_BROWSER_NEXTMESSAGE, decodedPacket.getType());
//   }
//
//   public void testSessionXACommitMessage() throws Exception
//   {
//      SessionXACommitMessage message = new SessionXACommitMessage(randomXid(),
//            randomBoolean());
//      AbstractPacketCodec codec = new SessionXACommitMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getXid(), message.isOnePhase());
//
//      assertTrue(decodedPacket instanceof SessionXACommitMessage);
//      SessionXACommitMessage decodedMessage = (SessionXACommitMessage) decodedPacket;
//      assertEquals(SESS_XA_COMMIT, decodedMessage.getType());
//      assertEquals(message.getXid(), decodedMessage.getXid());
//      assertEquals(message.isOnePhase(), decodedMessage.isOnePhase());
//   }
//
//   public void testSessionXAEndMessage() throws Exception
//   {
//      SessionXAEndMessage message = new SessionXAEndMessage(randomXid(),
//            randomBoolean());
//      AbstractPacketCodec codec = new SessionXAEndMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getXid(), message.isFailed());
//
//      assertTrue(decodedPacket instanceof SessionXAEndMessage);
//      SessionXAEndMessage decodedMessage = (SessionXAEndMessage) decodedPacket;
//      assertEquals(SESS_XA_END, decodedMessage.getType());
//      assertEquals(message.getXid(), decodedMessage.getXid());
//      assertEquals(message.isFailed(), decodedMessage.isFailed());
//   }
//
//   public void testSessionXAForgetMessage() throws Exception
//   {
//      SessionXAForgetMessage message = new SessionXAForgetMessage(randomXid());
//      AbstractPacketCodec codec = new SessionXAForgetMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getXid());
//
//      assertTrue(decodedPacket instanceof SessionXAForgetMessage);
//      SessionXAForgetMessage decodedMessage = (SessionXAForgetMessage) decodedPacket;
//      assertEquals(SESS_XA_FORGET, decodedMessage.getType());
//      assertEquals(message.getXid(), decodedMessage.getXid());
//   }
//
//   public void testSessionXAGetInDoubtXidsMessage() throws Exception
//   {
//      Packet request = new PacketImpl(SESS_XA_INDOUBT_XIDS);
//      AbstractPacketCodec codec = createCodecForEmptyPacket(SESS_XA_INDOUBT_XIDS);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, codec);
//
//      assertEquals(SESS_XA_INDOUBT_XIDS, decodedPacket.getType());
//   }
//
//   public void testSessionXAGetInDoubtXidsResponseMessage() throws Exception
//   {
//      final int numXids = 10;
//      List<Xid> xids = new ArrayList<Xid>();
//      for (int i = 0; i < numXids; i++)
//      {
//         xids.add(randomXid());
//      }
//      SessionXAGetInDoubtXidsResponseMessage message = new SessionXAGetInDoubtXidsResponseMessage(
//            xids);
//      AbstractPacketCodec codec = new SessionXAGetInDoubtXidsResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec, xids);
//
//      assertTrue(decodedPacket instanceof SessionXAGetInDoubtXidsResponseMessage);
//      SessionXAGetInDoubtXidsResponseMessage decodedMessage = (SessionXAGetInDoubtXidsResponseMessage) decodedPacket;
//      assertEquals(SESS_XA_INDOUBT_XIDS_RESP, decodedMessage.getType());
//
//      assertSameXids(message.getXids(), decodedMessage.getXids());
//   }
//
//   public void testSessionXAGetTimeoutMessage() throws Exception
//   {
//      Packet message = new PacketImpl(SESS_XA_GET_TIMEOUT);
//      AbstractPacketCodec codec = createCodecForEmptyPacket(PacketType.SESS_XA_GET_TIMEOUT);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec);
//
//      assertEquals(SESS_XA_GET_TIMEOUT, decodedPacket.getType());
//   }
//
//   public void testSessionXAGetTimeoutResponseMessage() throws Exception
//   {
//      SessionXAGetTimeoutResponseMessage message = new SessionXAGetTimeoutResponseMessage(
//            randomInt());
//      AbstractPacketCodec codec = new SessionXAGetTimeoutResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getTimeoutSeconds());
//
//      assertTrue(decodedPacket instanceof SessionXAGetTimeoutResponseMessage);
//      SessionXAGetTimeoutResponseMessage decodedMessage = (SessionXAGetTimeoutResponseMessage) decodedPacket;
//      assertEquals(SESS_XA_GET_TIMEOUT_RESP, decodedMessage.getType());
//      assertEquals(message.getTimeoutSeconds(), decodedMessage
//            .getTimeoutSeconds());
//   }
//
//   public void testSessionXAJoinMessage() throws Exception
//   {
//      SessionXAJoinMessage message = new SessionXAJoinMessage(randomXid());
//      AbstractPacketCodec codec = new SessionXAJoinMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getXid());
//
//      assertTrue(decodedPacket instanceof SessionXAJoinMessage);
//      SessionXAJoinMessage decodedMessage = (SessionXAJoinMessage) decodedPacket;
//      assertEquals(SESS_XA_JOIN, decodedMessage.getType());
//      assertEquals(message.getXid(), decodedMessage.getXid());
//   }
//
//   public void testSessionXAPrepareMessage() throws Exception
//   {
//      SessionXAPrepareMessage message = new SessionXAPrepareMessage(randomXid());
//      AbstractPacketCodec codec = new SessionXAPrepareMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getXid());
//
//      assertTrue(decodedPacket instanceof SessionXAPrepareMessage);
//      SessionXAPrepareMessage decodedMessage = (SessionXAPrepareMessage) decodedPacket;
//      assertEquals(SESS_XA_PREPARE, decodedMessage.getType());
//      assertEquals(message.getXid(), decodedMessage.getXid());
//   }
//
//   public void testSessionXAResponseMessage() throws Exception
//   {
//      SessionXAResponseMessage message = new SessionXAResponseMessage(
//            randomBoolean(), randomInt(), randomString());
//      AbstractPacketCodec codec = new SessionXAResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.isError(), message.getResponseCode(), message.getMessage());
//
//      assertTrue(decodedPacket instanceof SessionXAResponseMessage);
//      SessionXAResponseMessage decodedMessage = (SessionXAResponseMessage) decodedPacket;
//      assertEquals(SESS_XA_RESP, decodedMessage.getType());
//      assertEquals(message.isError(), decodedMessage.isError());
//      assertEquals(message.getResponseCode(), decodedMessage.getResponseCode());
//      assertEquals(message.getMessage(), decodedMessage.getMessage());
//   }
//
//   public void testSessionXAResumeMessage() throws Exception
//   {
//      SessionXAResumeMessage message = new SessionXAResumeMessage(randomXid());
//      AbstractPacketCodec codec = new SessionXAResumeMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getXid());
//
//      assertTrue(decodedPacket instanceof SessionXAResumeMessage);
//      SessionXAResumeMessage decodedMessage = (SessionXAResumeMessage) decodedPacket;
//      assertEquals(SESS_XA_RESUME, decodedMessage.getType());
//      assertEquals(message.getXid(), decodedMessage.getXid());
//   }
//
//   public void testSessionXARollbackMessage() throws Exception
//   {
//      SessionXARollbackMessage message = new SessionXARollbackMessage(
//            randomXid());
//      AbstractPacketCodec codec = new SessionXARollbackMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getXid());
//
//      assertTrue(decodedPacket instanceof SessionXARollbackMessage);
//      SessionXARollbackMessage decodedMessage = (SessionXARollbackMessage) decodedPacket;
//      assertEquals(SESS_XA_ROLLBACK, decodedMessage.getType());
//      assertEquals(message.getXid(), decodedMessage.getXid());
//   }
//
//   public void testSessionXASetTimeoutMessage() throws Exception
//   {
//      SessionXASetTimeoutMessage message = new SessionXASetTimeoutMessage(
//            randomInt());
//      AbstractPacketCodec codec = new SessionXASetTimeoutMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getTimeoutSeconds());
//
//      assertTrue(decodedPacket instanceof SessionXASetTimeoutMessage);
//      SessionXASetTimeoutMessage decodedMessage = (SessionXASetTimeoutMessage) decodedPacket;
//      assertEquals(SESS_XA_SET_TIMEOUT, decodedMessage.getType());
//      assertEquals(message.getTimeoutSeconds(), decodedMessage
//            .getTimeoutSeconds());
//   }
//
//   public void testSessionXASetTimeoutResponseMessage() throws Exception
//   {
//      SessionXASetTimeoutResponseMessage message = new SessionXASetTimeoutResponseMessage(
//            randomBoolean());
//      AbstractPacketCodec codec = new SessionXASetTimeoutResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.isOK());
//
//      assertTrue(decodedPacket instanceof SessionXASetTimeoutResponseMessage);
//      SessionXASetTimeoutResponseMessage decodedMessage = (SessionXASetTimeoutResponseMessage) decodedPacket;
//      assertEquals(SESS_XA_SET_TIMEOUT_RESP, decodedMessage.getType());
//      assertEquals(message.isOK(), decodedMessage.isOK());
//   }
//
//   public void testSessionXAStartMessage() throws Exception
//   {
//      SessionXAStartMessage message = new SessionXAStartMessage(randomXid());
//      AbstractPacketCodec codec = new SessionXAStartMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getXid());
//
//      assertTrue(decodedPacket instanceof SessionXAStartMessage);
//      SessionXAStartMessage decodedMessage = (SessionXAStartMessage) decodedPacket;
//      assertEquals(SESS_XA_START, decodedMessage.getType());
//      assertEquals(message.getXid(), decodedMessage.getXid());
//   }
//
//   public void testSessionXASuspendMessage() throws Exception
//   {
//      Packet message = new PacketImpl(SESS_XA_SUSPEND);
//      AbstractPacketCodec codec = PacketCodecFactory
//            .createCodecForEmptyPacket(PacketType.SESS_XA_SUSPEND);
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec);
//
//      assertEquals(SESS_XA_SUSPEND, decodedPacket.getType());
//   }
//
//   public void testSessionRemoveDestinationMessage() throws Exception
//   {
//      SessionRemoveDestinationMessage message = new SessionRemoveDestinationMessage(
//            new SimpleString(randomString()), randomBoolean());
//      AbstractPacketCodec codec = new SessionRemoveDestinationMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getAddress(), message.isTemporary());
//
//      assertTrue(decodedPacket instanceof SessionRemoveDestinationMessage);
//      SessionRemoveDestinationMessage decodedMessage = (SessionRemoveDestinationMessage) decodedPacket;
//      assertEquals(SESS_REMOVE_DESTINATION, decodedMessage.getType());
//      assertEquals(message.getAddress(), decodedMessage.getAddress());
//      assertEquals(message.isTemporary(), decodedMessage.isTemporary());
//   }
//
//   public void testSessionCreateQueueMessage() throws Exception
//   {
//      SessionCreateQueueMessage message = new SessionCreateQueueMessage(
//            new SimpleString(randomString()), new SimpleString(randomString()),
//            new SimpleString(randomString()), randomBoolean(),
//            randomBoolean());
//      AbstractPacketCodec codec = new SessionCreateQueueMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getAddress(), message.getQueueName(), new NullableStringHolder(message.getFilterString()), message.isDurable(), message
//                  .isTemporary());
//
//      assertTrue(decodedPacket instanceof SessionCreateQueueMessage);
//      SessionCreateQueueMessage decodedMessage = (SessionCreateQueueMessage) decodedPacket;
//      assertEquals(SESS_CREATEQUEUE, decodedMessage.getType());
//
//      assertEquals(message.getAddress(), decodedMessage.getAddress());
//      assertEquals(message.getQueueName(), decodedMessage.getQueueName());
//      assertEquals(message.getFilterString(), decodedMessage.getFilterString());
//      assertEquals(message.isDurable(), decodedMessage.isDurable());
//      assertEquals(message.isTemporary(), decodedMessage.isTemporary());
//
//   }
//
//   public void testSessionQueueQueryMessage() throws Exception
//   {
//      SessionQueueQueryMessage message = new SessionQueueQueryMessage(
//            new SimpleString(randomString()));
//      AbstractPacketCodec codec = new SessionQueueQueryMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getQueueName());
//
//      assertTrue(decodedPacket instanceof SessionQueueQueryMessage);
//      SessionQueueQueryMessage decodedMessage = (SessionQueueQueryMessage) decodedPacket;
//      assertEquals(SESS_QUEUEQUERY, decodedMessage.getType());
//      assertEquals(message.getQueueName(), decodedMessage.getQueueName());
//   }
//
//   public void testSessionQueueQueryResponseMessage() throws Exception
//   {
//      SessionQueueQueryResponseMessage message = new SessionQueueQueryResponseMessage(
//            randomBoolean(), randomBoolean(), randomInt(), randomInt(),
//            randomInt(), new SimpleString(randomString()), new SimpleString(randomString()));
//      AbstractPacketCodec codec = new SessionQueueQueryResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.isExists(), message.isDurable(), message.isTemporary(),
//            message.getMaxSize(), message.getConsumerCount(), message
//                  .getMessageCount(), new NullableStringHolder(message.getFilterString()),
//                  new NullableStringHolder(message.getAddress()));
//
//      assertTrue(decodedPacket instanceof SessionQueueQueryResponseMessage);
//      SessionQueueQueryResponseMessage decodedMessage = (SessionQueueQueryResponseMessage) decodedPacket;
//      assertEquals(SESS_QUEUEQUERY_RESP, decodedMessage.getType());
//
//      assertEquals(message.isExists(), decodedMessage.isExists());
//      assertEquals(message.isDurable(), decodedMessage.isDurable());
//      assertEquals(message.isTemporary(), decodedMessage.isTemporary());
//      assertEquals(message.getConsumerCount(), decodedMessage
//            .getConsumerCount());
//      assertEquals(message.getMessageCount(), decodedMessage.getMessageCount());
//      assertEquals(message.getFilterString(), decodedMessage.getFilterString());
//      assertEquals(message.getAddress(), decodedMessage.getAddress());
//   }
//
//   public void testSessionAddAddressMessage() throws Exception
//   {
//      SessionAddDestinationMessage message = new SessionAddDestinationMessage(
//            new SimpleString(randomString()), randomBoolean());
//      AbstractPacketCodec<SessionAddDestinationMessage> codec = new SessionAddDestinationMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getAddress(), message.isTemporary());
//
//      assertTrue(decodedPacket instanceof SessionAddDestinationMessage);
//      SessionAddDestinationMessage decodedMessage = (SessionAddDestinationMessage) decodedPacket;
//      assertEquals(SESS_ADD_DESTINATION, decodedMessage.getType());
//      assertEquals(message.getAddress(), decodedMessage.getAddress());
//      assertEquals(message.isTemporary(), decodedMessage.isTemporary());
//   }
//
//   public void testSessionBindingQueryMessage() throws Exception
//   {
//      SessionBindingQueryMessage message = new SessionBindingQueryMessage(
//            new SimpleString(randomString()));
//      AbstractPacketCodec codec = new SessionBindingQueryMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getAddress());
//
//      assertTrue(decodedPacket instanceof SessionBindingQueryMessage);
//      SessionBindingQueryMessage decodedMessage = (SessionBindingQueryMessage) decodedPacket;
//      assertEquals(SESS_BINDINGQUERY, decodedMessage.getType());
//
//      assertEquals(message.getAddress(), decodedMessage.getAddress());
//   }
//
//   public void testSessionBindingQueryResponseMessage() throws Exception
//   {
//      boolean exists = true;
//      List<SimpleString> queueNames = new ArrayList<SimpleString>();
//      queueNames.add(new SimpleString(randomString()));
//      queueNames.add(new SimpleString(randomString()));
//      queueNames.add(new SimpleString(randomString()));
//      SessionBindingQueryResponseMessage message = new SessionBindingQueryResponseMessage(
//            exists, queueNames);
//      AbstractPacketCodec codec = new SessionBindingQueryResponseMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.isExists(), message.getQueueNames());
//
//      assertTrue(decodedPacket instanceof SessionBindingQueryResponseMessage);
//      SessionBindingQueryResponseMessage decodedMessage = (SessionBindingQueryResponseMessage) decodedPacket;
//      assertEquals(SESS_BINDINGQUERY_RESP, decodedMessage.getType());
//      assertEquals(message.isExists(), decodedMessage.isExists());
//
//      List<SimpleString> decodedNames = decodedMessage.getQueueNames();
//      assertEquals(queueNames.size(), decodedNames.size());
//      for (int i = 0; i < queueNames.size(); i++)
//      {
//         assertEquals(queueNames.get(i), decodedNames.get(i));
//      }
//   }
//
//   public void testDeleteQueueRequest() throws Exception
//   {
//      SessionDeleteQueueMessage message = new SessionDeleteQueueMessage(
//            new SimpleString(randomString()));
//      AbstractPacketCodec codec = new SessionDeleteQueueMessageCodec();
//
//      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, codec,
//            message.getQueueName());
//
//      assertTrue(decodedPacket instanceof SessionDeleteQueueMessage);
//      SessionDeleteQueueMessage decodedMessage = (SessionDeleteQueueMessage) decodedPacket;
//      assertEquals(SESS_DELETE_QUEUE, decodedMessage.getType());
//      assertEquals(message.getQueueName(), decodedMessage.getQueueName());
//   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static class SimpleProtocolEncoderOutput implements
         ProtocolEncoderOutput
   {
      private Object encodedMessage;

      public WriteFuture flush()
      {
         return null;
      }

      public void mergeAll()
      {
      }

      public void write(Object encodedMessage)
      {
         this.encodedMessage = encodedMessage;
      }

      public Object getEncodedMessage()
      {
         return this.encodedMessage;
      }

   }

   private static class SimpleProtocolDencoderOutput implements
         ProtocolDecoderOutput
   {
      private Object message;

      public void flush()
      {
      }

      public void write(Object message)
      {
         this.message = message;
      }

      public Object getMessage()
      {
         return message;
      }

   }
   
   private class NullableStringHolder
   {
      public SimpleString str;
      NullableStringHolder(SimpleString str)
      {
         this.str = str;
      }
   }
}
