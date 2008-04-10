/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat.test.unit;

import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.BOOLEAN_LENGTH;
import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.FALSE;
import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.INT_LENGTH;
import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.LONG_LENGTH;
import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.TRUE;
import static org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec.sizeof;
import static org.jboss.messaging.core.remoting.impl.mina.BufferWrapper.NOT_NULL_STRING;
import static org.jboss.messaging.core.remoting.impl.mina.BufferWrapper.NULL_BYTE;
import static org.jboss.messaging.core.remoting.impl.mina.BufferWrapper.NULL_STRING;
import static org.jboss.messaging.core.remoting.impl.mina.BufferWrapper.UTF_8_ENCODER;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.NO_ID_SET;
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
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.NULL;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PING;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PONG;
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
import static org.jboss.messaging.core.remoting.impl.wireformat.test.unit.CodecAssert.assertEqualsByteArrays;
import static org.jboss.messaging.test.unit.RandomUtil.randomBytes;
import static org.jboss.messaging.test.unit.RandomUtil.randomInt;
import static org.jboss.messaging.test.unit.RandomUtil.randomLong;
import static org.jboss.messaging.test.unit.RandomUtil.randomString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.apache.mina.common.IoBuffer;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.BytesPacketCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConnectionCreateSessionMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConnectionCreateSessionResponseMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConsumerDeliverMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.ConsumerFlowTokenMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.CreateConnectionMessageCodec;
import org.jboss.messaging.core.remoting.impl.codec.CreateConnectionResponseMessageCodec;
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
import org.jboss.messaging.core.remoting.impl.mina.BufferWrapper;
import org.jboss.messaging.core.remoting.impl.mina.PacketCodecFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.BytesPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerDeliverMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerReceiveTokensMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageBlockMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageBlockResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserNextMessageResponseMessage;
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
import org.jboss.messaging.test.unit.RandomUtil;
import org.jboss.messaging.test.unit.UnitTestCase;
import org.jboss.messaging.util.StreamUtils;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
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

   private static ByteBuffer encode(int length, Object... args)
   {
      ByteBuffer buffer = ByteBuffer.allocate(length);
      for (Object arg : args)
      {
         if (arg instanceof Byte)
            buffer.put(((Byte) arg).byteValue());
         else if (arg instanceof Boolean)
         {
            Boolean bool = (Boolean) arg;
            buffer.put(bool ? TRUE : FALSE);
         } else if (arg instanceof Integer)
            buffer.putInt(((Integer) arg).intValue());
         else if (arg instanceof Long)
            buffer.putLong(((Long) arg).longValue());
         else if (arg instanceof Float)
            buffer.putFloat(((Float) arg).floatValue());
         else if (arg instanceof String)
            putNullableString((String) arg, buffer);
         else if (arg == null)
            putNullableString(null, buffer);
         else if (arg instanceof byte[])
         {
            byte[] b = (byte[]) arg;
            buffer.putInt(b.length);
            buffer.put(b);
         } else if (arg instanceof long[])
         {
            long[] longs = (long[]) arg;
            for (long l : longs)
            {
               buffer.putLong(l);
            }
         } else
         {
            fail("no encoding defined for " + arg);
         }
      }
      buffer.flip();
      return buffer;
   }

   private static void putNullableString(String string, ByteBuffer buffer)
   {
      if (string == null)
      {
         buffer.put(NULL_STRING);
      } else
      {
         buffer.put(NOT_NULL_STRING);
         UTF_8_ENCODER.reset();
         UTF_8_ENCODER.encode(CharBuffer.wrap(string), buffer, true);
         buffer.put(NULL_BYTE);
      }
   }

   private static void checkHeader(SimpleRemotingBuffer buffer,
         PacketImpl packet) throws Exception
   {
      checkHeaderBytes(packet, buffer.buffer().buf());

      assertEquals(buffer.get(), packet.getType().byteValue());

      String targetID = (packet.getTargetID().equals(NO_ID_SET) ? null : packet
            .getTargetID());
      String callbackID = (packet.getCallbackID().equals(NO_ID_SET) ? null
            : packet.getCallbackID());
      String executorID = (packet.getExecutorID().equals(NO_ID_SET) ? null
            : packet.getExecutorID());

      int headerLength = LONG_LENGTH + sizeof(targetID) + sizeof(callbackID) + sizeof(executorID)
            + BOOLEAN_LENGTH;
      assertEquals(buffer.getInt(), headerLength);
      assertEquals(buffer.getLong(), packet.getCorrelationID());

      String bufferTargetID = buffer.getNullableString();
      if (bufferTargetID == null)
         bufferTargetID = NO_ID_SET;
      String bufferCallbackID = buffer.getNullableString();
      if (bufferCallbackID == null)
         bufferCallbackID = NO_ID_SET;
      String bufferExecutorID = buffer.getNullableString();
      if (bufferExecutorID == null)
         bufferExecutorID = NO_ID_SET;
      boolean oneWay = buffer.getBoolean();

      assertEquals(bufferTargetID, packet.getTargetID());
      assertEquals(bufferCallbackID, packet.getCallbackID());
      assertEquals(bufferExecutorID, packet.getExecutorID());
      assertEquals(oneWay, packet.isOneWay());
   }

   private static void checkHeaderBytes(PacketImpl packet, ByteBuffer actual)
   {
      String targetID = (packet.getTargetID().equals(NO_ID_SET) ? null : packet
            .getTargetID());
      String callbackID = (packet.getCallbackID().equals(NO_ID_SET) ? null
            : packet.getCallbackID());
      String executorID = (packet.getExecutorID().equals(NO_ID_SET) ? null
            : packet.getExecutorID());

      int headerLength = LONG_LENGTH + sizeof(targetID) + sizeof(callbackID) + sizeof(executorID)
            + BOOLEAN_LENGTH;
      ByteBuffer expected = ByteBuffer.allocate(1 + 1 + INT_LENGTH
            + headerLength);
      expected.put(packet.getType().byteValue());

      expected.putInt(headerLength);
      expected.putLong(packet.getCorrelationID());
      putNullableString(targetID, expected);
      putNullableString(callbackID, expected);
      putNullableString(executorID, expected);
      expected.put(packet.isOneWay() ? TRUE : FALSE);
      expected.flip();

      assertEqualsByteArrays(expected.remaining(), expected.array(), actual
            .array());
   }

   private static void checkBodyIsEmpty(RemotingBuffer buffer)
   {
      assertEquals(0, buffer.getInt());
      // check the buffer has been wholly read
      assertEquals(0, buffer.remaining());
   }

   private static void checkBody(RemotingBuffer buffer, Object... bodyObjects)
   {
      byte[] actualBody = new byte[buffer.getInt()];
      buffer.get(actualBody);
      ByteBuffer expectedBody = encode(actualBody.length, bodyObjects);
      assertEqualsByteArrays(expectedBody.array(), actualBody);
      // check the buffer has been wholly read
      assertEquals(0, buffer.remaining());
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testNullPacket() throws Exception
   {
      PacketImpl packet = new PacketImpl(NULL);
      packet.setCallbackID(randomString());
      packet.setCorrelationID(randomLong());
      packet.setTargetID(randomString());
      packet.setExecutorID(randomString());

      AbstractPacketCodec<Packet> codec = PacketCodecFactory
            .createCodecForEmptyPacket(NULL);
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(NULL, decodedPacket.getType());
      assertEquals(packet.getCallbackID(), decodedPacket.getCallbackID());
      assertEquals(packet.getCorrelationID(), decodedPacket.getCorrelationID());
      assertEquals(packet.getTargetID(), decodedPacket.getTargetID());
   }

   public void testPing() throws Exception
   {
      Ping ping = new Ping(randomString());
      AbstractPacketCodec<Ping> codec = new PingCodec();
      
      SimpleRemotingBuffer buffer = encode(ping, codec);
      checkHeader(buffer, ping);
      checkBody(buffer, ping.getSessionID());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof Ping);
      Ping decodedPing = (Ping) decodedPacket;
      assertEquals(PING, decodedPing.getType());
      assertEquals(ping.getSessionID(), decodedPing.getSessionID());
   }

   public void testPong() throws Exception
   {
      Pong pong = new Pong(randomString(), true);
      AbstractPacketCodec<Pong> codec = new PongCodec();
      
      SimpleRemotingBuffer buffer = encode(pong, codec);
      checkHeader(buffer, pong);
      checkBody(buffer, pong.getSessionID(), pong.isSessionFailed());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof Pong);
      Pong decodedPong = (Pong) decodedPacket;
      assertEquals(PONG, decodedPong.getType());
      assertEquals(pong.getSessionID(), decodedPong.getSessionID());
      assertEquals(pong.isSessionFailed(), decodedPong.isSessionFailed());
   }

   public void testTextPacket() throws Exception
   {
      TextPacket packet = new TextPacket("testTextPacket");
      AbstractPacketCodec<TextPacket> codec = new TextPacketCodec();

      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBody(buffer, packet.getText());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof TextPacket);
      TextPacket p = (TextPacket) decodedPacket;

      assertEquals(TEXT, p.getType());
      assertEquals(packet.getText(), p.getText());
   }
   
   public void testBytesPacket() throws Exception
   {
      BytesPacket packet = new BytesPacket(randomBytes());

      AbstractPacketCodec codec = new BytesPacketCodec();
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBody(buffer, packet.getBytes());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BytesPacket);
      BytesPacket p = (BytesPacket) decodedPacket;

      assertEquals(BYTES, p.getType());
      assertEqualsByteArrays(packet.getBytes(), p.getBytes());
   }
   
   public void testCreateConnectionRequest() throws Exception
   {
      int version = randomInt();
      String remotingSessionID = randomString();
      String clientVMID = randomString();
      String username = null;
      String password = null;

      CreateConnectionRequest request = new CreateConnectionRequest(version,
            remotingSessionID, clientVMID, username, password);

      AbstractPacketCodec<CreateConnectionRequest> codec = new CreateConnectionMessageCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, version, remotingSessionID, clientVMID, username, password);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateConnectionRequest);
      CreateConnectionRequest decodedRequest = (CreateConnectionRequest) decodedPacket;

      assertEquals(CREATECONNECTION, decodedPacket.getType());
      assertEquals(request.getVersion(), decodedRequest.getVersion());
      assertEquals(request.getRemotingSessionID(), decodedRequest
            .getRemotingSessionID());
      assertEquals(request.getClientVMID(), decodedRequest.getClientVMID());
      assertEquals(request.getUsername(), decodedRequest.getUsername());
      assertEquals(request.getPassword(), decodedRequest.getPassword());
   }

   public void testCreateConnectionResponse() throws Exception
   {
      CreateConnectionResponse response = new CreateConnectionResponse(
            randomString());

      AbstractPacketCodec<CreateConnectionResponse> codec = new CreateConnectionResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getConnectionID());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateConnectionResponse);
      CreateConnectionResponse decodedResponse = (CreateConnectionResponse) decodedPacket;
      assertEquals(CREATECONNECTION_RESP, decodedResponse.getType());
      assertEquals(response.getConnectionID(), decodedResponse
            .getConnectionID());
   }

   public void testCreateSessionRequest() throws Exception
   {
      //TODO test this more thoroughly
      
      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(true, true, true);

      AbstractPacketCodec codec = new ConnectionCreateSessionMessageCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.isXA(), request.isAutoCommitSends(), request.isAutoCommitAcks());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ConnectionCreateSessionMessage);
      ConnectionCreateSessionMessage decodedRequest = (ConnectionCreateSessionMessage) decodedPacket;
      assertEquals(CONN_CREATESESSION, decodedRequest.getType());
      assertEquals(request.isXA(), decodedRequest.isXA());
      assertEquals(request.isAutoCommitSends(), decodedRequest.isAutoCommitSends());
      assertEquals(request.isAutoCommitAcks(), decodedRequest.isAutoCommitAcks());
      assertEquals(request.isXA(), decodedRequest.isXA());
   }

   public void testCreateSessionResponse() throws Exception
   {
      ConnectionCreateSessionResponseMessage response = new ConnectionCreateSessionResponseMessage(randomString());

      AbstractPacketCodec codec = new ConnectionCreateSessionResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getSessionID());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ConnectionCreateSessionResponseMessage);
      ConnectionCreateSessionResponseMessage decodedResponse = (ConnectionCreateSessionResponseMessage) decodedPacket;
      assertEquals(CONN_CREATESESSION_RESP, decodedResponse.getType());
      assertEquals(response.getSessionID(), decodedResponse.getSessionID());
   }

   public void testSendMessage() throws Exception
   {
      ProducerSendMessage packet = new ProducerSendMessage(randomString(), new MessageImpl());

      AbstractPacketCodec codec = new ProducerSendMessageCodec();
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBody(buffer, packet.getAddress(), StreamUtils.toBytes(packet.getMessage()));
      buffer.rewind();

      Packet p = codec.decode(buffer);

      assertTrue(p instanceof ProducerSendMessage);
      ProducerSendMessage decodedPacket = (ProducerSendMessage) p;
      assertEquals(PROD_SEND, decodedPacket.getType());
      assertEquals(packet.getAddress(), decodedPacket.getAddress());
      assertEquals(packet.getMessage().getMessageID(), decodedPacket
            .getMessage().getMessageID());
   }

   public void testCreateConsumerRequest() throws Exception
   {      
      String destination = "queue.testCreateConsumerRequest";
      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(destination,
            "color = 'red'", false, false, randomInt(), randomInt());

      AbstractPacketCodec codec = new SessionCreateConsumerMessageCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.getQueueName(), request
            .getFilterString(), request.isNoLocal(), request.isAutoDeleteQueue(), request.getWindowSize(), request.getMaxRate());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCreateConsumerMessage);
      SessionCreateConsumerMessage decodedRequest = (SessionCreateConsumerMessage) decodedPacket;
      assertEquals(SESS_CREATECONSUMER, decodedRequest.getType());
      assertEquals(request.getQueueName(), decodedRequest.getQueueName());
      assertEquals(request.getFilterString(), decodedRequest.getFilterString());
      assertEquals(request.isNoLocal(), decodedRequest.isNoLocal());
      assertEquals(request.isAutoDeleteQueue(), decodedRequest.isAutoDeleteQueue());
      assertEquals(request.getWindowSize(), decodedRequest.getWindowSize());
      assertEquals(request.getMaxRate(), decodedRequest.getMaxRate());
   }

   public void testCreateConsumerResponse() throws Exception
   {
      SessionCreateConsumerResponseMessage response =
      	new SessionCreateConsumerResponseMessage(randomString(), randomInt());

      AbstractPacketCodec codec = new SessionCreateConsumerResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getConsumerID(), response.getWindowSize());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCreateConsumerResponseMessage);
      SessionCreateConsumerResponseMessage decodedResponse = (SessionCreateConsumerResponseMessage) decodedPacket;
      assertEquals(SESS_CREATECONSUMER_RESP, decodedResponse.getType());
      
      assertEquals(response.getConsumerID(), decodedResponse.getConsumerID());
      assertEquals(response.getWindowSize(), decodedResponse.getWindowSize());
   }
   
   public void testCreateProducerRequest() throws Exception
   {      
      String destination = "queue.testCreateProducerRequest";
      int windowSize = randomInt();
      int maxRate = randomInt();
      SessionCreateProducerMessage request = new SessionCreateProducerMessage(destination, windowSize, maxRate);

      AbstractPacketCodec codec = new SessionCreateProducerMessageCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.getAddress(), request.getWindowSize(), request.getMaxRate());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCreateProducerMessage);
      SessionCreateProducerMessage decodedRequest = (SessionCreateProducerMessage) decodedPacket;
      assertEquals(SESS_CREATEPRODUCER, decodedRequest.getType());
      assertEquals(request.getAddress(), decodedRequest.getAddress());
      assertEquals(request.getWindowSize(), decodedRequest.getWindowSize());
      assertEquals(request.getMaxRate(), decodedRequest.getMaxRate());
   }
   
   public void testCreateProducerResponse() throws Exception
   {
      SessionCreateProducerResponseMessage response =
      	new SessionCreateProducerResponseMessage(randomString(), randomInt(), randomInt());

      AbstractPacketCodec codec = new SessionCreateProducerResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getProducerID(), response.getWindowSize(), response.getMaxRate());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCreateProducerResponseMessage);
      SessionCreateProducerResponseMessage decodedResponse = (SessionCreateProducerResponseMessage) decodedPacket;
      assertEquals(SESS_CREATEPRODUCER_RESP, decodedResponse.getType());
      assertEquals(response.getProducerID(), decodedResponse.getProducerID());
      assertEquals(response.getWindowSize(), decodedResponse.getWindowSize());
      assertEquals(response.getMaxRate(), decodedResponse.getMaxRate());
   }

   public void testStartConnectionMessage() throws Exception
   {
      PacketImpl packet = new PacketImpl(CONN_START);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            CONN_START);
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(CONN_START, decodedPacket.getType());
   }

   public void testStopConnectionMessage() throws Exception
   {
      PacketImpl packet = new PacketImpl(CONN_STOP);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            CONN_STOP);
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(CONN_STOP, decodedPacket.getType());
   }

   public void testConsumerFlowTokenMessage() throws Exception
   {
      ConsumerFlowTokenMessage message = new ConsumerFlowTokenMessage(10);
      AbstractPacketCodec codec = new ConsumerFlowTokenMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getTokens());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ConsumerFlowTokenMessage);
      ConsumerFlowTokenMessage decodedMessage = (ConsumerFlowTokenMessage) decodedPacket;
      assertEquals(CONS_FLOWTOKEN, decodedMessage.getType());
      assertEquals(message.getTokens(), decodedMessage.getTokens());
   }
   
   public void testProducerReceiveTokensMessage() throws Exception
   {
   	ProducerReceiveTokensMessage message = new ProducerReceiveTokensMessage(10);
      AbstractPacketCodec codec = new ProducerReceiveTokensMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getTokens());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ProducerReceiveTokensMessage);
      ProducerReceiveTokensMessage decodedMessage = (ProducerReceiveTokensMessage) decodedPacket;
      assertEquals(PacketType.PROD_RECEIVETOKENS, decodedMessage.getType());
      assertEquals(message.getTokens(), decodedMessage.getTokens());
   }

   public void testDeliverMessage() throws Exception
   {
      Message msg = new MessageImpl();
      ConsumerDeliverMessage message = new ConsumerDeliverMessage(msg, randomLong());

      AbstractPacketCodec codec = new ConsumerDeliverMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, StreamUtils.toBytes(msg), message.getDeliveryID());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ConsumerDeliverMessage);
      ConsumerDeliverMessage decodedMessage = (ConsumerDeliverMessage) decodedPacket;
      assertEquals(CONS_DELIVER, decodedMessage.getType());
      assertEquals(message.getMessage().getMessageID(), decodedMessage
            .getMessage().getMessageID());
      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
   }

   public void testSessionAcknowledgeMessage() throws Exception
   {
      SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(
            randomLong(), true);

      AbstractPacketCodec codec = new SessionAcknowledgeMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getDeliveryID(), message.isAllUpTo());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionAcknowledgeMessage);
      SessionAcknowledgeMessage decodedMessage = (SessionAcknowledgeMessage) decodedPacket;
      assertEquals(SESS_ACKNOWLEDGE, decodedMessage.getType());
      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
      assertEquals(message.isAllUpTo(), decodedMessage.isAllUpTo());
   }

   public void testSessionCancelMessage() throws Exception
   {
      SessionCancelMessage message = new SessionCancelMessage(randomLong(),
            true);

      AbstractPacketCodec codec = new SessionCancelMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getDeliveryID(), message.isExpired());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCancelMessage);
      SessionCancelMessage decodedMessage = (SessionCancelMessage) decodedPacket;
      assertEquals(SESS_CANCEL, decodedMessage.getType());
      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
      assertEquals(message.isExpired(), decodedMessage.isExpired());
   }

   public void testSessionCommitMessage() throws Exception
   {
      PacketImpl message = new PacketImpl(SESS_COMMIT);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_COMMIT);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);

      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(SESS_COMMIT, decodedPacket.getType());
   }

   public void testSessionRollbackMessage() throws Exception
   {
      PacketImpl message = new PacketImpl(SESS_ROLLBACK);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_ROLLBACK);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);

      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(SESS_ROLLBACK, decodedPacket.getType());
   }
   
   public void testSessionRecoverMessage() throws Exception
   {
      PacketImpl message = new PacketImpl(SESS_RECOVER);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_RECOVER);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);

      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(SESS_RECOVER, decodedPacket.getType());
   }

   public void testCloseMessage() throws Exception
   {
      PacketImpl message = new PacketImpl(CLOSE);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            CLOSE);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(CLOSE, decodedPacket.getType());
   }


   public void testCreateBrowserRequest() throws Exception
   {
      String destination = "queue.testCreateBrowserRequest";
      SessionCreateBrowserMessage request = new SessionCreateBrowserMessage(destination,
            "color = 'red'");

      AbstractPacketCodec codec = new SessionCreateBrowserMessageCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.getQueueName(), request.getFilterString());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCreateBrowserMessage);
      SessionCreateBrowserMessage decodedRequest = (SessionCreateBrowserMessage) decodedPacket;
      assertEquals(SESS_CREATEBROWSER, decodedRequest.getType());
      assertEquals(request.getQueueName(), decodedRequest.getQueueName());
      assertEquals(request.getFilterString(), decodedRequest.getFilterString());
   }

   public void testCreateBrowserResponse() throws Exception
   {
      SessionCreateBrowserResponseMessage response = new SessionCreateBrowserResponseMessage(randomString());

      AbstractPacketCodec codec = new SessionCreateBrowserResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getBrowserID());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCreateBrowserResponseMessage);
      SessionCreateBrowserResponseMessage decodedResponse = (SessionCreateBrowserResponseMessage) decodedPacket;
      assertEquals(SESS_CREATEBROWSER_RESP, decodedResponse.getType());
      assertEquals(response.getBrowserID(), decodedResponse.getBrowserID());
   }

   public void testBrowserResetMessage() throws Exception
   {
      PacketImpl message = new PacketImpl(SESS_BROWSER_RESET);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_BROWSER_RESET);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(SESS_BROWSER_RESET, decodedPacket.getType());
   }

   public void testBrowserHasNextMessageRequest() throws Exception
   {
      PacketImpl request = new PacketImpl(SESS_BROWSER_HASNEXTMESSAGE);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_BROWSER_HASNEXTMESSAGE);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(SESS_BROWSER_HASNEXTMESSAGE, decodedPacket.getType());
   }

   public void testBrowserHasNextMessageResponse() throws Exception
   {
      SessionBrowserHasNextMessageResponseMessage response = new SessionBrowserHasNextMessageResponseMessage(
            false);
      AbstractPacketCodec codec = new SessionBrowserHasNextMessageResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.hasNext());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBrowserHasNextMessageResponseMessage);
      SessionBrowserHasNextMessageResponseMessage decodedResponse = (SessionBrowserHasNextMessageResponseMessage) decodedPacket;
      assertEquals(SESS_BROWSER_HASNEXTMESSAGE_RESP, decodedResponse.getType());
      assertEquals(response.hasNext(), decodedResponse.hasNext());
   }

   public void testBrowserNextMessageRequest() throws Exception
   {
      PacketImpl request = new PacketImpl(SESS_BROWSER_NEXTMESSAGE);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_BROWSER_NEXTMESSAGE);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(SESS_BROWSER_NEXTMESSAGE, decodedPacket.getType());
   }

   public void testBrowserNextMessageResponse() throws Exception
   {
      Message msg = new MessageImpl();
      SessionBrowserNextMessageResponseMessage response = new SessionBrowserNextMessageResponseMessage(msg);

      AbstractPacketCodec codec = new SessionBrowserNextMessageResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, StreamUtils.toBytes(msg));
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBrowserNextMessageResponseMessage);
      SessionBrowserNextMessageResponseMessage decodedResponse = (SessionBrowserNextMessageResponseMessage) decodedPacket;
      assertEquals(SESS_BROWSER_NEXTMESSAGE_RESP, decodedResponse.getType());
      assertEquals(response.getMessage().getMessageID(), decodedResponse
            .getMessage().getMessageID());
   }

   public void testBrowserNextMessageBlockRequest() throws Exception
   {
      SessionBrowserNextMessageBlockMessage request = new SessionBrowserNextMessageBlockMessage(
            randomLong());

      AbstractPacketCodec codec = new SessionBrowserNextMessageBlockMessageCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.getMaxMessages());
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBrowserNextMessageBlockMessage);
      SessionBrowserNextMessageBlockMessage decodedRequest = (SessionBrowserNextMessageBlockMessage) decodedPacket;
      assertEquals(SESS_BROWSER_NEXTMESSAGEBLOCK, decodedPacket.getType());
      assertEquals(request.getMaxMessages(), decodedRequest.getMaxMessages());
   }

   public void testBrowserNextMessageBlockResponse() throws Exception
   {
      Message[] messages = new Message[] { new MessageImpl(), new MessageImpl() };
      SessionBrowserNextMessageBlockResponseMessage response = new SessionBrowserNextMessageBlockResponseMessage(
            messages);

      AbstractPacketCodec codec = new SessionBrowserNextMessageBlockResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, messages.length, SessionBrowserNextMessageBlockResponseMessageCodec
            .encode(messages));
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBrowserNextMessageBlockResponseMessage);
      SessionBrowserNextMessageBlockResponseMessage decodedResponse = (SessionBrowserNextMessageBlockResponseMessage) decodedPacket;
      assertEquals(SESS_BROWSER_NEXTMESSAGEBLOCK_RESP, decodedResponse.getType());
      assertEquals(response.getMessages()[0].getMessageID(), decodedResponse
            .getMessages()[0].getMessageID());
      assertEquals(response.getMessages()[1].getMessageID(), decodedResponse
            .getMessages()[1].getMessageID());
   }

  
   public void testSesssionXACommitMessageOnePhase() throws Exception
   {
      this.testSessionXACommitMessage(true);
   }
   
   public void testSessionXACommitMessageNotOnePhase() throws Exception
   {
      this.testSessionXACommitMessage(false);
   }
   
   private void testSessionXACommitMessage(boolean onePhase) throws Exception
   {
      Xid xid = this.generateXid();
      SessionXACommitMessage message = new SessionXACommitMessage(xid, onePhase);
      AbstractPacketCodec codec = new SessionXACommitMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXACommitMessage);
      SessionXACommitMessage decodedMessage = (SessionXACommitMessage)decodedPacket;
      assertEquals(SESS_XA_COMMIT, decodedMessage.getType());
      assertEquals(xid, decodedMessage.getXid());      
      assertEquals(onePhase, decodedMessage.isOnePhase());
   }
   
   public void testSessionXAEndMessageFailed() throws Exception
   {
      this.testSessionXAEndMessage(true);
   }
   
   public void testSessionXAEndMessageNotFailed() throws Exception
   {
      this.testSessionXACommitMessage(false);
   }
   
   private void testSessionXAEndMessage(boolean failed) throws Exception
   {
      Xid xid = this.generateXid();
      SessionXAEndMessage message = new SessionXAEndMessage(xid, failed);
      AbstractPacketCodec codec = new SessionXAEndMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAEndMessage);
      SessionXAEndMessage decodedMessage = (SessionXAEndMessage)decodedPacket;
      assertEquals(SESS_XA_END, decodedMessage.getType());
      assertEquals(xid, decodedMessage.getXid());      
      assertEquals(failed, decodedMessage.isFailed());
   }
   
   public void testSessionXAForgetMessage() throws Exception
   {
      Xid xid = this.generateXid();
      SessionXAForgetMessage message = new SessionXAForgetMessage(xid);
      AbstractPacketCodec codec = new SessionXAForgetMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAForgetMessage);
      SessionXAForgetMessage decodedMessage = (SessionXAForgetMessage)decodedPacket;
      assertEquals(SESS_XA_FORGET, decodedMessage.getType());
      assertEquals(xid, decodedMessage.getXid());      
   }
   
   public void testSessionXAGetInDoubtXidsMessage() throws Exception
   {
      PacketImpl request = new PacketImpl(SESS_XA_INDOUBT_XIDS);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_XA_INDOUBT_XIDS);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(SESS_XA_INDOUBT_XIDS, decodedPacket.getType());            
   }
   
   public void testSessionGetInDoubtXidsResponse() throws Exception
   {
      final int numXids = 10;
      List<Xid> xids = new ArrayList<Xid>();
      for (int i = 0; i < numXids; i++)
      {
         xids.add(generateXid());
      }
      
      SessionXAGetInDoubtXidsResponseMessage message = new SessionXAGetInDoubtXidsResponseMessage(xids);
      AbstractPacketCodec codec = new SessionXAGetInDoubtXidsResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAGetInDoubtXidsResponseMessage);
      SessionXAGetInDoubtXidsResponseMessage decodedMessage = (SessionXAGetInDoubtXidsResponseMessage)decodedPacket;
      assertEquals(SESS_XA_INDOUBT_XIDS_RESP, decodedMessage.getType());
           
      List<Xid> decodedXids = decodedMessage.getXids();
      assertNotNull(decodedXids);
      assertEquals(xids.size(), decodedXids.size());
      
      for (int i = 0; i < numXids; i++)
      {
         assertEquals(xids.get(i), decodedXids.get(i));
      }
   }
   
   public void testSessionXAGetTimeoutMessage() throws Exception
   {
      PacketImpl message = new PacketImpl(SESS_XA_GET_TIMEOUT);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            PacketType.SESS_XA_GET_TIMEOUT);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(SESS_XA_GET_TIMEOUT, decodedPacket.getType());     
   }
   
   public void testSessionXAGetTimeoutResponse() throws Exception
   {
      final int timeout = RandomUtil.randomInt();
      
      SessionXAGetTimeoutResponseMessage message = new SessionXAGetTimeoutResponseMessage(timeout);
      AbstractPacketCodec codec = new SessionXAGetTimeoutResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAGetTimeoutResponseMessage);
      SessionXAGetTimeoutResponseMessage decodedMessage = (SessionXAGetTimeoutResponseMessage)decodedPacket;
      assertEquals(SESS_XA_GET_TIMEOUT_RESP, decodedMessage.getType());
           
      assertEquals(timeout, decodedMessage.getTimeoutSeconds());
   }

   public void testSessionXAJoinMessage() throws Exception
   {
      Xid xid = this.generateXid();
      SessionXAJoinMessage message = new SessionXAJoinMessage(xid);
      AbstractPacketCodec codec = new SessionXAJoinMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAJoinMessage);
      SessionXAJoinMessage decodedMessage = (SessionXAJoinMessage)decodedPacket;
      assertEquals(SESS_XA_JOIN, decodedMessage.getType());
      assertEquals(xid, decodedMessage.getXid());      
   }
   
   public void testSessionXAPrepareMessage() throws Exception
   {
      Xid xid = this.generateXid();
      SessionXAPrepareMessage message = new SessionXAPrepareMessage(xid);
      AbstractPacketCodec codec = new SessionXAPrepareMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAPrepareMessage);
      SessionXAPrepareMessage decodedMessage = (SessionXAPrepareMessage)decodedPacket;
      assertEquals(SESS_XA_PREPARE, decodedMessage.getType());
      assertEquals(xid, decodedMessage.getXid());      
   }
   
   public void testSessionXAResponseErrorNullString() throws Exception
   {
      testSessionXAResponse(true, true);
   }
   
   public void testSessionXAResponseErrorNotNullString() throws Exception
   {
      testSessionXAResponse(true, false);
   }
   
   public void testSessionXAResponseNoErrorNullString() throws Exception
   {
      testSessionXAResponse(false, true);
   }
   
   public void testSessionXAResponseNoErrorNotNullString() throws Exception
   {
      testSessionXAResponse(false, false);
   }
   
   private void testSessionXAResponse(boolean error, boolean nullString) throws Exception
   {
      int responseCode = RandomUtil.randomInt();
      
      String str = nullString ? null : RandomUtil.randomString();
      
      SessionXAResponseMessage message = new SessionXAResponseMessage(error, responseCode, str);
      AbstractPacketCodec codec = new SessionXAResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAResponseMessage);
      SessionXAResponseMessage decodedMessage = (SessionXAResponseMessage)decodedPacket;
      assertEquals(SESS_XA_RESP, decodedMessage.getType());
      assertEquals(error, decodedMessage.isError());
      assertEquals(responseCode, decodedMessage.getResponseCode());
      assertEquals(str, decodedMessage.getMessage());
   }
   
   public void testSessionXAResumeMessage() throws Exception
   {
      Xid xid = this.generateXid();
      SessionXAResumeMessage message = new SessionXAResumeMessage(xid);
      AbstractPacketCodec codec = new SessionXAResumeMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAResumeMessage);
      SessionXAResumeMessage decodedMessage = (SessionXAResumeMessage)decodedPacket;
      assertEquals(SESS_XA_RESUME, decodedMessage.getType());
      assertEquals(xid, decodedMessage.getXid());      
   }
   
   public void testSessionXARollbackMessage() throws Exception
   {
      Xid xid = this.generateXid();
      SessionXARollbackMessage message = new SessionXARollbackMessage(xid);
      AbstractPacketCodec codec = new SessionXARollbackMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXARollbackMessage);
      SessionXARollbackMessage decodedMessage = (SessionXARollbackMessage)decodedPacket;
      assertEquals(SESS_XA_ROLLBACK, decodedMessage.getType());
      assertEquals(xid, decodedMessage.getXid());      
   }
   
   public void testSessionXASetTimeoutMessage() throws Exception
   {
      final int timeout = RandomUtil.randomInt();
      SessionXASetTimeoutMessage message = new SessionXASetTimeoutMessage(timeout);
      AbstractPacketCodec codec = new SessionXASetTimeoutMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXASetTimeoutMessage);
      SessionXASetTimeoutMessage decodedMessage = (SessionXASetTimeoutMessage)decodedPacket;
      assertEquals(SESS_XA_SET_TIMEOUT, decodedMessage.getType());
      assertEquals(timeout, decodedMessage.getTimeoutSeconds());      
   }
   
   public void testSessionXASetTimeoutResponseMessageOK() throws Exception
   {
      testSessionXASetTimeoutResponseMessage(true);
   }
   
   public void testSessionXASetTimeoutResponseMessageNotOK() throws Exception
   {
      testSessionXASetTimeoutResponseMessage(false);
   }
   
   private void testSessionXASetTimeoutResponseMessage(boolean ok) throws Exception
   {
      final int timeout = RandomUtil.randomInt();
      SessionXASetTimeoutResponseMessage message = new SessionXASetTimeoutResponseMessage(ok);
      AbstractPacketCodec codec = new SessionXASetTimeoutResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXASetTimeoutResponseMessage);
      SessionXASetTimeoutResponseMessage decodedMessage = (SessionXASetTimeoutResponseMessage)decodedPacket;
      assertEquals(SESS_XA_SET_TIMEOUT_RESP, decodedMessage.getType());
      assertEquals(ok, decodedMessage.isOK());      
   }
   
   public void testSessionXAStartMessage() throws Exception
   {
      Xid xid = this.generateXid();
      SessionXAStartMessage message = new SessionXAStartMessage(xid);
      AbstractPacketCodec codec = new SessionXAStartMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();
      
      Packet decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAStartMessage);
      SessionXAStartMessage decodedMessage = (SessionXAStartMessage)decodedPacket;
      assertEquals(SESS_XA_START, decodedMessage.getType());
      assertEquals(xid, decodedMessage.getXid());      
   }
   
   public void testSessionXASuspendMessage() throws Exception
   {
      PacketImpl message = new PacketImpl(SESS_XA_SUSPEND);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            PacketType.SESS_XA_SUSPEND);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertEquals(SESS_XA_SUSPEND, decodedPacket.getType());     
   }
   
   
   public void testSessionRemoveAddressMessage() throws Exception
   {
      SessionRemoveDestinationMessage message = new SessionRemoveDestinationMessage(randomString(), true);

      AbstractPacketCodec codec = new SessionRemoveDestinationMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionRemoveDestinationMessage);
      SessionRemoveDestinationMessage decodedMessage = (SessionRemoveDestinationMessage)decodedPacket;
      assertEquals(SESS_REMOVE_DESTINATION, decodedMessage.getType());
      
      assertEquals(message.getAddress(), decodedMessage.getAddress());
      assertEquals(message.isTemporary(), decodedMessage.isTemporary());
            
   }
   
   public void testSessionCreateQueueRequest() throws Exception
   {
      SessionCreateQueueMessage message = new SessionCreateQueueMessage(randomString(), randomString(), randomString(), true, true);

      AbstractPacketCodec codec = new SessionCreateQueueMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCreateQueueMessage);
      SessionCreateQueueMessage decodedMessage = (SessionCreateQueueMessage)decodedPacket;
      assertEquals(SESS_CREATEQUEUE, decodedMessage.getType());
      
      assertEquals(message.getAddress(), decodedMessage.getAddress());
      assertEquals(message.getQueueName(), decodedMessage.getQueueName());
      assertEquals(message.getFilterString(), decodedMessage.getFilterString());
      assertEquals(message.isDurable(), decodedMessage.isDurable());
      assertEquals(message.isTemporary(), decodedMessage.isDurable());
            
   }
   
   public void testSessionQueueQueryRequest() throws Exception
   {
      SessionQueueQueryMessage message = new SessionQueueQueryMessage(randomString());

      AbstractPacketCodec codec = new SessionQueueQueryMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionQueueQueryMessage);
      SessionQueueQueryMessage decodedMessage = (SessionQueueQueryMessage)decodedPacket;
      assertEquals(SESS_QUEUEQUERY, decodedMessage.getType());
      
      assertEquals(message.getQueueName(), decodedMessage.getQueueName());            
   }
   
   public void testSessionQueueQueryResponse() throws Exception
   {
      SessionQueueQueryResponseMessage message = new SessionQueueQueryResponseMessage(true, true, randomInt(), randomInt(), randomInt(),
                                                          randomString(), randomString());

      AbstractPacketCodec codec = new SessionQueueQueryResponseMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionQueueQueryResponseMessage);
      SessionQueueQueryResponseMessage decodedMessage = (SessionQueueQueryResponseMessage)decodedPacket;
      assertEquals(SESS_QUEUEQUERY_RESP, decodedMessage.getType());
      
      assertEquals(message.isExists(), decodedMessage.isExists());
      assertEquals(message.isDurable(), decodedMessage.isDurable());
      assertEquals(message.isTemporary(), decodedMessage.isTemporary());
      assertEquals(message.getConsumerCount(), decodedMessage.getConsumerCount());
      assertEquals(message.getMessageCount(), decodedMessage.getMessageCount());
      assertEquals(message.getFilterString(), decodedMessage.getFilterString());
      assertEquals(message.getAddress(), decodedMessage.getAddress());         
   }
   
   public void testSessionAddAddressMessage() throws Exception
   {
      SessionAddDestinationMessage message = new SessionAddDestinationMessage(randomString(), true);

      AbstractPacketCodec<SessionAddDestinationMessage> codec = new SessionAddDestinationMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionAddDestinationMessage);
      SessionAddDestinationMessage decodedMessage = (SessionAddDestinationMessage)decodedPacket;
      assertEquals(SESS_ADD_DESTINATION, decodedMessage.getType());
      
      assertEquals(message.getAddress(), decodedMessage.getAddress());      
      assertEquals(message.isTemporary(), decodedMessage.isTemporary());
   }
   
   public void testSessionBindingQueryMessage() throws Exception
   {
      SessionBindingQueryMessage message = new SessionBindingQueryMessage(randomString());

      AbstractPacketCodec codec = new SessionBindingQueryMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBindingQueryMessage);
      SessionBindingQueryMessage decodedMessage = (SessionBindingQueryMessage)decodedPacket;
      assertEquals(SESS_BINDINGQUERY, decodedMessage.getType());
      
      assertEquals(message.getAddress(), decodedMessage.getAddress());        
   }
   
   public void testSessionBindingQueryResponseMessage() throws Exception
   {
      boolean exists = true;
      List<String> queueNames = new ArrayList<String>();
      queueNames.add(randomString());
      queueNames.add(randomString());
      queueNames.add(randomString());
      SessionBindingQueryResponseMessage message = new SessionBindingQueryResponseMessage(exists, queueNames);

      AbstractPacketCodec codec = new SessionBindingQueryResponseMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBindingQueryResponseMessage);
      SessionBindingQueryResponseMessage decodedMessage = (SessionBindingQueryResponseMessage)decodedPacket;
      assertEquals(SESS_BINDINGQUERY_RESP, decodedMessage.getType());
      
      assertEquals(message.isExists(), decodedMessage.isExists());
      
      List<String> decodedNames = decodedMessage.getQueueNames();
      assertEquals(queueNames.size(), decodedNames.size());
      for (int i = 0; i < queueNames.size(); i++)
      {
         assertEquals(queueNames.get(i), decodedNames.get(i));
      }
   }
   
   
   public void testDeleteQueueRequest() throws Exception
   {
      SessionDeleteQueueMessage message = new SessionDeleteQueueMessage(randomString());

      AbstractPacketCodec codec = new SessionDeleteQueueMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      Packet decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionDeleteQueueMessage);
      SessionDeleteQueueMessage decodedMessage = (SessionDeleteQueueMessage)decodedPacket;
      assertEquals(SESS_DELETE_QUEUE, decodedMessage.getType());
      
      assertEquals(message.getQueueName(), decodedMessage.getQueueName());        
   }
   
   
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private SimpleRemotingBuffer encode(PacketImpl packet,
         AbstractPacketCodec codec) throws Exception
   {
      log.debug("encode " + packet);

      IoBuffer b = IoBuffer.allocate(256);
      b.setAutoExpand(true);

      SimpleRemotingBuffer buf = new SimpleRemotingBuffer(b);

      codec.encode(packet, buf);
      buf.flip();

      return buf;
   }

   private final class SimpleRemotingBuffer extends BufferWrapper
   {

      public SimpleRemotingBuffer(IoBuffer buffer)
      {
         super(buffer);
      }

      IoBuffer buffer()
      {
         return buffer;
      }

      public void flip()
      {
         buffer.flip();
      }

      public void rewind() throws IOException
      {
         buffer.rewind();
      }

      public int remaining()
      {
         return buffer.remaining();
      }
   }
}
