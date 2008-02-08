/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat.test.unit;

import static org.jboss.messaging.core.remoting.codec.AbstractPacketCodec.BOOLEAN_LENGTH;
import static org.jboss.messaging.core.remoting.codec.AbstractPacketCodec.FALSE;
import static org.jboss.messaging.core.remoting.codec.AbstractPacketCodec.INT_LENGTH;
import static org.jboss.messaging.core.remoting.codec.AbstractPacketCodec.LONG_LENGTH;
import static org.jboss.messaging.core.remoting.codec.AbstractPacketCodec.TRUE;
import static org.jboss.messaging.core.remoting.codec.AbstractPacketCodec.encodeMessage;
import static org.jboss.messaging.core.remoting.codec.AbstractPacketCodec.sizeof;
import static org.jboss.messaging.core.remoting.impl.mina.MinaPacketCodec.NOT_NULL_STRING;
import static org.jboss.messaging.core.remoting.impl.mina.MinaPacketCodec.NULL_BYTE;
import static org.jboss.messaging.core.remoting.impl.mina.MinaPacketCodec.NULL_STRING;
import static org.jboss.messaging.core.remoting.impl.mina.MinaPacketCodec.UTF_8_ENCODER;
import static org.jboss.messaging.core.remoting.wireformat.AbstractPacket.NO_ID_SET;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.BYTES;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_ACKNOWLEDGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_ADD_ADDRESS;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_RESET;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CANCEL;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONS_FLOWTOKEN;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_COMMIT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_DELIVER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_RECOVER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_REMOVE_ADDRESS;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_ROLLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_SEND;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_SETID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONN_START;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONN_STOP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_COMMIT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_END;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_FORGET;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_GET_TIMEOUT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_GET_TIMEOUT_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_JOIN;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_RESUME;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_ROLLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_SET_TIMEOUT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_SET_TIMEOUT_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_START;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_SUSPEND;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.NULL;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.PING;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.PONG;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BINDINGQUERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGEBLOCK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CREATECONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONN_CREATESESSION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATEQUEUE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_DELETE_QUEUE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_QUEUEQUERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_INDOUBT_XIDS;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_PREPARE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BINDINGQUERY_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_HASNEXTMESSAGE_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGE_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BROWSER_NEXTMESSAGEBLOCK_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATEBROWSER_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CREATECONNECTION_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATECONSUMER_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONN_CREATESESSION_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_QUEUEQUERY_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_XA_INDOUBT_XIDS_RESP;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.TEXT;
import static org.jboss.messaging.core.remoting.wireformat.test.unit.CodecAssert.assertEqualsByteArrays;
import static org.jboss.messaging.test.unit.RandomUtil.randomByte;
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
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.impl.MessageImpl;
import org.jboss.messaging.core.remoting.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.codec.SessionBindingQueryMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBindingQueryResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBrowserHasNextMessageResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBrowserNextMessageBlockMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBrowserNextMessageBlockResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionBrowserNextMessageResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.BytesPacketCodec;
import org.jboss.messaging.core.remoting.codec.CreateConnectionMessageCodec;
import org.jboss.messaging.core.remoting.codec.CreateConnectionResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.ConsumerFlowTokenMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCreateBrowserMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCreateBrowserResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCreateConsumerMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCreateConsumerResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.ConnectionCreateSessionMessageCodec;
import org.jboss.messaging.core.remoting.codec.ConnectionCreateSessionResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionDeleteQueueMessageCodec;
import org.jboss.messaging.core.remoting.codec.DeliverMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionQueueQueryMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionQueueQueryResponseMessageCodec;
import org.jboss.messaging.core.remoting.codec.RemotingBuffer;
import org.jboss.messaging.core.remoting.codec.SessionAcknowledgeMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionAddAddressMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCancelMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionCreateQueueMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionRemoveAddressMessageCodec;
import org.jboss.messaging.core.remoting.codec.SessionSendMessageCodec;
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
import org.jboss.messaging.core.remoting.codec.SessionSetIDMessageCodec;
import org.jboss.messaging.core.remoting.codec.TextPacketCodec;
import org.jboss.messaging.core.remoting.impl.mina.PacketCodecFactory;
import org.jboss.messaging.core.remoting.impl.mina.MinaPacketCodec.BufferWrapper;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserHasNextMessageMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageBlockMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageBlockResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserNextMessageResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionBrowserResetMessage;
import org.jboss.messaging.core.remoting.wireformat.BytesPacket;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateBrowserMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateBrowserResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCreateQueueMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionDeleteQueueMessage;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.Ping;
import org.jboss.messaging.core.remoting.wireformat.Pong;
import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionAcknowledgeMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionAddAddressMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCancelMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionCommitMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRecoverMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRemoveAddressMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionRollbackMessage;
import org.jboss.messaging.core.remoting.wireformat.SessionSendMessage;
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
import org.jboss.messaging.core.remoting.wireformat.SessionSetIDMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionStartMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionStopMessage;
import org.jboss.messaging.core.remoting.wireformat.TextPacket;
import org.jboss.messaging.test.unit.RandomUtil;
import org.jboss.messaging.test.unit.UnitTestCase;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
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
         AbstractPacket packet) throws Exception
   {
      checkHeaderBytes(packet, buffer.buffer().buf());

      assertEquals(buffer.get(), packet.getType().byteValue());

      String targetID = packet.getTargetID();
      if (NO_ID_SET.equals(packet.getTargetID()))
         targetID = null;
      String callbackID = packet.getCallbackID();
      if (NO_ID_SET.equals(packet.getCallbackID()))
         callbackID = null;

      int headerLength = LONG_LENGTH + sizeof(targetID) + sizeof(callbackID)
            + BOOLEAN_LENGTH;
      assertEquals(buffer.getInt(), headerLength);
      assertEquals(buffer.getLong(), packet.getCorrelationID());

      String bufferTargetID = buffer.getNullableString();
      if (bufferTargetID == null)
         bufferTargetID = NO_ID_SET;
      String bufferCallbackID = buffer.getNullableString();
      if (bufferCallbackID == null)
         bufferCallbackID = NO_ID_SET;
      boolean oneWay = buffer.getBoolean();

      assertEquals(bufferTargetID, packet.getTargetID());
      assertEquals(bufferCallbackID, packet.getCallbackID());
      assertEquals(oneWay, packet.isOneWay());
   }

   private static void checkHeaderBytes(AbstractPacket packet, ByteBuffer actual)
   {
      String targetID = (packet.getTargetID().equals(NO_ID_SET) ? null : packet
            .getTargetID());
      String callbackID = (packet.getCallbackID().equals(NO_ID_SET) ? null
            : packet.getCallbackID());

      int headerLength = LONG_LENGTH + sizeof(targetID) + sizeof(callbackID)
            + BOOLEAN_LENGTH;
      ByteBuffer expected = ByteBuffer.allocate(1 + 1 + INT_LENGTH
            + headerLength);
      expected.put(packet.getType().byteValue());

      expected.putInt(headerLength);
      expected.putLong(packet.getCorrelationID());
      putNullableString(targetID, expected);
      putNullableString(callbackID, expected);
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
      NullPacket packet = new NullPacket();
      packet.setCallbackID(randomString());
      packet.setCorrelationID(randomLong());
      packet.setTargetID(randomString());

      AbstractPacketCodec<AbstractPacket> codec = PacketCodecFactory
            .createCodecForEmptyPacket(NULL, NullPacket.class);
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof NullPacket);
      NullPacket p = (NullPacket) decodedPacket;

      assertEquals(NULL, p.getType());
      assertEquals(packet.getCallbackID(), p.getCallbackID());
      assertEquals(packet.getCorrelationID(), p.getCorrelationID());
      assertEquals(packet.getTargetID(), p.getTargetID());
   }

   public void testPing() throws Exception
   {
      Ping packet = new Ping();
      AbstractPacketCodec<AbstractPacket> codec = PacketCodecFactory
            .createCodecForEmptyPacket(PING, Ping.class);

      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof Ping);
      assertEquals(PING, decodedPacket.getType());
   }

   public void testPong() throws Exception
   {
      Pong packet = new Pong();
      AbstractPacketCodec<AbstractPacket> codec = PacketCodecFactory
            .createCodecForEmptyPacket(PONG, Pong.class);

      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof Pong);
      assertEquals(PONG, decodedPacket.getType());
   }

   public void testTextPacket() throws Exception
   {
      TextPacket packet = new TextPacket("testTextPacket");
      AbstractPacketCodec<TextPacket> codec = new TextPacketCodec();

      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBody(buffer, packet.getText());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BytesPacket);
      BytesPacket p = (BytesPacket) decodedPacket;

      assertEquals(BYTES, p.getType());
      assertEqualsByteArrays(packet.getBytes(), p.getBytes());
   }

   public void testSetSessionIDMessage() throws Exception
   {
      SessionSetIDMessage message = new SessionSetIDMessage(randomString());

      AbstractPacketCodec codec = new SessionSetIDMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getSessionID());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionSetIDMessage);
      SessionSetIDMessage decodedMessage = (SessionSetIDMessage) decodedPacket;
      assertEquals(SESS_SETID, decodedMessage.getType());
      assertEquals(message.getSessionID(), decodedMessage.getSessionID());
   }
   
   public void testCreateConnectionRequest() throws Exception
   {
      byte version = randomByte();
      String remotingSessionID = randomString();
      String clientVMID = randomString();
      String username = null;
      String password = null;
      int prefetchSize = 0;
 
      CreateConnectionRequest request = new CreateConnectionRequest(version,
            remotingSessionID, clientVMID, username, password, prefetchSize);

      AbstractPacketCodec<CreateConnectionRequest> codec = new CreateConnectionMessageCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, version, remotingSessionID, clientVMID, username,
            password, prefetchSize);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ConnectionCreateSessionResponseMessage);
      ConnectionCreateSessionResponseMessage decodedResponse = (ConnectionCreateSessionResponseMessage) decodedPacket;
      assertEquals(CONN_CREATESESSION_RESP, decodedResponse.getType());
      assertEquals(response.getSessionID(), decodedResponse.getSessionID());
   }

   public void testSendMessage() throws Exception
   {
      SessionSendMessage packet = new SessionSendMessage(randomString(), new MessageImpl());

      AbstractPacketCodec codec = new SessionSendMessageCodec();
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBody(buffer, packet.getAddress(), encodeMessage(packet.getMessage()));
      buffer.rewind();

      AbstractPacket p = codec.decode(buffer);

      assertTrue(p instanceof SessionSendMessage);
      SessionSendMessage decodedPacket = (SessionSendMessage) p;
      assertEquals(SESS_SEND, decodedPacket.getType());
      assertEquals(packet.getAddress(), decodedPacket.getAddress());
      assertEquals(packet.getMessage().getMessageID(), decodedPacket
            .getMessage().getMessageID());
   }

   public void testCreateConsumerRequest() throws Exception
   {      
      String destination = "queue.testCreateConsumerRequest";
      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(destination,
            "color = 'red'", false, false);

      AbstractPacketCodec codec = new SessionCreateConsumerMessageCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.getQueueName(), request
            .getFilterString(), request.isNoLocal(), request.isAutoDeleteQueue());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCreateConsumerMessage);
      SessionCreateConsumerMessage decodedRequest = (SessionCreateConsumerMessage) decodedPacket;
      assertEquals(SESS_CREATECONSUMER, decodedRequest.getType());
      assertEquals(request.getQueueName(), decodedRequest.getQueueName());
      assertEquals(request.getFilterString(), decodedRequest.getFilterString());
      assertEquals(request.isNoLocal(), decodedRequest.isNoLocal());
      assertEquals(request.isAutoDeleteQueue(), decodedRequest.isAutoDeleteQueue());
   }

   public void testCreateConsumerResponse() throws Exception
   {

      SessionCreateConsumerResponseMessage response = new SessionCreateConsumerResponseMessage(
            randomString(), RandomUtil.randomInt());

      AbstractPacketCodec codec = new SessionCreateConsumerResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getConsumerID(), response.getPrefetchSize());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCreateConsumerResponseMessage);
      SessionCreateConsumerResponseMessage decodedResponse = (SessionCreateConsumerResponseMessage) decodedPacket;
      assertEquals(SESS_CREATECONSUMER_RESP, decodedResponse.getType());
      assertEquals(response.getPrefetchSize(), decodedResponse.getPrefetchSize());
   }

   public void testStartConnectionMessage() throws Exception
   {
      ConnectionStartMessage packet = new ConnectionStartMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            CONN_START, ConnectionStartMessage.class);
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ConnectionStartMessage);
      assertEquals(CONN_START, decodedPacket.getType());
   }

   public void testStopConnectionMessage() throws Exception
   {
      ConnectionStopMessage packet = new ConnectionStopMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            CONN_STOP, ConnectionStopMessage.class);
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ConnectionStopMessage);
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

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ConsumerFlowTokenMessage);
      ConsumerFlowTokenMessage decodedMessage = (ConsumerFlowTokenMessage) decodedPacket;
      assertEquals(CONS_FLOWTOKEN, decodedMessage.getType());
      assertEquals(message.getTokens(), decodedMessage.getTokens());
   }

   public void testDeliverMessage() throws Exception
   {
      Message msg = new MessageImpl();
      DeliverMessage message = new DeliverMessage(msg, randomLong(), 23);

      AbstractPacketCodec codec = new DeliverMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, encodeMessage(msg), message.getDeliveryID(), message
            .getDeliveryCount());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof DeliverMessage);
      DeliverMessage decodedMessage = (DeliverMessage) decodedPacket;
      assertEquals(SESS_DELIVER, decodedMessage.getType());
      assertEquals(message.getMessage().getMessageID(), decodedMessage
            .getMessage().getMessageID());
      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
      assertEquals(message.getDeliveryCount(), decodedMessage
            .getDeliveryCount());
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

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCancelMessage);
      SessionCancelMessage decodedMessage = (SessionCancelMessage) decodedPacket;
      assertEquals(SESS_CANCEL, decodedMessage.getType());
      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
      assertEquals(message.isExpired(), decodedMessage.isExpired());
   }

   public void testSessionCommitMessage() throws Exception
   {
      SessionCommitMessage message = new SessionCommitMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_COMMIT, SessionCommitMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);

      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCommitMessage);
      assertEquals(SESS_COMMIT, decodedPacket.getType());
   }

   public void testSessionRollbackMessage() throws Exception
   {
      SessionRollbackMessage message = new SessionRollbackMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_ROLLBACK, SessionRollbackMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);

      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionRollbackMessage);
      assertEquals(SESS_ROLLBACK, decodedPacket.getType());
   }
   
   public void testSessionRecoverMessage() throws Exception
   {
      SessionRecoverMessage message = new SessionRecoverMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_RECOVER, SessionRecoverMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);

      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionRecoverMessage);
      assertEquals(SESS_RECOVER, decodedPacket.getType());
   }

   public void testCloseMessage() throws Exception
   {
      CloseMessage message = new CloseMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            CLOSE, CloseMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CloseMessage);
      CloseMessage decodedMessage = (CloseMessage) decodedPacket;
      assertEquals(CLOSE, decodedMessage.getType());
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

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCreateBrowserResponseMessage);
      SessionCreateBrowserResponseMessage decodedResponse = (SessionCreateBrowserResponseMessage) decodedPacket;
      assertEquals(SESS_CREATEBROWSER_RESP, decodedResponse.getType());
      assertEquals(response.getBrowserID(), decodedResponse.getBrowserID());
   }

   public void testBrowserResetMessage() throws Exception
   {
      SessionBrowserResetMessage message = new SessionBrowserResetMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_BROWSER_RESET, SessionBrowserResetMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBrowserResetMessage);
      assertEquals(SESS_BROWSER_RESET, decodedPacket.getType());
   }

   public void testBrowserHasNextMessageRequest() throws Exception
   {
      SessionBrowserHasNextMessageMessage request = new SessionBrowserHasNextMessageMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_BROWSER_HASNEXTMESSAGE, SessionBrowserHasNextMessageMessage.class);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBrowserHasNextMessageMessage);
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

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBrowserHasNextMessageResponseMessage);
      SessionBrowserHasNextMessageResponseMessage decodedResponse = (SessionBrowserHasNextMessageResponseMessage) decodedPacket;
      assertEquals(SESS_BROWSER_HASNEXTMESSAGE_RESP, decodedResponse.getType());
      assertEquals(response.hasNext(), decodedResponse.hasNext());
   }

   public void testBrowserNextMessageRequest() throws Exception
   {
      SessionBrowserNextMessageMessage request = new SessionBrowserNextMessageMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_BROWSER_NEXTMESSAGE, SessionBrowserNextMessageMessage.class);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBrowserNextMessageMessage);
      assertEquals(SESS_BROWSER_NEXTMESSAGE, decodedPacket.getType());
   }

   public void testBrowserNextMessageResponse() throws Exception
   {
      Message msg = new MessageImpl();
      SessionBrowserNextMessageResponseMessage response = new SessionBrowserNextMessageResponseMessage(msg);

      AbstractPacketCodec codec = new SessionBrowserNextMessageResponseMessageCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, encodeMessage(msg));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAForgetMessage);
      SessionXAForgetMessage decodedMessage = (SessionXAForgetMessage)decodedPacket;
      assertEquals(SESS_XA_FORGET, decodedMessage.getType());
      assertEquals(xid, decodedMessage.getXid());      
   }
   
   public void testSessionXAGetInDoubtXidsMessage() throws Exception
   {
      SessionXAGetInDoubtXidsMessage request = new SessionXAGetInDoubtXidsMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            SESS_XA_INDOUBT_XIDS, SessionXAGetInDoubtXidsMessage.class);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionXAGetInDoubtXidsMessage);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      SessionXAGetTimeoutMessage message = new SessionXAGetTimeoutMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            PacketType.SESS_XA_GET_TIMEOUT, SessionXAGetTimeoutMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionXAGetTimeoutMessage);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
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
      
      AbstractPacket decodedPacket = codec.decode(buffer);
      assertTrue(decodedPacket instanceof SessionXAStartMessage);
      SessionXAStartMessage decodedMessage = (SessionXAStartMessage)decodedPacket;
      assertEquals(SESS_XA_START, decodedMessage.getType());
      assertEquals(xid, decodedMessage.getXid());      
   }
   
   public void testSessionXASuspendMessage() throws Exception
   {
      SessionXASuspendMessage message = new SessionXASuspendMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            PacketType.SESS_XA_SUSPEND, SessionXASuspendMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionXASuspendMessage);
      assertEquals(SESS_XA_SUSPEND, decodedPacket.getType());     
   }
   
   
   public void testSessionRemoveAddressMessage() throws Exception
   {
      SessionRemoveAddressMessage message = new SessionRemoveAddressMessage(randomString());

      AbstractPacketCodec codec = new SessionRemoveAddressMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionRemoveAddressMessage);
      SessionRemoveAddressMessage decodedMessage = (SessionRemoveAddressMessage)decodedPacket;
      assertEquals(SESS_REMOVE_ADDRESS, decodedMessage.getType());
      
      assertEquals(message.getAddress(), decodedMessage.getAddress());
            
   }
   
   public void testSessionCreateQueueRequest() throws Exception
   {
      SessionCreateQueueMessage message = new SessionCreateQueueMessage(randomString(), randomString(), randomString(), true, true);

      AbstractPacketCodec codec = new SessionCreateQueueMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

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
      SessionAddAddressMessage message = new SessionAddAddressMessage(randomString());

      AbstractPacketCodec codec = new SessionAddAddressMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionAddAddressMessage);
      SessionAddAddressMessage decodedMessage = (SessionAddAddressMessage)decodedPacket;
      assertEquals(SESS_ADD_ADDRESS, decodedMessage.getType());
      
      assertEquals(message.getAddress(), decodedMessage.getAddress());        
   }
   
   public void testBindingQueryRequest() throws Exception
   {
      SessionBindingQueryMessage message = new SessionBindingQueryMessage(randomString());

      AbstractPacketCodec codec = new SessionBindingQueryMessageCodec();
      
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionBindingQueryMessage);
      SessionBindingQueryMessage decodedMessage = (SessionBindingQueryMessage)decodedPacket;
      assertEquals(SESS_BINDINGQUERY, decodedMessage.getType());
      
      assertEquals(message.getAddress(), decodedMessage.getAddress());        
   }
   
   public void testBindingQueryResponse() throws Exception
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

      AbstractPacket decodedPacket = codec.decode(buffer);

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

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionDeleteQueueMessage);
      SessionDeleteQueueMessage decodedMessage = (SessionDeleteQueueMessage)decodedPacket;
      assertEquals(SESS_DELETE_QUEUE, decodedMessage.getType());
      
      assertEquals(message.getQueueName(), decodedMessage.getQueueName());        
   }
   
   
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private SimpleRemotingBuffer encode(AbstractPacket packet,
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
