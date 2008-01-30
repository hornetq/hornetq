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
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ACKNOWLEDGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ADDTEMPORARYDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_BROWSER_RESET;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCEL;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CHANGERATE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_COMMIT;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELETETEMPORARYDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELIVERMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_JMSEXCEPTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_RECOVER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ROLLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SETSESSIONID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_STARTCONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_STOPCONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UNSUBSCRIBE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.NULL;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.PING;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.PONG;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_BROWSER_NEXTMESSAGEBLOCK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATESESSION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_NEXTMESSAGEBLOCK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATECONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATEDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATESESSION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.TEXT;
import static org.jboss.messaging.core.remoting.wireformat.test.unit.CodecAssert.assertEqualsByteArrays;
import static org.jboss.messaging.test.unit.RandomUtil.randomByte;
import static org.jboss.messaging.test.unit.RandomUtil.randomBytes;
import static org.jboss.messaging.test.unit.RandomUtil.randomLong;
import static org.jboss.messaging.test.unit.RandomUtil.randomString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import junit.framework.TestCase;

import org.apache.mina.common.IoBuffer;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.impl.DestinationImpl;
import org.jboss.messaging.core.impl.MessageImpl;
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
import org.jboss.messaging.core.remoting.impl.mina.PacketCodecFactory;
import org.jboss.messaging.core.remoting.impl.mina.MinaPacketCodec.BufferWrapper;
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
 * 
 * @version <tt>$Revision$</tt>
 */
public class PacketTypeTest extends TestCase
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

   public void testJMSExceptionMessage() throws Exception
   {
      JMSException e = new InvalidDestinationException(
            "testJMSExceptionMessage");
      JMSExceptionMessage message = new JMSExceptionMessage(e);

      AbstractPacketCodec<JMSExceptionMessage> codec = new JMSExceptionMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, JMSExceptionMessageCodec.encodeJMSException(e));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof JMSExceptionMessage);
      JMSExceptionMessage decodedMessage = (JMSExceptionMessage) decodedPacket;

      assertEquals(MSG_JMSEXCEPTION, decodedMessage.getType());
      assertEquals(message.getException().getMessage(), decodedMessage
            .getException().getMessage());
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
      SetSessionIDMessage message = new SetSessionIDMessage(randomString());

      AbstractPacketCodec codec = new SetSessionIDMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getSessionID());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SetSessionIDMessage);
      SetSessionIDMessage decodedMessage = (SetSessionIDMessage) decodedPacket;
      assertEquals(MSG_SETSESSIONID, decodedMessage.getType());
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
      int dupsOkBatchSize = 150;
      String clientID = null;

      CreateConnectionRequest request = new CreateConnectionRequest(version,
            remotingSessionID, clientVMID, username, password, prefetchSize,
            dupsOkBatchSize, null);

      AbstractPacketCodec<CreateConnectionRequest> codec = new ConnectionFactoryCreateConnectionRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, version, remotingSessionID, clientVMID, username,
            password, prefetchSize, dupsOkBatchSize, clientID);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateConnectionRequest);
      CreateConnectionRequest decodedRequest = (CreateConnectionRequest) decodedPacket;

      assertEquals(REQ_CREATECONNECTION, decodedPacket.getType());
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

      AbstractPacketCodec<CreateConnectionResponse> codec = new ConnectionFactoryCreateConnectionResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getConnectionID());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateConnectionResponse);
      CreateConnectionResponse decodedResponse = (CreateConnectionResponse) decodedPacket;
      assertEquals(RESP_CREATECONNECTION, decodedResponse.getType());
      assertEquals(response.getConnectionID(), decodedResponse
            .getConnectionID());
   }

   public void testCreateSessionRequest() throws Exception
   {
      CreateSessionRequest request = new CreateSessionRequest(true, 0, false);

      AbstractPacketCodec codec = new CreateSessionRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.isTransacted(), request
            .getAcknowledgementMode(), request.isXA());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateSessionRequest);
      CreateSessionRequest decodedRequest = (CreateSessionRequest) decodedPacket;
      assertEquals(REQ_CREATESESSION, decodedRequest.getType());
      assertEquals(request.isTransacted(), decodedRequest.isTransacted());
      assertEquals(request.getAcknowledgementMode(), decodedRequest
            .getAcknowledgementMode());
      assertEquals(request.isXA(), decodedRequest.isXA());
   }

   public void testCreateSessionResponse() throws Exception
   {
      CreateSessionResponse response = new CreateSessionResponse(
            randomString(), 23);

      AbstractPacketCodec codec = new CreateSessionResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getSessionID(), response.getDupsOKBatchSize());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateSessionResponse);
      CreateSessionResponse decodedResponse = (CreateSessionResponse) decodedPacket;
      assertEquals(RESP_CREATESESSION, decodedResponse.getType());
      assertEquals(response.getSessionID(), decodedResponse.getSessionID());
      assertEquals(response.getDupsOKBatchSize(), decodedResponse
            .getDupsOKBatchSize());
   }

   public void testSendMessage() throws Exception
   {
      Destination originalDestination = new DestinationImpl(DestinationType.QUEUE, java.util.UUID.randomUUID().toString(), true);
      SessionSendMessage packet = new SessionSendMessage(new MessageImpl(), originalDestination);

      AbstractPacketCodec codec = new SessionSendMessageCodec();
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBody(buffer, encodeMessage(packet.getMessage()), DestinationType.toInt(originalDestination.getType()),
                        originalDestination.isTemporary(), originalDestination.getName());
      buffer.rewind();

      AbstractPacket p = codec.decode(buffer);

      assertTrue(p instanceof SessionSendMessage);
      SessionSendMessage decodedPacket = (SessionSendMessage) p;
      assertEquals(MSG_SENDMESSAGE, decodedPacket.getType());
      assertEquals(packet.getMessage().getMessageID(), decodedPacket
            .getMessage().getMessageID());
      assertEquals(originalDestination.getName(), packet.getDestination().getName());
      assertEquals(DestinationType.QUEUE, packet.getDestination().getType());
      assertEquals(originalDestination.isTemporary(), packet.getDestination().isTemporary());
   }

   public void testCreateConsumerRequest() throws Exception
   {
      Destination destination = new DestinationImpl(DestinationType.QUEUE,
            "testCreateConsumerRequest", false);
      CreateConsumerRequest request = new CreateConsumerRequest(destination,
            "color = 'red'", false, "subscription", false);

      AbstractPacketCodec codec = new CreateConsumerRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, AbstractPacketCodec.encode(destination), request
            .getSelector(), request.isNoLocal(), request.getSubscriptionName(),
            request.isConnectionConsumer());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateConsumerRequest);
      CreateConsumerRequest decodedRequest = (CreateConsumerRequest) decodedPacket;
      assertEquals(REQ_CREATECONSUMER, decodedRequest.getType());
      assertEquals(request.getDestination(), decodedRequest.getDestination());
      assertEquals(request.getSelector(), decodedRequest.getSelector());
      assertEquals(request.isNoLocal(), decodedRequest.isNoLocal());
      assertEquals(request.getSubscriptionName(), decodedRequest
            .getSubscriptionName());
      assertEquals(request.isConnectionConsumer(), decodedRequest
            .isConnectionConsumer());
      ;
   }

   public void testCreateDestinationRequest() throws Exception
   {
      CreateDestinationRequest request = new CreateDestinationRequest(
            "testCreateDestinationRequest", false);

      AbstractPacketCodec codec = new CreateDestinationRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.getName(), request.isQueue());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateDestinationRequest);
      CreateDestinationRequest decodedRequest = (CreateDestinationRequest) decodedPacket;
      assertEquals(REQ_CREATEDESTINATION, decodedRequest.getType());
      assertEquals(request.getName(), decodedRequest.getName());
      assertEquals(request.isQueue(), decodedRequest.isQueue());
   }

   public void testCreateDestinationResponseForQueue() throws Exception
   {
      JBossQueue destination = new JBossQueue("testCreateQueueResponse");

      CreateDestinationResponse response = new CreateDestinationResponse(
            destination);

      AbstractPacketCodec codec = new CreateDestinationResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, AbstractPacketCodec.encodeJBossDestination(destination));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateDestinationResponse);
      CreateDestinationResponse decodedResponse = (CreateDestinationResponse) decodedPacket;
      assertEquals(RESP_CREATEDESTINATION, decodedResponse.getType());
      assertTrue(decodedResponse.getDestination() instanceof JBossQueue);
      assertEquals(response.getDestination(), decodedResponse.getDestination());
   }

   public void testCreateDestinationResponseForTopic() throws Exception
   {
      JBossTopic destination = new JBossTopic(
            "testCreateDestinationResponseForTopic");

      CreateDestinationResponse response = new CreateDestinationResponse(
            destination);

      AbstractPacketCodec codec = new CreateDestinationResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, AbstractPacketCodec.encodeJBossDestination(destination));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateDestinationResponse);
      CreateDestinationResponse decodedResponse = (CreateDestinationResponse) decodedPacket;
      assertEquals(RESP_CREATEDESTINATION, decodedResponse.getType());
      assertTrue(decodedResponse.getDestination() instanceof JBossTopic);
      assertEquals(response.getDestination(), decodedResponse.getDestination());
   }

   public void testCreateConsumerResponse() throws Exception
   {

      CreateConsumerResponse response = new CreateConsumerResponse(
            randomString(), 23, 42, randomLong());

      AbstractPacketCodec codec = new CreateConsumerResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getConsumerID(), response.getBufferSize(),
            response.getMaxDeliveries(), response.getRedeliveryDelay());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateConsumerResponse);
      CreateConsumerResponse decodedResponse = (CreateConsumerResponse) decodedPacket;
      assertEquals(RESP_CREATECONSUMER, decodedResponse.getType());
      assertEquals(response.getBufferSize(), decodedResponse.getBufferSize());
      assertEquals(response.getMaxDeliveries(), decodedResponse
            .getMaxDeliveries());
      assertEquals(response.getRedeliveryDelay(), decodedResponse
            .getRedeliveryDelay());
   }

   public void testStartConnectionMessage() throws Exception
   {
      StartConnectionMessage packet = new StartConnectionMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            MSG_STARTCONNECTION, StartConnectionMessage.class);
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof StartConnectionMessage);
      assertEquals(MSG_STARTCONNECTION, decodedPacket.getType());
   }

   public void testStopConnectionMessage() throws Exception
   {
      StopConnectionMessage packet = new StopConnectionMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            MSG_STOPCONNECTION, StopConnectionMessage.class);
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof StopConnectionMessage);
      assertEquals(MSG_STOPCONNECTION, decodedPacket.getType());
   }

   public void testChangeRateMessage() throws Exception
   {
      ConsumerChangeRateMessage message = new ConsumerChangeRateMessage(0.63f);
      AbstractPacketCodec codec = new ConsumerChangeRateMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getRate());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ConsumerChangeRateMessage);
      ConsumerChangeRateMessage decodedMessage = (ConsumerChangeRateMessage) decodedPacket;
      assertEquals(MSG_CHANGERATE, decodedMessage.getType());
      assertEquals(message.getRate(), decodedMessage.getRate());
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
      assertEquals(MSG_DELIVERMESSAGE, decodedMessage.getType());
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
      assertEquals(MSG_ACKNOWLEDGE, decodedMessage.getType());
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
      assertEquals(MSG_CANCEL, decodedMessage.getType());
      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
      assertEquals(message.isExpired(), decodedMessage.isExpired());
   }

   public void testSessionCommitMessage() throws Exception
   {
      SessionCommitMessage message = new SessionCommitMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            MSG_COMMIT, SessionCommitMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);

      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionCommitMessage);
      assertEquals(MSG_COMMIT, decodedPacket.getType());
   }

   public void testSessionRollbackMessage() throws Exception
   {
      SessionRollbackMessage message = new SessionRollbackMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            MSG_ROLLBACK, SessionRollbackMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);

      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionRollbackMessage);
      assertEquals(MSG_ROLLBACK, decodedPacket.getType());
   }
   
   public void testSessionRecoverMessage() throws Exception
   {
      SessionRecoverMessage message = new SessionRecoverMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            MSG_RECOVER, SessionRecoverMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);

      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SessionRecoverMessage);
      assertEquals(MSG_RECOVER, decodedPacket.getType());
   }

   public void testClosingMessage() throws Exception
   {
      ClosingMessage request = new ClosingMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            PacketType.MSG_CLOSING, ClosingMessage.class);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);

      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ClosingMessage);
      ClosingMessage decodedRequest = (ClosingMessage) decodedPacket;
      assertEquals(PacketType.MSG_CLOSING, decodedRequest.getType());
   }

   public void testCloseMessage() throws Exception
   {
      CloseMessage message = new CloseMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            MSG_CLOSE, CloseMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CloseMessage);
      CloseMessage decodedMessage = (CloseMessage) decodedPacket;
      assertEquals(MSG_CLOSE, decodedMessage.getType());
   }

   public void testGetClientIDRequest() throws Exception
   {
      GetClientIDRequest request = new GetClientIDRequest();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            REQ_GETCLIENTID, GetClientIDRequest.class);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof GetClientIDRequest);
      assertEquals(REQ_GETCLIENTID, decodedPacket.getType());
   }

   public void testGetClientIDResponse() throws Exception
   {
      GetClientIDResponse response = new GetClientIDResponse(randomString());

      AbstractPacketCodec codec = new GetClientIDResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getClientID());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof GetClientIDResponse);
      GetClientIDResponse decodedResponse = (GetClientIDResponse) decodedPacket;
      assertEquals(RESP_GETCLIENTID, decodedResponse.getType());
      assertEquals(response.getClientID(), decodedResponse.getClientID());
   }

   public void testSetClientIDMessage() throws Exception
   {
      SetClientIDMessage message = new SetClientIDMessage(randomString());

      AbstractPacketCodec codec = new SetClientIDMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getClientID());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SetClientIDMessage);
      SetClientIDMessage decodedMessage = (SetClientIDMessage) decodedPacket;
      assertEquals(MSG_SETCLIENTID, decodedMessage.getType());
      assertEquals(message.getClientID(), decodedMessage.getClientID());
   }

   public void testCreateBrowserRequest() throws Exception
   {
      Destination destination = new DestinationImpl(DestinationType.QUEUE,
            "testCreateBrowserRequest", false);
      CreateBrowserRequest request = new CreateBrowserRequest(destination,
            "color = 'red'");

      AbstractPacketCodec codec = new CreateBrowserRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, AbstractPacketCodec.encode(destination), request
            .getSelector());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateBrowserRequest);
      CreateBrowserRequest decodedRequest = (CreateBrowserRequest) decodedPacket;
      assertEquals(REQ_CREATEBROWSER, decodedRequest.getType());
      assertEquals(request.getDestination(), decodedRequest.getDestination());
      assertEquals(request.getSelector(), decodedRequest.getSelector());
   }

   public void testCreateBrowserResponse() throws Exception
   {
      CreateBrowserResponse response = new CreateBrowserResponse(randomString());

      AbstractPacketCodec codec = new CreateBrowserResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getBrowserID());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateBrowserResponse);
      CreateBrowserResponse decodedResponse = (CreateBrowserResponse) decodedPacket;
      assertEquals(RESP_CREATEBROWSER, decodedResponse.getType());
      assertEquals(response.getBrowserID(), decodedResponse.getBrowserID());
   }

   public void testBrowserResetMessage() throws Exception
   {
      BrowserResetMessage message = new BrowserResetMessage();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            MSG_BROWSER_RESET, BrowserResetMessage.class);
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BrowserResetMessage);
      assertEquals(MSG_BROWSER_RESET, decodedPacket.getType());
   }

   public void testBrowserHasNextMessageRequest() throws Exception
   {
      BrowserHasNextMessageRequest request = new BrowserHasNextMessageRequest();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            REQ_BROWSER_HASNEXTMESSAGE, BrowserHasNextMessageRequest.class);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BrowserHasNextMessageRequest);
      assertEquals(REQ_BROWSER_HASNEXTMESSAGE, decodedPacket.getType());
   }

   public void testBrowserHasNextMessageResponse() throws Exception
   {
      BrowserHasNextMessageResponse response = new BrowserHasNextMessageResponse(
            false);
      AbstractPacketCodec codec = new BrowserHasNextMessageResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.hasNext());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BrowserHasNextMessageResponse);
      BrowserHasNextMessageResponse decodedResponse = (BrowserHasNextMessageResponse) decodedPacket;
      assertEquals(RESP_BROWSER_HASNEXTMESSAGE, decodedResponse.getType());
      assertEquals(response.hasNext(), decodedResponse.hasNext());
   }

   public void testBrowserNextMessageRequest() throws Exception
   {
      BrowserNextMessageRequest request = new BrowserNextMessageRequest();

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            REQ_BROWSER_NEXTMESSAGE, BrowserNextMessageRequest.class);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BrowserNextMessageRequest);
      assertEquals(REQ_BROWSER_NEXTMESSAGE, decodedPacket.getType());
   }

   public void testBrowserNextMessageResponse() throws Exception
   {
      Message msg = new MessageImpl();
      BrowserNextMessageResponse response = new BrowserNextMessageResponse(msg);

      AbstractPacketCodec codec = new BrowserNextMessageResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, encodeMessage(msg));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BrowserNextMessageResponse);
      BrowserNextMessageResponse decodedResponse = (BrowserNextMessageResponse) decodedPacket;
      assertEquals(RESP_BROWSER_NEXTMESSAGE, decodedResponse.getType());
      assertEquals(response.getMessage().getMessageID(), decodedResponse
            .getMessage().getMessageID());
   }

   public void testBrowserNextMessageBlockRequest() throws Exception
   {
      BrowserNextMessageBlockRequest request = new BrowserNextMessageBlockRequest(
            randomLong());

      AbstractPacketCodec codec = new BrowserNextMessageBlockRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.getMaxMessages());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BrowserNextMessageBlockRequest);
      BrowserNextMessageBlockRequest decodedRequest = (BrowserNextMessageBlockRequest) decodedPacket;
      assertEquals(REQ_BROWSER_NEXTMESSAGEBLOCK, decodedPacket.getType());
      assertEquals(request.getMaxMessages(), decodedRequest.getMaxMessages());
   }

   public void testBrowserNextMessageBlockResponse() throws Exception
   {
      Message[] messages = new Message[] { new MessageImpl(), new MessageImpl() };
      BrowserNextMessageBlockResponse response = new BrowserNextMessageBlockResponse(
            messages);

      AbstractPacketCodec codec = new BrowserNextMessageBlockResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, messages.length, BrowserNextMessageBlockResponseCodec
            .encode(messages));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BrowserNextMessageBlockResponse);
      BrowserNextMessageBlockResponse decodedResponse = (BrowserNextMessageBlockResponse) decodedPacket;
      assertEquals(RESP_BROWSER_NEXTMESSAGEBLOCK, decodedResponse.getType());
      assertEquals(response.getMessages()[0].getMessageID(), decodedResponse
            .getMessages()[0].getMessageID());
      assertEquals(response.getMessages()[1].getMessageID(), decodedResponse
            .getMessages()[1].getMessageID());
   }

   public void testUnsubscribeMessage() throws Exception
   {
      UnsubscribeMessage message = new UnsubscribeMessage(
            "testUnsubscribeMessage");

      AbstractPacketCodec codec = new UnsubscribeMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getSubscriptionName());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof UnsubscribeMessage);
      UnsubscribeMessage decodedMessage = (UnsubscribeMessage) decodedPacket;
      assertEquals(MSG_UNSUBSCRIBE, decodedMessage.getType());
      assertEquals(decodedMessage.getSubscriptionName(), message
            .getSubscriptionName());
   }

   public void testAddTemporaryDestinationMessage() throws Exception
   {
      Destination destination = new DestinationImpl(DestinationType.QUEUE,
            "testAddTemporaryDestinationMessage", false);
      AddTemporaryDestinationMessage message = new AddTemporaryDestinationMessage(
            destination);

      AbstractPacketCodec codec = new AddTemporaryDestinationMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, AbstractPacketCodec.encode(destination));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof AddTemporaryDestinationMessage);
      AddTemporaryDestinationMessage decodedMessage = (AddTemporaryDestinationMessage) decodedPacket;
      assertEquals(MSG_ADDTEMPORARYDESTINATION, decodedMessage.getType());
      assertEquals(message.getDestination(), decodedMessage.getDestination());
   }

   public void testDeleteTemporaryDestinationMessage() throws Exception
   {
      Destination destination = new DestinationImpl(DestinationType.QUEUE,
            "testDeleteTemporaryDestinationMessage", false);
      ;
      DeleteTemporaryDestinationMessage message = new DeleteTemporaryDestinationMessage(
            destination);

      AbstractPacketCodec codec = new DeleteTemporaryDestinationMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof DeleteTemporaryDestinationMessage);
      DeleteTemporaryDestinationMessage decodedMessage = (DeleteTemporaryDestinationMessage) decodedPacket;
      assertEquals(MSG_DELETETEMPORARYDESTINATION, decodedMessage.getType());
      assertEquals(message.getDestination(), decodedMessage.getDestination());
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
