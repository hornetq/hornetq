/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat.test.unit;

import junit.framework.TestCase;
import org.apache.mina.common.IoBuffer;
import org.jboss.jms.client.impl.Ack;
import org.jboss.jms.client.impl.AckImpl;
import org.jboss.jms.client.impl.Cancel;
import org.jboss.jms.client.impl.CancelImpl;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.tx.ClientTransaction;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.impl.DestinationImpl;
import org.jboss.messaging.core.impl.MessageImpl;
import org.jboss.messaging.core.remoting.codec.*;
import static org.jboss.messaging.core.remoting.codec.AbstractPacketCodec.*;
import static org.jboss.messaging.core.remoting.codec.SendTransactionMessageCodec.encodeTransactionRequest;
import org.jboss.messaging.core.remoting.impl.mina.MinaPacketCodec.*;
import org.jboss.messaging.core.remoting.impl.mina.PacketCodecFactory;
import org.jboss.messaging.core.remoting.wireformat.*;
import static org.jboss.messaging.core.remoting.wireformat.AbstractPacket.NO_ID_SET;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.*;
import static org.jboss.messaging.core.remoting.wireformat.test.unit.CodecAssert.*;
import org.jboss.messaging.core.tx.MessagingXid;
import static org.jboss.messaging.test.unit.RandomUtil.*;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class PacketTypeTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static void addVersion(AbstractPacket packet)
   {
      byte version = (byte) 19;
      packet.setVersion(version);
   }

   private static ByteBuffer encode(int length, Object... args)
   {
      ByteBuffer buffer = ByteBuffer.allocate(length);
      for (Object arg : args)
      {
         if (arg instanceof Byte)
            buffer.put(((Byte)arg).byteValue());
         else if (arg instanceof Boolean)
         {
            Boolean bool = (Boolean) arg;
            buffer.put(bool ? TRUE : FALSE);
         }
         else if (arg instanceof Integer)
            buffer.putInt(((Integer)arg).intValue());
         else if (arg instanceof Long)
            buffer.putLong(((Long)arg).longValue());
         else if (arg instanceof Float)
            buffer.putFloat(((Float)arg).floatValue());
         else if (arg instanceof String)
            putNullableString((String)arg, buffer);
         else if (arg == null)
            putNullableString(null, buffer);
         else if (arg instanceof byte[])
         {
            byte[] b = (byte[])arg;
            buffer.putInt(b.length);
            buffer.put(b);
         }
         else if (arg instanceof long[])
         {
            long[] longs = (long[])arg;
            for (long l : longs)
            {
               buffer.putLong(l);
            }
         }
         else if (arg instanceof Cancel[])
         {
            Cancel[] cancels = (Cancel[])arg;
            for (Cancel cancel : cancels)
            {
               buffer.putLong(cancel.getDeliveryId());
               buffer.putInt(cancel.getDeliveryCount());
               buffer.put(cancel.isExpired() ? TRUE : FALSE);
               buffer.put(cancel.isReachedMaxDeliveryAttempts() ? TRUE : FALSE);
            }
         }

         else
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
      assertEquals(buffer.get(), packet.getVersion());

      String targetID = packet.getTargetID();
      if (NO_ID_SET.equals(packet.getTargetID()))
         targetID = null;
      String callbackID = packet.getCallbackID();
      if (NO_ID_SET.equals(packet.getCallbackID()))
         callbackID = null;

      int headerLength = LONG_LENGTH + sizeof(targetID) + sizeof(callbackID);
      assertEquals(buffer.getInt(), headerLength);
      assertEquals(buffer.getLong(), packet.getCorrelationID());

      String bufferTargetID = buffer.getNullableString();
      if (bufferTargetID == null)
         bufferTargetID = NO_ID_SET;
      String bufferCallbackID = buffer.getNullableString();
      if (bufferCallbackID == null)
         bufferCallbackID = NO_ID_SET;

      assertEquals(bufferTargetID, packet.getTargetID());
      assertEquals(bufferCallbackID, packet.getCallbackID());
   }

   private static void checkHeaderBytes(AbstractPacket packet, ByteBuffer actual)
   {
      String targetID = (packet.getTargetID().equals(NO_ID_SET)? null : packet.getTargetID());
      String callbackID = (packet.getCallbackID().equals(NO_ID_SET)? null : packet.getCallbackID());

      int headerLength = LONG_LENGTH + sizeof(targetID) + sizeof(callbackID);
      ByteBuffer expected = ByteBuffer.allocate(1 + 1 + INT_LENGTH + headerLength);
      expected.put(packet.getType().byteValue());
      expected.put(packet.getVersion());
      
      expected.putInt(headerLength);
      expected.putLong(packet.getCorrelationID());
      putNullableString(targetID, expected);
      putNullableString(callbackID, expected);
      expected.flip();

      assertEqualsByteArrays(expected.remaining(), expected.array(), actual.array());
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
      packet.setVersion(randomByte());
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
      assertEquals(packet.getVersion(), p.getVersion());
      assertEquals(packet.getCallbackID(), p.getCallbackID());
      assertEquals(packet.getCorrelationID(), p.getCorrelationID());
      assertEquals(packet.getTargetID(), p.getTargetID());
   }

   public void testJMSExceptionMessage() throws Exception
   {
      JMSException e = new InvalidDestinationException(
            "testJMSExceptionMessage");
      JMSExceptionMessage message = new JMSExceptionMessage(e);
      addVersion(message);

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

   public void testTextPacket() throws Exception
   {
      TextPacket packet = new TextPacket("testTextPacket");
      addVersion(packet);
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
            remotingSessionID, clientVMID, username, password, prefetchSize, dupsOkBatchSize, null);
      addVersion(request);
      
      AbstractPacketCodec<CreateConnectionRequest> codec = new ConnectionFactoryCreateConnectionRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, version, remotingSessionID, clientVMID, username, password, prefetchSize, dupsOkBatchSize, clientID);      
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
      addVersion(response);
      
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

   

   public void testUpdateCallbackMessage() throws Exception
   {
      UpdateCallbackMessage message = new UpdateCallbackMessage(randomString(),
            randomString(), true);
      addVersion(message);
      
      AbstractPacketCodec codec = new UpdateCallbackMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getRemotingSessionID(), message.getClientVMID(), message.isAdd());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof UpdateCallbackMessage);

      UpdateCallbackMessage decodedMessage = (UpdateCallbackMessage) decodedPacket;
      assertEquals(MSG_UPDATECALLBACK, decodedMessage.getType());
      assertEquals(message.getRemotingSessionID(), decodedMessage
            .getRemotingSessionID());
      assertEquals(message.getClientVMID(), decodedMessage.getClientVMID());
      assertEquals(message.isAdd(), decodedMessage.isAdd());
   }

   public void testCreateSessionRequest() throws Exception
   {
      CreateSessionRequest request = new CreateSessionRequest(true, 0, false);
      addVersion(request);
      
      AbstractPacketCodec codec = new CreateSessionRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.isTransacted(), request.getAcknowledgementMode(), request.isXA());
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
      addVersion(response);
      
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
      SendMessage packet = new SendMessage(new MessageImpl(), 
            randomLong());
      addVersion(packet);
      
      AbstractPacketCodec codec = new SendMessageCodec();
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      checkBody(buffer, encodeMessage(packet.getMessage()), packet.getSequence());
      buffer.rewind();

      AbstractPacket p = codec.decode(buffer);

      assertTrue(p instanceof SendMessage);
      SendMessage decodedPacket = (SendMessage) p;
      assertEquals(MSG_SENDMESSAGE, decodedPacket.getType());
      assertEquals(packet.getSequence(), decodedPacket.getSequence());
      assertEquals(packet.getMessage().getMessageID(), decodedPacket
            .getMessage().getMessageID());
   }

   public void testCreateConsumerRequest() throws Exception
   {
      Destination destination = new DestinationImpl(DestinationType.QUEUE, "testCreateConsumerRequest", false);
      CreateConsumerRequest request = new CreateConsumerRequest(destination,
            "color = 'red'", false, "subscription", false);
      addVersion(request);
      
      AbstractPacketCodec codec = new CreateConsumerRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, AbstractPacketCodec.encode(destination), request.getSelector(),
            request.isNoLocal(), request.getSubscriptionName(), request.isConnectionConsumer());
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
            .isConnectionConsumer());;
   }

   public void testCreateDestinationRequest() throws Exception
   {
      CreateDestinationRequest request = new CreateDestinationRequest(
            "testCreateDestinationRequest", false);
      addVersion(request);
      
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
      addVersion(response);
      
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
      JBossTopic destination = new JBossTopic("testCreateDestinationResponseForTopic");

      CreateDestinationResponse response = new CreateDestinationResponse(
            destination);
      addVersion(response);
      
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
      addVersion(response);
      
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
      addVersion(packet);

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
      addVersion(packet);

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
      ChangeRateMessage message = new ChangeRateMessage(0.63f);
      addVersion(message);
      AbstractPacketCodec codec = new ChangeRateMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, message.getRate());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ChangeRateMessage);
      ChangeRateMessage decodedMessage = (ChangeRateMessage) decodedPacket;
      assertEquals(MSG_CHANGERATE, decodedMessage.getType());
      assertEquals(message.getRate(), decodedMessage.getRate());
   }

   public void testDeliverMessage() throws Exception
   {
      Message msg = new MessageImpl();
      DeliverMessage message = new DeliverMessage(msg, randomString(),
            randomLong(), 23);
      addVersion(message);
      
      AbstractPacketCodec codec = new DeliverMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, encodeMessage(msg), message.getConsumerID(), 
            message.getDeliveryID(), message.getDeliveryCount());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof DeliverMessage);
      DeliverMessage decodedMessage = (DeliverMessage) decodedPacket;
      assertEquals(MSG_DELIVERMESSAGE, decodedMessage.getType());
      assertEquals(message.getMessage().getMessageID(), decodedMessage
            .getMessage().getMessageID());
      assertEquals(message.getConsumerID(), decodedMessage.getConsumerID());
      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
      assertEquals(message.getDeliveryCount(), decodedMessage
            .getDeliveryCount());
   }

   public void testAcknowledgeDeliveryRequest() throws Exception
   {
      AcknowledgeDeliveryRequest request = new AcknowledgeDeliveryRequest(
            randomLong());
      addVersion(request);
      
      AbstractPacketCodec codec = new AcknowledgeDeliveryRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.getDeliveryID());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof AcknowledgeDeliveryRequest);
      AcknowledgeDeliveryRequest decodedRequest = (AcknowledgeDeliveryRequest) decodedPacket;
      assertEquals(REQ_ACKDELIVERY, decodedRequest.getType());
      assertEquals(request.getDeliveryID(), decodedRequest.getDeliveryID());
   }

   public void testAcknowledgeDeliveriesRequest() throws Exception
   {
      List<Ack> acks = new ArrayList<Ack>();
      acks.add(new AckImpl(randomLong()));
      acks.add(new AckImpl(randomLong()));
      acks.add(new AckImpl(randomLong()));
      AcknowledgeDeliveriesMessage request = new AcknowledgeDeliveriesMessage(
            acks);
      addVersion(request);
      
      AcknowledgeDeliveriesRequestCodec codec = new AcknowledgeDeliveriesRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, AcknowledgeDeliveriesRequestCodec.convert(acks));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof AcknowledgeDeliveriesMessage);
      AcknowledgeDeliveriesMessage decodedRequest = (AcknowledgeDeliveriesMessage) decodedPacket;
      assertEquals(MSG_ACKDELIVERIES, decodedRequest.getType());
      assertEqualsAcks(request.getAcks(), decodedRequest.getAcks());
   }

   public void testAcknowledgeDeliveryResponse() throws Exception
   {
      AcknowledgeDeliveryResponse response = new AcknowledgeDeliveryResponse(
            true);
      addVersion(response);
      
      AbstractPacketCodec codec = new AcknowledgeDeliveryResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.isAcknowledged());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof AcknowledgeDeliveryResponse);
      AcknowledgeDeliveryResponse decodedResponse = (AcknowledgeDeliveryResponse) decodedPacket;
      assertEquals(RESP_ACKDELIVERY, decodedResponse.getType());
      assertEquals(response.isAcknowledged(), decodedResponse.isAcknowledged());

   }

   public void testClosingRequest() throws Exception
   {
      ClosingRequest request = new ClosingRequest(randomLong());
      addVersion(request);
      
      AbstractPacketCodec codec = new ClosingRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, request.getSequence());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ClosingRequest);
      ClosingRequest decodedRequest = (ClosingRequest) decodedPacket;
      assertEquals(REQ_CLOSING, decodedRequest.getType());
      assertEquals(request.getSequence(), decodedRequest.getSequence());
   }

   public void testClosingResponse() throws Exception
   {
      ClosingResponse response = new ClosingResponse(randomLong());
      addVersion(response);
      
      AbstractPacketCodec codec = new ClosingResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, response.getID());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ClosingResponse);
      ClosingResponse decodedRequest = (ClosingResponse) decodedPacket;
      assertEquals(RESP_CLOSING, decodedRequest.getType());
      assertEquals(response.getID(), decodedRequest.getID());
   }

   public void testCloseMessage() throws Exception
   {
      CloseMessage message = new CloseMessage();
      addVersion(message);

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

   public void testSendTransactionMessage() throws Exception
   {
      ClientTransaction tx = new ClientTransaction();
      MessagingXid xid = new MessagingXid(randomBytes(), randomInt(), randomBytes());
      TransactionRequest tr = new TransactionRequest(
            TransactionRequest.ONE_PHASE_COMMIT_REQUEST, xid, tx);
      SendTransactionMessage message = new SendTransactionMessage(tr);
      addVersion(message);
      
      AbstractPacketCodec codec = new SendTransactionMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, encodeTransactionRequest(tr));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SendTransactionMessage);
      SendTransactionMessage decodedMessage = (SendTransactionMessage) decodedPacket;
      assertEquals(MSG_SENDTRANSACTION, decodedMessage.getType());
      TransactionRequest expectedTxReq = message.getTransactionRequest();
      TransactionRequest actualTxReq = decodedMessage.getTransactionRequest();
      assertEquals(expectedTxReq.getRequestType(), actualTxReq.getRequestType());
      assertEquals(expectedTxReq.getXid(), actualTxReq.getXid());
   }

   public void testGetPreparedTransactionsRequest() throws Exception
   {
      GetPreparedTransactionsRequest request = new GetPreparedTransactionsRequest();
      addVersion(request);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            REQ_GETPREPAREDTRANSACTIONS, GetPreparedTransactionsRequest.class);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBodyIsEmpty(buffer);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof GetPreparedTransactionsRequest);
      assertEquals(REQ_GETPREPAREDTRANSACTIONS, decodedPacket.getType());
   }

   public void testGetPreparedTransactionsResponse() throws Exception
   {
      MessagingXid[] xids = new MessagingXid[] {
            new MessagingXid(randomBytes(), 23, randomBytes()),
            new MessagingXid(randomBytes(), 33, randomBytes()),
            new MessagingXid(randomBytes(), 91, randomBytes()) };
      GetPreparedTransactionsResponse response = new GetPreparedTransactionsResponse(
            xids);
      addVersion(response);
      
      AbstractPacketCodec codec = new GetPreparedTransactionsResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, xids.length, GetPreparedTransactionsResponseCodec.encode(xids));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof GetPreparedTransactionsResponse);
      GetPreparedTransactionsResponse decodedResponse = (GetPreparedTransactionsResponse) decodedPacket;
      assertEquals(RESP_GETPREPAREDTRANSACTIONS, decodedResponse.getType());
      assertSameXids(response.getXids(), decodedResponse.getXids());
   }

   public void testGetClientIDRequest() throws Exception
   {
      GetClientIDRequest request = new GetClientIDRequest();
      addVersion(request);

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
      addVersion(response);
      
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
      addVersion(message);
      
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
 
   public void testCancelDeliveryMessage() throws Exception
   {
      Cancel cancel = new CancelImpl(randomLong(), randomInt(), true, false);
      CancelDeliveryMessage message = new CancelDeliveryMessage(cancel);
      addVersion(message);
      AbstractPacketCodec codec = new CancelDeliveryMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, cancel.getDeliveryId(), cancel.getDeliveryCount(), cancel.isExpired(),
            cancel.isReachedMaxDeliveryAttempts());
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CancelDeliveryMessage);
      CancelDeliveryMessage decodedMessage = (CancelDeliveryMessage) decodedPacket;
      assertEquals(MSG_CANCELDELIVERY, decodedMessage.getType());
      Cancel expected = message.getCancel();
      Cancel actual = decodedMessage.getCancel();
      assertEquals(expected.getDeliveryId(), actual.getDeliveryId());
      assertEquals(expected.getDeliveryCount(), actual.getDeliveryCount());
      assertEquals(expected.isExpired(), actual.isExpired());
      assertEquals(expected.isReachedMaxDeliveryAttempts(), actual
            .isReachedMaxDeliveryAttempts());
   }

   public void testCancelDeliveriesMessage() throws Exception
   {
      List<Cancel> cancels = new ArrayList<Cancel>();
      cancels.add(new CancelImpl(randomLong(), 23, true, false));
      cancels.add(new CancelImpl(randomLong(), 33, false, true));
      CancelDeliveriesMessage message = new CancelDeliveriesMessage(cancels);
      addVersion(message);
      
      AbstractPacketCodec codec = new CancelDeliveriesMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      checkBody(buffer, cancels.size(), cancels.toArray(new Cancel[cancels.size()]));
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CancelDeliveriesMessage);
      CancelDeliveriesMessage decodedMessage = (CancelDeliveriesMessage) decodedPacket;
      assertEquals(MSG_CANCELDELIVERIES, decodedMessage.getType());
      assertEqualsCancels(message.getCancels(), decodedMessage.getCancels());
   }

   public void testCreateBrowserRequest() throws Exception
   {
      Destination destination = new DestinationImpl(DestinationType.QUEUE, "testCreateBrowserRequest", false);
      CreateBrowserRequest request = new CreateBrowserRequest(destination,
            "color = 'red'");
      addVersion(request);
      
      AbstractPacketCodec codec = new CreateBrowserRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      checkBody(buffer, AbstractPacketCodec.encode(destination), request.getSelector());
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
      addVersion(response);
      
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
      addVersion(message);

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
      addVersion(request);

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
      addVersion(response);
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
      addVersion(request);

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
      addVersion(response);
      
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
      addVersion(request);
      
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
      addVersion(response);
      
      AbstractPacketCodec codec = new BrowserNextMessageBlockResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      checkBody(buffer, messages.length, BrowserNextMessageBlockResponseCodec.encode(messages));
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
      addVersion(message);
      
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
      Destination destination = new DestinationImpl(DestinationType.QUEUE, "testAddTemporaryDestinationMessage", false);
      AddTemporaryDestinationMessage message = new AddTemporaryDestinationMessage(
            destination);
      addVersion(message);
      
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
      Destination destination = new DestinationImpl(DestinationType.QUEUE, "testDeleteTemporaryDestinationMessage", false);;
      DeleteTemporaryDestinationMessage message = new DeleteTemporaryDestinationMessage(
            destination);
      addVersion(message);
      
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
