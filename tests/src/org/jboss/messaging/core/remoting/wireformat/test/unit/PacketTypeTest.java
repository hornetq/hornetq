/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat.test.unit;

import static org.jboss.messaging.core.remoting.codec.AbstractPacketCodec.LONG_LENGTH;
import static org.jboss.messaging.core.remoting.codec.AbstractPacketCodec.sizeof;
import static org.jboss.messaging.core.remoting.wireformat.AbstractPacket.NO_ID_SET;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ACKDELIVERIES;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ADDTEMPORARYDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_BROWSER_RESET;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCELDELIVERIES;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCELDELIVERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CHANGERATE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELETETEMPORARYDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELIVERMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_JMSEXCEPTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_RECOVERDELIVERIES;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDTRANSACTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_STARTCONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_STOPCONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UNSUBSCRIBE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UPDATECALLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.NULL;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_ACKDELIVERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_BROWSER_NEXTMESSAGEBLOCK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CLOSING;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATESESSION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETCLIENTAOPSTACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETPREPAREDTRANSACTIONS;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETTOPOLOGY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_IDBLOCK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_ACKDELIVERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_NEXTMESSAGEBLOCK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CLOSING;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATECONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATEDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATESESSION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETCLIENTAOPSTACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETPREPAREDTRANSACTIONS;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETTOPOLOGY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_IDBLOCK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.TEXT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import junit.framework.TestCase;

import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.delegate.Ack;
import org.jboss.jms.delegate.Cancel;
import org.jboss.jms.delegate.DefaultAck;
import org.jboss.jms.delegate.DefaultCancel;
import org.jboss.jms.delegate.DeliveryRecovery;
import org.jboss.jms.delegate.TopologyResult;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.tx.ClientTransaction;
import org.jboss.jms.tx.MessagingXid;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.remoting.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.codec.AcknowledgeDeliveriesRequestCodec;
import org.jboss.messaging.core.remoting.codec.AcknowledgeDeliveryRequestCodec;
import org.jboss.messaging.core.remoting.codec.AcknowledgeDeliveryResponseCodec;
import org.jboss.messaging.core.remoting.codec.AddTemporaryDestinationMessageCodec;
import org.jboss.messaging.core.remoting.codec.BrowserHasNextMessageResponseCodec;
import org.jboss.messaging.core.remoting.codec.BrowserNextMessageBlockRequestCodec;
import org.jboss.messaging.core.remoting.codec.BrowserNextMessageBlockResponseCodec;
import org.jboss.messaging.core.remoting.codec.BrowserNextMessageResponseCodec;
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
import org.jboss.messaging.core.remoting.codec.GetClientAOPStackResponseCodec;
import org.jboss.messaging.core.remoting.codec.GetClientIDResponseCodec;
import org.jboss.messaging.core.remoting.codec.GetPreparedTransactionsResponseCodec;
import org.jboss.messaging.core.remoting.codec.GetTopologyResponseCodec;
import org.jboss.messaging.core.remoting.codec.IDBlockRequestCodec;
import org.jboss.messaging.core.remoting.codec.IDBlockResponseCodec;
import org.jboss.messaging.core.remoting.codec.JMSExceptionMessageCodec;
import org.jboss.messaging.core.remoting.codec.RecoverDeliveriesMessageCodec;
import org.jboss.messaging.core.remoting.codec.RemotingBuffer;
import org.jboss.messaging.core.remoting.codec.SendMessageCodec;
import org.jboss.messaging.core.remoting.codec.SendTransactionMessageCodec;
import org.jboss.messaging.core.remoting.codec.SetClientIDMessageCodec;
import org.jboss.messaging.core.remoting.codec.TextPacketCodec;
import org.jboss.messaging.core.remoting.codec.UnsubscribeMessageCodec;
import org.jboss.messaging.core.remoting.codec.UpdateCallbackMessageCodec;
import org.jboss.messaging.core.remoting.integration.PacketCodecFactory;
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
import org.jboss.messaging.core.remoting.wireformat.GetClientAOPStackRequest;
import org.jboss.messaging.core.remoting.wireformat.GetClientAOPStackResponse;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDRequest;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDResponse;
import org.jboss.messaging.core.remoting.wireformat.GetPreparedTransactionsRequest;
import org.jboss.messaging.core.remoting.wireformat.GetPreparedTransactionsResponse;
import org.jboss.messaging.core.remoting.wireformat.GetTopologyRequest;
import org.jboss.messaging.core.remoting.wireformat.GetTopologyResponse;
import org.jboss.messaging.core.remoting.wireformat.IDBlockRequest;
import org.jboss.messaging.core.remoting.wireformat.IDBlockResponse;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.RecoverDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.SendMessage;
import org.jboss.messaging.core.remoting.wireformat.SendTransactionMessage;
import org.jboss.messaging.core.remoting.wireformat.SetClientIDMessage;
import org.jboss.messaging.core.remoting.wireformat.StartConnectionMessage;
import org.jboss.messaging.core.remoting.wireformat.StopConnectionMessage;
import org.jboss.messaging.core.remoting.wireformat.TextPacket;
import org.jboss.messaging.core.remoting.wireformat.UnsubscribeMessage;
import org.jboss.messaging.core.remoting.wireformat.UpdateCallbackMessage;
import org.jboss.messaging.util.Version;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class PacketTypeTest extends TestCase
{

   // Constants -----------------------------------------------------

   private static final Random random = new Random(System.currentTimeMillis());

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static void addVersion(AbstractPacket packet)
   {
      byte version = (byte)19;
      packet.setVersion(version);
   }

   private static String randomString()
   {
      return UUID.randomUUID().toString();
   }

   private static long randomLong()
   {
      return random.nextLong();
   }

   private static int randomInt()
   {
      return random.nextInt();
   }

   private static byte randomByte()
   {
      return Integer.valueOf(random.nextInt()).byteValue();
   }

   private static byte[] randomBytes()
   {
      return randomString().getBytes();
   }

   private static void checkHeader(RemotingBuffer buffer, AbstractPacket packet)
         throws Exception
   {
      assertEquals(buffer.get(), packet.getType().byteValue());
      assertEquals(buffer.get(), packet.getVersion());

      String targetID = packet.getTargetID();
      if (NO_ID_SET.equals(packet.getTargetID()))
            targetID = null;
      String callbackID = packet.getCallbackID();
      if (NO_ID_SET.equals(packet.getCallbackID()))
         callbackID = null;
           
      int headerLength = LONG_LENGTH + sizeof(targetID)
            + sizeof(callbackID);
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

   private static void assertEqualsAcks(List<Ack> expected, List<Ack> actual)
   {
      assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++)
      {
         assertEquals(expected.get(i).getDeliveryID(), actual.get(i)
               .getDeliveryID());
      }
   }

   private static void assertEqualsDeliveries(List<DeliveryRecovery> expected,
         List<DeliveryRecovery> actual)
   {
      assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++)
      {
         DeliveryRecovery expectedDelivery = expected.get(i);
         DeliveryRecovery actualDelivery = actual.get(i);
         assertEquals(expectedDelivery.getDeliveryID(), actualDelivery
               .getDeliveryID());
         assertEquals(expectedDelivery.getMessageID(), actualDelivery
               .getMessageID());
         assertEquals(expectedDelivery.getQueueName(), actualDelivery
               .getQueueName());
      }
   }

   private static void assertEqualsCancels(List<Cancel> expected,
         List<Cancel> actual)
   {
      assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++)
      {
         Cancel expectedCancel = expected.get(i);
         Cancel actualCancel = actual.get(i);
         assertEquals(expectedCancel.getDeliveryId(), actualCancel
               .getDeliveryId());
         assertEquals(expectedCancel.getDeliveryCount(), actualCancel
               .getDeliveryCount());
         assertEquals(expectedCancel.isExpired(), actualCancel.isExpired());
         assertEquals(expectedCancel.isReachedMaxDeliveryAttempts(),
               actualCancel.isReachedMaxDeliveryAttempts());
      }
   }

   private static void assertSameXids(MessagingXid[] expected,
         MessagingXid[] actual)
   {
      assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         MessagingXid expectedXid = expected[i];
         MessagingXid actualXid = actual[i];
         assertEqualsByteArrays(expectedXid.getBranchQualifier(), actualXid
               .getBranchQualifier());
         assertEquals(expectedXid.getFormatId(), actualXid.getFormatId());
         assertEqualsByteArrays(expectedXid.getGlobalTransactionId(), actualXid
               .getGlobalTransactionId());
      }
   }

   private static void assertEqualsByteArrays(byte[] expected, byte[] actual)
   {
      assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         assertEquals(expected[i], actual[i]);
      }
   }

   private static void assertSameTopology(TopologyResult expected,
         TopologyResult actual)
   {
      assertEquals(expected.getUniqueName(), actual.getUniqueName());
      assertEquals(expected.getFailoverMap(), actual.getFailoverMap());

      ClientConnectionFactoryDelegate[] expectedDelegates = expected
            .getDelegates();
      ClientConnectionFactoryDelegate[] actualDelegates = actual.getDelegates();

      assertEquals(expectedDelegates.length, actualDelegates.length);

      for (int i = 0; i < expectedDelegates.length; i++)
      {
         ClientConnectionFactoryDelegate expectedDelegate = expectedDelegates[i];
         ClientConnectionFactoryDelegate actualDelegate = actualDelegates[i];

         assertEquals(expectedDelegate.getID(), actualDelegate.getID());
         assertEquals(expectedDelegate.getName(), actualDelegate.getName());
      }
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
      // no body
      assertEquals(0, buffer.getInt());
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
      assertEquals(buffer.getInt(), sizeof(packet.getText()));
      assertEquals(buffer.getNullableString(), packet.getText());
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
      int failedNodeID = 0;
      String username = null;
      String password = null;

      CreateConnectionRequest request = new CreateConnectionRequest(version,
            remotingSessionID, clientVMID, failedNodeID, username, password);
      addVersion(request);
      AbstractPacketCodec<CreateConnectionRequest> codec = new ConnectionFactoryCreateConnectionRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateConnectionRequest);
      CreateConnectionRequest decodedRequest = (CreateConnectionRequest) decodedPacket;

      assertEquals(REQ_CREATECONNECTION, decodedPacket.getType());
      assertEquals(request.getVersion(), decodedRequest.getVersion());
      assertEquals(request.getRemotingSessionID(), decodedRequest
            .getRemotingSessionID());
      assertEquals(request.getClientVMID(), decodedRequest.getClientVMID());
      assertEquals(request.getFailedNodeID(), decodedRequest.getFailedNodeID());
      assertEquals(request.getUsername(), decodedRequest.getUsername());
      assertEquals(request.getPassword(), decodedRequest.getPassword());
   }

   public void testCreateConnectionResponse() throws Exception
   {
      CreateConnectionResponse response = new CreateConnectionResponse(
            randomString(), 1234);
      addVersion(response);
      AbstractPacketCodec<CreateConnectionResponse> codec = new ConnectionFactoryCreateConnectionResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateConnectionResponse);

      CreateConnectionResponse decodedResponse = (CreateConnectionResponse) decodedPacket;
      assertEquals(RESP_CREATECONNECTION, decodedResponse.getType());
      assertEquals(response.getConnectionID(), decodedResponse
            .getConnectionID());
      assertEquals(response.getServerID(), decodedResponse.getServerID());
   }

   public void testGetClientAOPStackRequest() throws Exception
   {
      GetClientAOPStackRequest request = new GetClientAOPStackRequest();
      addVersion(request);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            REQ_GETCLIENTAOPSTACK, GetClientAOPStackRequest.class);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      buffer.rewind();
      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof GetClientAOPStackRequest);
      assertEquals(REQ_GETCLIENTAOPSTACK, decodedPacket.getType());
   }

   public void testGetClientAOPStackResponse() throws Exception
   {
      byte[] stack = randomBytes();

      GetClientAOPStackResponse response = new GetClientAOPStackResponse(stack);
      addVersion(response);
      AbstractPacketCodec codec = new GetClientAOPStackResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof GetClientAOPStackResponse);
      GetClientAOPStackResponse decodedResponse = (GetClientAOPStackResponse) decodedPacket;
      assertEquals(RESP_GETCLIENTAOPSTACK, decodedResponse.getType());
      assertEqualsByteArrays(response.getStack(), decodedResponse.getStack());
   }

   public void testGetTopologyRequest() throws Exception
   {
      GetTopologyRequest request = new GetTopologyRequest();
      addVersion(request);

      AbstractPacketCodec codec = PacketCodecFactory.createCodecForEmptyPacket(
            REQ_GETTOPOLOGY, GetTopologyRequest.class);
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof GetTopologyRequest);
      assertEquals(REQ_GETTOPOLOGY, decodedPacket.getType());
   }

   @SuppressWarnings("unchecked")
   public void testGetTopologyResponse() throws Exception
   {
      // FIXME should use mock objects with a correct interface
      ClientConnectionFactoryDelegate[] delegates = new ClientConnectionFactoryDelegate[] {
            new ClientConnectionFactoryDelegate(randomString(), randomString(),
                  23, randomString(), randomInt(), Version.instance(), false, true),
            new ClientConnectionFactoryDelegate(randomString(), randomString(),
                  33, randomString(), randomInt(), Version.instance(), true, false) };
      TopologyResult topology = new TopologyResult(randomString(), delegates,
            new HashMap());
      GetTopologyResponse response = new GetTopologyResponse(topology);
      addVersion(response);
      AbstractPacketCodec codec = new GetTopologyResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof GetTopologyResponse);
      GetTopologyResponse decodedResponse = (GetTopologyResponse) decodedPacket;
      assertEquals(RESP_GETTOPOLOGY, decodedPacket.getType());
      assertSameTopology(response.getTopology(), decodedResponse.getTopology());
   }

   public void testUpdateCallbackMessage() throws Exception
   {
      UpdateCallbackMessage message = new UpdateCallbackMessage(randomString(),
            randomString(), true);
      addVersion(message);
      AbstractPacketCodec codec = new UpdateCallbackMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
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
            randomString(), 23, false);
      addVersion(response);
      AbstractPacketCodec codec = new CreateSessionResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CreateSessionResponse);

      CreateSessionResponse decodedResponse = (CreateSessionResponse) decodedPacket;
      assertEquals(RESP_CREATESESSION, decodedResponse.getType());
      assertEquals(response.getSessionID(), decodedResponse.getSessionID());
      assertEquals(response.getDupsOKBatchSize(), decodedResponse
            .getDupsOKBatchSize());
      assertEquals(response.isStrictTCK(), decodedResponse.isStrictTCK());
   }

   public void testIDBlockRequest() throws Exception
   {
      IDBlockRequest request = new IDBlockRequest(23);
      addVersion(request);
      AbstractPacketCodec codec = new IDBlockRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof IDBlockRequest);

      IDBlockRequest decodedRequest = (IDBlockRequest) decodedPacket;
      assertEquals(REQ_IDBLOCK, decodedRequest.getType());
      assertEquals(request.getSize(), decodedRequest.getSize());
   }

   public void testIDBlockResponse() throws Exception
   {
      IDBlockResponse response = new IDBlockResponse(randomLong(),
            randomLong() * 2);
      addVersion(response);
      AbstractPacketCodec codec = new IDBlockResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof IDBlockResponse);

      IDBlockResponse decodedResponse = (IDBlockResponse) decodedPacket;
      assertEquals(RESP_IDBLOCK, decodedResponse.getType());
      assertEquals(response.getLow(), decodedResponse.getLow());
      assertEquals(response.getHigh(), decodedResponse.getHigh());
   }

   public void testSendMessage() throws Exception
   {
      SendMessage packet = new SendMessage(new JBossMessage(System
            .currentTimeMillis()), true, randomLong());
      addVersion(packet);
      AbstractPacketCodec codec = new SendMessageCodec();
      SimpleRemotingBuffer buffer = encode(packet, codec);
      checkHeader(buffer, packet);
      buffer.rewind();

      AbstractPacket p = codec.decode(buffer);

      assertTrue(p instanceof SendMessage);

      SendMessage decodedPacket = (SendMessage) p;
      assertEquals(MSG_SENDMESSAGE, decodedPacket.getType());
      assertEquals(packet.checkForDuplicates(), decodedPacket
            .checkForDuplicates());
      assertEquals(packet.getSequence(), decodedPacket.getSequence());
      assertEquals(packet.getMessage().getMessageID(), decodedPacket
            .getMessage().getMessageID());
   }

   public void testCreateConsumerRequest() throws Exception
   {
      JBossDestination destination = new JBossQueue(
            "testCreateConsumerRequest", true);
      CreateConsumerRequest request = new CreateConsumerRequest(destination,
            "color = 'red'", false, "subscription", false, false);
      addVersion(request);
      AbstractPacketCodec codec = new CreateConsumerRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
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
      assertEquals(request.isAutoFlowControl(), decodedRequest
            .isAutoFlowControl());
   }

   public void testCreateDestinationRequest() throws Exception
   {
      CreateDestinationRequest request = new CreateDestinationRequest(
            "testCreateDestinationRequest", false);
      addVersion(request);
      AbstractPacketCodec codec = new CreateDestinationRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
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
      JBossDestination destination = new JBossQueue("testCreateQueueResponse",
            true);
      CreateDestinationResponse response = new CreateDestinationResponse(
            destination);
      addVersion(response);
      AbstractPacketCodec codec = new CreateDestinationResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
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
      JBossDestination destination = new JBossTopic(
            "testCreateDestinationResponseForTopic");
      CreateDestinationResponse response = new CreateDestinationResponse(
            destination);
      addVersion(response);
      AbstractPacketCodec codec = new CreateDestinationResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
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
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ChangeRateMessage);
      ChangeRateMessage decodedMessage = (ChangeRateMessage) decodedPacket;
      assertEquals(MSG_CHANGERATE, decodedMessage.getType());
      assertEquals(message.getRate(), decodedMessage.getRate());
   }

   public void testDeliverMessage() throws Exception
   {
      Message msg = new JBossMessage(randomLong());
      DeliverMessage message = new DeliverMessage(msg, randomString(),
            randomLong(), 23);
      addVersion(message);
      AbstractPacketCodec codec = new DeliverMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
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
      acks.add(new DefaultAck(randomLong()));
      acks.add(new DefaultAck(randomLong()));
      acks.add(new DefaultAck(randomLong()));

      AcknowledgeDeliveriesMessage request = new AcknowledgeDeliveriesMessage(
            acks);
      addVersion(request);
      AbstractPacketCodec codec = new AcknowledgeDeliveriesRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
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
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof ClosingRequest);
      ClosingRequest decodedRequest = (ClosingRequest) decodedPacket;
      assertEquals(REQ_CLOSING, decodedRequest.getType());
      assertEquals(request.getSequence(), decodedRequest.getSequence());
   }

   public void testClosingResponse() throws Exception
   {
      ClosingResponse response = new ClosingResponse(System.currentTimeMillis());
      addVersion(response);
      AbstractPacketCodec codec = new ClosingResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
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
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CloseMessage);
      CloseMessage decodedMessage = (CloseMessage) decodedPacket;
      assertEquals(MSG_CLOSE, decodedMessage.getType());
   }

   public void testSendTransactionMessage() throws Exception
   {
      ClientTransaction tx = new ClientTransaction();
      MessagingXid xid = new MessagingXid(randomBytes(), 23, randomBytes());
      TransactionRequest tr = new TransactionRequest(
            TransactionRequest.ONE_PHASE_COMMIT_REQUEST, xid, tx);

      SendTransactionMessage message = new SendTransactionMessage(tr, true);
      addVersion(message);
      AbstractPacketCodec codec = new SendTransactionMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SendTransactionMessage);
      SendTransactionMessage decodedMessage = (SendTransactionMessage) decodedPacket;
      assertEquals(MSG_SENDTRANSACTION, decodedMessage.getType());
      assertEquals(message.checkForDuplicates(), decodedMessage
            .checkForDuplicates());

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
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof SetClientIDMessage);
      SetClientIDMessage decodedMessage = (SetClientIDMessage) decodedPacket;
      assertEquals(MSG_SETCLIENTID, decodedMessage.getType());
      assertEquals(message.getClientID(), decodedMessage.getClientID());
   }

   public void testRecoverDeliveriesMessage() throws Exception
   {
      List<DeliveryRecovery> deliveries = new ArrayList<DeliveryRecovery>();
      deliveries.add(new DeliveryRecovery(randomLong(), randomLong(),
            randomString()));
      deliveries.add(new DeliveryRecovery(randomLong(), randomLong(),
            randomString()));
      deliveries.add(new DeliveryRecovery(randomLong(), randomLong(),
            randomString()));

      RecoverDeliveriesMessage message = new RecoverDeliveriesMessage(
            deliveries, randomString());
      addVersion(message);
      AbstractPacketCodec codec = new RecoverDeliveriesMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof RecoverDeliveriesMessage);
      RecoverDeliveriesMessage decodedMessage = (RecoverDeliveriesMessage) decodedPacket;
      assertEquals(MSG_RECOVERDELIVERIES, decodedMessage.getType());
      assertEqualsDeliveries(message.getDeliveries(), decodedMessage
            .getDeliveries());
      assertEquals(message.getSessionID(), decodedMessage.getSessionID());
   }

   public void testCancelDeliveryMessage() throws Exception
   {
      Cancel cancel = new DefaultCancel(randomLong(), 23, true, false);
      CancelDeliveryMessage message = new CancelDeliveryMessage(cancel);
      addVersion(message);
      AbstractPacketCodec codec = new CancelDeliveryMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
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
      cancels.add(new DefaultCancel(randomLong(), 23, true, false));
      cancels.add(new DefaultCancel(randomLong(), 33, false, true));

      CancelDeliveriesMessage message = new CancelDeliveriesMessage(cancels);
      addVersion(message);
      AbstractPacketCodec codec = new CancelDeliveriesMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof CancelDeliveriesMessage);
      CancelDeliveriesMessage decodedMessage = (CancelDeliveriesMessage) decodedPacket;
      assertEquals(MSG_CANCELDELIVERIES, decodedMessage.getType());
      assertEqualsCancels(message.getCancels(), decodedMessage.getCancels());
   }

   public void testCreateBrowserRequest() throws Exception
   {
      JBossDestination destination = new JBossQueue("testCreateBrowserRequest",
            true);
      CreateBrowserRequest request = new CreateBrowserRequest(destination,
            "color = 'red'");
      addVersion(request);
      AbstractPacketCodec codec = new CreateBrowserRequestCodec();
      SimpleRemotingBuffer buffer = encode(request, codec);
      checkHeader(buffer, request);
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
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BrowserNextMessageRequest);
      assertEquals(REQ_BROWSER_NEXTMESSAGE, decodedPacket.getType());
   }

   public void testBrowserNextMessageResponse() throws Exception
   {
      JBossMessage msg = new JBossMessage(randomLong());
      BrowserNextMessageResponse response = new BrowserNextMessageResponse(msg);
      addVersion(response);
      AbstractPacketCodec codec = new BrowserNextMessageResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
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
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof BrowserNextMessageBlockRequest);
      BrowserNextMessageBlockRequest decodedRequest = (BrowserNextMessageBlockRequest) decodedPacket;
      assertEquals(REQ_BROWSER_NEXTMESSAGEBLOCK, decodedPacket.getType());
      assertEquals(request.getMaxMessages(), decodedRequest.getMaxMessages());
   }

   public void testBrowserNextMessageBlockResponse() throws Exception
   {
      JBossMessage[] messages = new JBossMessage[] {
            new JBossMessage(randomLong()), new JBossMessage(randomLong()) };

      BrowserNextMessageBlockResponse response = new BrowserNextMessageBlockResponse(
            messages);
      addVersion(response);
      AbstractPacketCodec codec = new BrowserNextMessageBlockResponseCodec();
      SimpleRemotingBuffer buffer = encode(response, codec);
      checkHeader(buffer, response);
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
      JBossDestination destination = new JBossQueue(
            "testAddTemporaryDestinationMessage", true);
      AddTemporaryDestinationMessage message = new AddTemporaryDestinationMessage(
            destination);
      addVersion(message);
      AbstractPacketCodec codec = new AddTemporaryDestinationMessageCodec();
      SimpleRemotingBuffer buffer = encode(message, codec);
      checkHeader(buffer, message);
      buffer.rewind();

      AbstractPacket decodedPacket = codec.decode(buffer);

      assertTrue(decodedPacket instanceof AddTemporaryDestinationMessage);

      AddTemporaryDestinationMessage decodedMessage = (AddTemporaryDestinationMessage) decodedPacket;
      assertEquals(MSG_ADDTEMPORARYDESTINATION, decodedMessage.getType());
      assertEquals(message.getDestination(), decodedMessage.getDestination());
   }

   public void testDeleteTemporaryDestinationMessage() throws Exception
   {
      JBossDestination destination = new JBossQueue(
            "testDeleteTemporaryDestinationMessage", true);
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
      SimpleRemotingBuffer buf = new SimpleRemotingBuffer();

      codec.encode(packet, buf);
      buf.flip();

      return buf;
   }

   private final class SimpleRemotingBuffer implements RemotingBuffer
   {
      private static final byte NON_NULL_STRING = (byte) 0;
      private static final byte NULL_STRING = (byte) 1;

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);

      DataInputStream dais;

      /**
       * the buffer is can no longer be written but just read
       */
      public void flip()
      {
         dos = null;
         dais = new DataInputStream(
               new ByteArrayInputStream(baos.toByteArray()));
         dais.mark(1024);
      }

      public void rewind() throws IOException
      {
         dais.reset();
      }

      public byte get()
      {
         try
         {
            return dais.readByte();
         } catch (IOException e)
         {
            fail();
            return -1;
         }
      }

      public void get(byte[] b)
      {
         try
         {
            dais.readFully(b);
         } catch (IOException e)
         {
            fail();
         }
      }

      public boolean getBoolean()
      {
         try
         {
            return dais.readBoolean();
         } catch (IOException e)
         {
            fail();
            return false;
         }
      }

      public float getFloat()
      {
         try
         {
            return dais.readFloat();
         } catch (IOException e)
         {
            fail();
            return -1;
         }
      }

      public int getInt()
      {
         try
         {
            return dais.readInt();
         } catch (IOException e)
         {
            fail();
            return -1;
         }
      }

      public long getLong()
      {
         try
         {
            return dais.readLong();
         } catch (IOException e)
         {
            fail();
            return -1;
         }
      }

      public String getNullableString() throws CharacterCodingException
      {
         try
         {
            byte check = dais.readByte();
            if (check == NULL_STRING)
            {
               return null;
            } else
            {
               return dais.readUTF();
            }
         } catch (IOException e)
         {
            fail();
            return null;
         }
      }

      public void put(byte byteValue)
      {
         try
         {
            dos.writeByte(byteValue);
         } catch (IOException e)
         {
            fail();
         }
      }

      public void put(byte[] bytes)
      {
         try
         {
            dos.write(bytes);
         } catch (IOException e)
         {
            fail();
         }
      }

      public void putBoolean(boolean b)
      {
         try
         {
            dos.writeBoolean(b);
         } catch (IOException e)
         {
            fail();
         }
      }

      public void putFloat(float floatValue)
      {
         try
         {
            dos.writeFloat(floatValue);
         } catch (IOException e)
         {
            fail();
         }
      }

      public void putInt(int intValue)
      {
         try
         {
            dos.writeInt(intValue);
         } catch (IOException e)
         {
            fail();
         }
      }

      public void putLong(long longValue)
      {
         try
         {
            dos.writeLong(longValue);
         } catch (IOException e)
         {
            fail();
         }
      }

      public void putNullableString(String nullableString)
            throws CharacterCodingException
      {
         try
         {
            if (nullableString == null)
            {
               dos.writeByte(NULL_STRING);
            } else
            {
               dos.writeByte(NON_NULL_STRING);
               dos.writeUTF(nullableString);
            }
         } catch (IOException e)
         {
            fail();
         }
      }

      public int remaining()
      {
         try
         {
            return dais.available();
         } catch (IOException e)
         {
            fail();
            return -1;
         }
      }
   }

}
