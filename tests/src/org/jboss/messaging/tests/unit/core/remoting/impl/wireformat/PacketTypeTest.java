/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.BYTES;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CONN_CREATESESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CONN_CREATESESSION_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CONN_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CONN_STOP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CONS_FLOWTOKEN;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CREATECONNECTION;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.CREATECONNECTION_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.NULL;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.PING;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.PONG;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.PROD_RECEIVETOKENS;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_ACKNOWLEDGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_ADD_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_BINDINGQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_BINDINGQUERY_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_BROWSER_HASNEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_BROWSER_HASNEXTMESSAGE_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_BROWSER_NEXTMESSAGE;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_BROWSER_RESET;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_CANCEL;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_CREATEBROWSER_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_CREATECONSUMER_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_CREATEPRODUCER;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_CREATEPRODUCER_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_CREATEQUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_DELETE_QUEUE;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_QUEUEQUERY;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_QUEUEQUERY_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_RECOVER;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_REMOVE_DESTINATION;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_COMMIT;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_END;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_FORGET;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_GET_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_GET_TIMEOUT_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_INDOUBT_XIDS;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_INDOUBT_XIDS_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_JOIN;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_PREPARE;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_RESUME;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_ROLLBACK;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_SET_TIMEOUT;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_SET_TIMEOUT_RESP;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_START;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.SESS_XA_SUSPEND;
import static org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket.TEXT;
import static org.jboss.messaging.tests.unit.core.remoting.impl.wireformat.CodecAssert.assertEqualsByteArrays;
import static org.jboss.messaging.tests.unit.core.remoting.impl.wireformat.CodecAssert.assertSameXids;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import static org.jboss.messaging.tests.util.RandomUtil.randomXid;
import static org.jboss.messaging.util.DataConstants.SIZE_BYTE;
import static org.jboss.messaging.util.DataConstants.SIZE_LONG;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.mina.BufferWrapper;
import org.jboss.messaging.core.remoting.impl.mina.MessagingCodec;
import org.jboss.messaging.core.remoting.impl.wireformat.BytesPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerReceiveTokensMessage;
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
import org.jboss.messaging.core.remoting.impl.wireformat.XidCodecSupport;
import org.jboss.messaging.core.version.impl.VersionImpl;
import org.jboss.messaging.tests.util.RandomUtil;
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
      BufferWrapper buffer = new BufferWrapper(length);
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
            buffer.putSimpleString((SimpleString) arg);
         else if (arg instanceof NullableStringHolder)
            buffer.putNullableSimpleString(((NullableStringHolder) arg).str);
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
                  XidCodecSupport.encodeXid((Xid) argInList, buffer);
               else
                  fail("no encoding defined for " + arg + " in List");
            }
         } else if (arg instanceof Xid)
         {
            Xid xid = (Xid) arg;
            XidCodecSupport.encodeXid(xid, buffer);
         } else
         {
            fail("no encoding defined for " + arg);
         }
      }
      buffer.flip();
      return buffer;
   }

   private static void checkHeader(final MessagingBuffer buffer,
         final Packet packet) throws Exception
   {
      assertEquals(buffer.getByte(), packet.getType());

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
      assertEqualsByteArrays(bodyLength, expectedBody.array(), actualBody);
      // check the buffer has been wholly read
      assertEquals(0, buffer.remaining());
   }

   private static Packet encodeAndCheckBytesAndDecode(Packet packet,
         Object... bodyObjects) throws Exception
   {
      MessagingBuffer buffer = encode(packet);
      int packetLength = buffer.getInt();
      checkHeader(buffer, packet);
      int bodyLength = packetLength - (SIZE_BYTE + 3 * SIZE_LONG);
      checkBody(buffer, bodyLength, bodyObjects);
      buffer.rewind();

      SimpleProtocolDecoderOutput out = new SimpleProtocolDecoderOutput();
      MessagingCodec codec = new MessagingCodec();
      codec.doDecode(null, IoBuffer.wrap(buffer.array()), out);
      Object message = out.getMessage();
      assertTrue(message instanceof Packet);

      return (Packet) message;
   }

   private static MessagingBuffer encode(final Packet packet) throws Exception
   {
      MessagingBuffer buffer = new BufferWrapper(512);
      packet.encode(buffer);

      assertNotNull(buffer);

      return buffer;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testNullPacket() throws Exception
   {
      Packet packet = new EmptyPacket(NULL);
      packet.setResponseTargetID(randomLong());
      packet.setTargetID(randomLong());
      packet.setExecutorID(randomLong());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet);

      assertTrue(decodedPacket instanceof EmptyPacket);
      assertEquals(NULL, decodedPacket.getType());
      assertEquals(packet.getResponseTargetID(), decodedPacket
            .getResponseTargetID());
      assertEquals(packet.getTargetID(), decodedPacket.getTargetID());
      assertEquals(packet.getExecutorID(), decodedPacket.getExecutorID());
   }

   public void testPing() throws Exception
   {
      Ping ping = new Ping(randomLong());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(ping, ping
            .getSessionID());

      assertTrue(decodedPacket instanceof Ping);
      Ping decodedPing = (Ping) decodedPacket;
      assertEquals(PING, decodedPing.getType());
      assertEquals(ping.getResponseTargetID(), decodedPacket
            .getResponseTargetID());
      assertEquals(ping.getTargetID(), decodedPacket.getTargetID());
      assertEquals(ping.getExecutorID(), decodedPacket.getExecutorID());
   }

   public void testPong() throws Exception
   {
      Pong pong = new Pong(randomLong(), true);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(pong, pong
            .getSessionID(), pong.isSessionFailed());

      assertTrue(decodedPacket instanceof Pong);
      Pong decodedPong = (Pong) decodedPacket;
      assertEquals(PONG, decodedPong.getType());
      assertEquals(pong.getSessionID(), decodedPong.getSessionID());
      assertEquals(pong.isSessionFailed(), decodedPong.isSessionFailed());
   }

   public void testTextPacket() throws Exception
   {
      TextPacket packet = new TextPacket("testTextPacket");

      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet, packet
            .getText());

      assertTrue(decodedPacket instanceof TextPacket);
      TextPacket p = (TextPacket) decodedPacket;

      assertEquals(TEXT, p.getType());
      assertEquals(packet.getText(), p.getText());
   }

   public void testBytesPacket() throws Exception
   {
      BytesPacket packet = new BytesPacket(RandomUtil.randomBytes());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet, packet
            .getBytes());

      assertTrue(decodedPacket instanceof BytesPacket);
      BytesPacket p = (BytesPacket) decodedPacket;

      assertEquals(BYTES, p.getType());
      assertEqualsByteArrays(packet.getBytes(), p.getBytes());
   }

   public void testCreateConnectionRequest() throws Exception
   {
      int version = randomInt();
      long remotingSessionID = randomLong();
      String username = null;
      String password = null;
      CreateConnectionRequest request = new CreateConnectionRequest(version,
            remotingSessionID, username, password);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, version,
            remotingSessionID, username, password);

      assertTrue(decodedPacket instanceof CreateConnectionRequest);
      CreateConnectionRequest decodedRequest = (CreateConnectionRequest) decodedPacket;

      assertEquals(CREATECONNECTION, decodedPacket.getType());
      assertEquals(request.getVersion(), decodedRequest.getVersion());
      assertEquals(request.getRemotingSessionID(), decodedRequest
            .getRemotingSessionID());
      assertEquals(request.getUsername(), decodedRequest.getUsername());
      assertEquals(request.getPassword(), decodedRequest.getPassword());
   }

   public void testCreateConnectionResponse() throws Exception
   {
      CreateConnectionResponse response = new CreateConnectionResponse(
            randomLong(), new VersionImpl("test", 1, 2, 3, 4, "xxx"));

      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, response
            .getConnectionTargetID(), response.getServerVersion()
            .getVersionName(), response.getServerVersion().getMajorVersion(),
            response.getServerVersion().getMinorVersion(), response
                  .getServerVersion().getMicroVersion(), response
                  .getServerVersion().getIncrementingVersion(), response
                  .getServerVersion().getVersionSuffix());

      assertTrue(decodedPacket instanceof CreateConnectionResponse);
      CreateConnectionResponse decodedResponse = (CreateConnectionResponse) decodedPacket;
      assertEquals(CREATECONNECTION_RESP, decodedResponse.getType());
      assertEquals(response.getConnectionTargetID(), decodedResponse
            .getConnectionTargetID());
      assertEquals(response.getServerVersion().getFullVersion(),
            decodedResponse.getServerVersion().getFullVersion());
   }

   public void testConnectionCreateSessionMessage() throws Exception
   {
      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(
            randomBoolean(), randomBoolean(), randomBoolean());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, request
            .isXA(), request.isAutoCommitSends(), request.isAutoCommitAcks());

      assertTrue(decodedPacket instanceof ConnectionCreateSessionMessage);
      ConnectionCreateSessionMessage decodedRequest = (ConnectionCreateSessionMessage) decodedPacket;
      assertEquals(CONN_CREATESESSION, decodedRequest.getType());
      assertEquals(request.isXA(), decodedRequest.isXA());
      assertEquals(request.isAutoCommitSends(), decodedRequest
            .isAutoCommitSends());
      assertEquals(request.isAutoCommitAcks(), decodedRequest
            .isAutoCommitAcks());
   }

   public void testConnectionCreateSessionResponseMessage() throws Exception
   {
      ConnectionCreateSessionResponseMessage response = new ConnectionCreateSessionResponseMessage(
            randomLong());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, response
            .getSessionID());

      assertTrue(decodedPacket instanceof ConnectionCreateSessionResponseMessage);
      ConnectionCreateSessionResponseMessage decodedResponse = (ConnectionCreateSessionResponseMessage) decodedPacket;
      assertEquals(CONN_CREATESESSION_RESP, decodedResponse.getType());
      assertEquals(response.getSessionID(), decodedResponse.getSessionID());
   }

   /*
    * public void testProducerSendMessage() throws Exception { Message msg = new
    * MessageImpl((byte)1, false, 1212212L, 761276712L, (byte)1);
    * msg.setDestination(new SimpleString("blah")); ProducerSendMessage packet =
    * new ProducerSendMessage(msg); MessagingBuffer buff =
    * packet.getMessage().encode();
    * 
    * Message msg2 = new MessageImpl(); msg2.decode(buff);
    * 
    * 
    * byte[] messageBytes = buff.array(); byte[] data = new byte[buff.limit()];
    * System.arraycopy(messageBytes, 0, data, 0, buff.limit());
    * AbstractPacketCodec codec = new ProducerSendMessageCodec();
    * 
    * Packet decodedPacket = encodeAndCheckBytesAndDecode(packet, data);
    * 
    * assertTrue(decodedPacket instanceof ProducerSendMessage);
    * ProducerSendMessage decodedMessage = (ProducerSendMessage) decodedPacket;
    * assertEquals(PacketType.PROD_SEND, decodedPacket.getType());
    * assertEquals(packet.getMessage().getMessageID(), decodedMessage
    * .getMessage().getMessageID()); }
    */

   public void testSessionCreateConsumerMessage() throws Exception
   {
      SimpleString destination = new SimpleString(
            "queue.SessionCreateConsumerMessage");
      SessionCreateConsumerMessage request = new SessionCreateConsumerMessage(
            randomLong(), destination, new SimpleString("color = 'red'"),
            false, false, randomInt(), randomInt());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, request
            .getClientTargetID(), request.getQueueName(),
            new NullableStringHolder(request.getFilterString()), request
                  .isNoLocal(), request.isAutoDeleteQueue(), request
                  .getWindowSize(), request.getMaxRate());

      assertTrue(decodedPacket instanceof SessionCreateConsumerMessage);
      SessionCreateConsumerMessage decodedRequest = (SessionCreateConsumerMessage) decodedPacket;
      assertEquals(SESS_CREATECONSUMER, decodedRequest.getType());
      assertEquals(request.getClientTargetID(), decodedRequest
            .getClientTargetID());
      assertEquals(request.getQueueName(), decodedRequest.getQueueName());
      assertEquals(request.getFilterString(), decodedRequest.getFilterString());
      assertEquals(request.isNoLocal(), decodedRequest.isNoLocal());
      assertEquals(request.isAutoDeleteQueue(), decodedRequest
            .isAutoDeleteQueue());
      assertEquals(request.getWindowSize(), decodedRequest.getWindowSize());
      assertEquals(request.getMaxRate(), decodedRequest.getMaxRate());
   }

   public void testSessionCreateConsumerResponseMessage() throws Exception
   {
      SessionCreateConsumerResponseMessage response = new SessionCreateConsumerResponseMessage(
            randomLong(), randomInt());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, response
            .getConsumerTargetID(), response.getWindowSize());

      assertTrue(decodedPacket instanceof SessionCreateConsumerResponseMessage);
      SessionCreateConsumerResponseMessage decodedResponse = (SessionCreateConsumerResponseMessage) decodedPacket;
      assertEquals(SESS_CREATECONSUMER_RESP, decodedResponse.getType());

      assertEquals(response.getConsumerTargetID(), decodedResponse
            .getConsumerTargetID());
      assertEquals(response.getWindowSize(), decodedResponse.getWindowSize());
   }

   public void testSessionCreateProducerMessage() throws Exception
   {
      SimpleString destination = new SimpleString(
            "queue.testSessionCreateProducerMessage");
      int windowSize = randomInt();
      int maxRate = randomInt();
      SessionCreateProducerMessage request = new SessionCreateProducerMessage(
            randomLong(), destination, windowSize, maxRate);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, request
            .getClientTargetID(),
            new NullableStringHolder(request.getAddress()), request
                  .getWindowSize(), request.getMaxRate());

      assertTrue(decodedPacket instanceof SessionCreateProducerMessage);
      SessionCreateProducerMessage decodedRequest = (SessionCreateProducerMessage) decodedPacket;
      assertEquals(SESS_CREATEPRODUCER, decodedRequest.getType());
      assertEquals(request.getClientTargetID(), decodedRequest
            .getClientTargetID());
      assertEquals(request.getAddress(), decodedRequest.getAddress());
      assertEquals(request.getWindowSize(), decodedRequest.getWindowSize());
      assertEquals(request.getMaxRate(), decodedRequest.getMaxRate());
   }

   public void testSessionCreateProducerResponseMessage() throws Exception
   {
      SessionCreateProducerResponseMessage response = new SessionCreateProducerResponseMessage(
            randomLong(), randomInt(), randomInt());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, response
            .getProducerTargetID(), response.getWindowSize(), response
            .getMaxRate());

      assertTrue(decodedPacket instanceof SessionCreateProducerResponseMessage);
      SessionCreateProducerResponseMessage decodedResponse = (SessionCreateProducerResponseMessage) decodedPacket;
      assertEquals(SESS_CREATEPRODUCER_RESP, decodedResponse.getType());
      assertEquals(response.getProducerTargetID(), decodedResponse
            .getProducerTargetID());
      assertEquals(response.getWindowSize(), decodedResponse.getWindowSize());
      assertEquals(response.getMaxRate(), decodedResponse.getMaxRate());
   }

   public void testStartConnectionMessage() throws Exception
   {
      Packet packet = new EmptyPacket(CONN_START);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet);

      assertEquals(CONN_START, decodedPacket.getType());
   }

   public void testStopConnectionMessage() throws Exception
   {
      Packet packet = new EmptyPacket(CONN_STOP);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(packet);

      assertEquals(CONN_STOP, decodedPacket.getType());
   }

   public void testConsumerFlowTokenMessage() throws Exception
   {
      ConsumerFlowTokenMessage message = new ConsumerFlowTokenMessage(
            randomInt());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getTokens());

      assertTrue(decodedPacket instanceof ConsumerFlowTokenMessage);
      ConsumerFlowTokenMessage decodedMessage = (ConsumerFlowTokenMessage) decodedPacket;
      assertEquals(CONS_FLOWTOKEN, decodedMessage.getType());
      assertEquals(message.getTokens(), decodedMessage.getTokens());
   }

   public void testProducerReceiveTokensMessage() throws Exception
   {
      ProducerReceiveTokensMessage message = new ProducerReceiveTokensMessage(
            randomInt());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getTokens());

      assertTrue(decodedPacket instanceof ProducerReceiveTokensMessage);
      ProducerReceiveTokensMessage decodedMessage = (ProducerReceiveTokensMessage) decodedPacket;
      assertEquals(PROD_RECEIVETOKENS, decodedMessage.getType());
      assertEquals(message.getTokens(), decodedMessage.getTokens());
   }

   /*
    * public void testReceiveMessage() throws Exception { Message msg = new
    * MessageImpl(); ReceiveMessage message = new ReceiveMessage(msg);
    * AbstractPacketCodec codec = new ReceiveMessageCodec();
    * 
    * byte[] messageBytes = message.getMessage().encode().array();
    * 
    * Packet decodedPacket = encodeAndCheckBytesAndDecode(message,
    * messageBytes);
    * 
    * assertTrue(decodedPacket instanceof ReceiveMessage); ReceiveMessage
    * decodedMessage = (ReceiveMessage) decodedPacket; assertEquals(RECEIVE_MSG,
    * decodedMessage.getType());
    * assertEquals(message.getMessage().getMessageID(), decodedMessage
    * .getMessage().getMessageID()); }
    */

   public void testSessionAcknowledgeMessage() throws Exception
   {
      SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(
            randomLong(), randomBoolean());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getDeliveryID(), message.isAllUpTo());

      assertTrue(decodedPacket instanceof SessionAcknowledgeMessage);
      SessionAcknowledgeMessage decodedMessage = (SessionAcknowledgeMessage) decodedPacket;
      assertEquals(SESS_ACKNOWLEDGE, decodedMessage.getType());
      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
      assertEquals(message.isAllUpTo(), decodedMessage.isAllUpTo());
   }

   public void testSessionCancelMessage() throws Exception
   {
      SessionCancelMessage message = new SessionCancelMessage(randomLong(),
            randomBoolean());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getDeliveryID(), message.isExpired());

      assertTrue(decodedPacket instanceof SessionCancelMessage);
      SessionCancelMessage decodedMessage = (SessionCancelMessage) decodedPacket;
      assertEquals(SESS_CANCEL, decodedMessage.getType());
      assertEquals(message.getDeliveryID(), decodedMessage.getDeliveryID());
      assertEquals(message.isExpired(), decodedMessage.isExpired());
   }

   public void testSessionCommitMessage() throws Exception
   {
      Packet message = new EmptyPacket(SESS_COMMIT);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message);

      assertEquals(SESS_COMMIT, decodedPacket.getType());
   }

   public void testSessionRollbackMessage() throws Exception
   {
      Packet message = new EmptyPacket(SESS_ROLLBACK);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message);

      assertEquals(SESS_ROLLBACK, decodedPacket.getType());
   }

   public void testSessionRecoverMessage() throws Exception
   {
      Packet message = new EmptyPacket(SESS_RECOVER);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message);

      assertEquals(SESS_RECOVER, decodedPacket.getType());
   }

   public void testCloseMessage() throws Exception
   {
      Packet message = new EmptyPacket(CLOSE);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message);

      assertEquals(CLOSE, decodedPacket.getType());
   }

   public void testSessionCreateBrowserMessage() throws Exception
   {
      SimpleString destination = new SimpleString(
            "queue.testCreateBrowserRequest");
      SessionCreateBrowserMessage request = new SessionCreateBrowserMessage(
            destination, new SimpleString("color = 'red'"));

      Packet decodedPacket = encodeAndCheckBytesAndDecode(request, request
            .getQueueName(),
            new NullableStringHolder(request.getFilterString()));

      assertTrue(decodedPacket instanceof SessionCreateBrowserMessage);
      SessionCreateBrowserMessage decodedRequest = (SessionCreateBrowserMessage) decodedPacket;
      assertEquals(SESS_CREATEBROWSER, decodedRequest.getType());
      assertEquals(request.getQueueName(), decodedRequest.getQueueName());
      assertEquals(request.getFilterString(), decodedRequest.getFilterString());
   }

   public void testSessionCreateBrowserResponseMessage() throws Exception
   {
      SessionCreateBrowserResponseMessage response = new SessionCreateBrowserResponseMessage(
            randomLong());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, response
            .getBrowserTargetID());

      assertTrue(decodedPacket instanceof SessionCreateBrowserResponseMessage);
      SessionCreateBrowserResponseMessage decodedResponse = (SessionCreateBrowserResponseMessage) decodedPacket;
      assertEquals(SESS_CREATEBROWSER_RESP, decodedResponse.getType());
      assertEquals(response.getBrowserTargetID(), decodedResponse
            .getBrowserTargetID());
   }

   public void testBrowserResetMessage() throws Exception
   {
      Packet message = new EmptyPacket(SESS_BROWSER_RESET);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message);

      assertEquals(SESS_BROWSER_RESET, decodedPacket.getType());
   }

   public void testBrowserHasNextMessageRequest() throws Exception
   {
      Packet request = new EmptyPacket(SESS_BROWSER_HASNEXTMESSAGE);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(request);

      assertEquals(SESS_BROWSER_HASNEXTMESSAGE, decodedPacket.getType());
   }

   public void testSessionBrowserHasNextMessageResponseMessage()
         throws Exception
   {
      SessionBrowserHasNextMessageResponseMessage response = new SessionBrowserHasNextMessageResponseMessage(
            randomBoolean());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(response, response
            .hasNext());

      assertTrue(decodedPacket instanceof SessionBrowserHasNextMessageResponseMessage);
      SessionBrowserHasNextMessageResponseMessage decodedResponse = (SessionBrowserHasNextMessageResponseMessage) decodedPacket;
      assertEquals(SESS_BROWSER_HASNEXTMESSAGE_RESP, decodedResponse.getType());
      assertEquals(response.hasNext(), decodedResponse.hasNext());
   }

   public void testBrowserNextMessageRequest() throws Exception
   {
      Packet request = new EmptyPacket(SESS_BROWSER_NEXTMESSAGE);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(request);

      assertEquals(SESS_BROWSER_NEXTMESSAGE, decodedPacket.getType());
   }

   public void testSessionXACommitMessage() throws Exception
   {
      SessionXACommitMessage message = new SessionXACommitMessage(randomXid(),
            randomBoolean());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getXid(), message.isOnePhase());

      assertTrue(decodedPacket instanceof SessionXACommitMessage);
      SessionXACommitMessage decodedMessage = (SessionXACommitMessage) decodedPacket;
      assertEquals(SESS_XA_COMMIT, decodedMessage.getType());
      assertEquals(message.getXid(), decodedMessage.getXid());
      assertEquals(message.isOnePhase(), decodedMessage.isOnePhase());
   }

   public void testSessionXAEndMessage() throws Exception
   {
      SessionXAEndMessage message = new SessionXAEndMessage(randomXid(),
            randomBoolean());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getXid(), message.isFailed());

      assertTrue(decodedPacket instanceof SessionXAEndMessage);
      SessionXAEndMessage decodedMessage = (SessionXAEndMessage) decodedPacket;
      assertEquals(SESS_XA_END, decodedMessage.getType());
      assertEquals(message.getXid(), decodedMessage.getXid());
      assertEquals(message.isFailed(), decodedMessage.isFailed());
   }

   public void testSessionXAForgetMessage() throws Exception
   {
      SessionXAForgetMessage message = new SessionXAForgetMessage(randomXid());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getXid());

      assertTrue(decodedPacket instanceof SessionXAForgetMessage);
      SessionXAForgetMessage decodedMessage = (SessionXAForgetMessage) decodedPacket;
      assertEquals(SESS_XA_FORGET, decodedMessage.getType());
      assertEquals(message.getXid(), decodedMessage.getXid());
   }

   public void testSessionXAGetInDoubtXidsMessage() throws Exception
   {
      Packet request = new EmptyPacket(SESS_XA_INDOUBT_XIDS);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(request);

      assertEquals(SESS_XA_INDOUBT_XIDS, decodedPacket.getType());
   }

   public void testSessionXAGetInDoubtXidsResponseMessage() throws Exception
   {
      final int numXids = 10;
      List<Xid> xids = new ArrayList<Xid>();
      for (int i = 0; i < numXids; i++)
      {
         xids.add(randomXid());
      }
      SessionXAGetInDoubtXidsResponseMessage message = new SessionXAGetInDoubtXidsResponseMessage(
            xids);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, xids);

      assertTrue(decodedPacket instanceof SessionXAGetInDoubtXidsResponseMessage);
      SessionXAGetInDoubtXidsResponseMessage decodedMessage = (SessionXAGetInDoubtXidsResponseMessage) decodedPacket;
      assertEquals(SESS_XA_INDOUBT_XIDS_RESP, decodedMessage.getType());

      assertSameXids(message.getXids(), decodedMessage.getXids());
   }

   public void testSessionXAGetTimeoutMessage() throws Exception
   {
      Packet message = new EmptyPacket(SESS_XA_GET_TIMEOUT);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message);

      assertEquals(SESS_XA_GET_TIMEOUT, decodedPacket.getType());
   }

   public void testSessionXAGetTimeoutResponseMessage() throws Exception
   {
      SessionXAGetTimeoutResponseMessage message = new SessionXAGetTimeoutResponseMessage(
            randomInt());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getTimeoutSeconds());

      assertTrue(decodedPacket instanceof SessionXAGetTimeoutResponseMessage);
      SessionXAGetTimeoutResponseMessage decodedMessage = (SessionXAGetTimeoutResponseMessage) decodedPacket;
      assertEquals(SESS_XA_GET_TIMEOUT_RESP, decodedMessage.getType());
      assertEquals(message.getTimeoutSeconds(), decodedMessage
            .getTimeoutSeconds());
   }

   public void testSessionXAJoinMessage() throws Exception
   {
      SessionXAJoinMessage message = new SessionXAJoinMessage(randomXid());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getXid());

      assertTrue(decodedPacket instanceof SessionXAJoinMessage);
      SessionXAJoinMessage decodedMessage = (SessionXAJoinMessage) decodedPacket;
      assertEquals(SESS_XA_JOIN, decodedMessage.getType());
      assertEquals(message.getXid(), decodedMessage.getXid());
   }

   public void testSessionXAPrepareMessage() throws Exception
   {
      SessionXAPrepareMessage message = new SessionXAPrepareMessage(randomXid());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getXid());

      assertTrue(decodedPacket instanceof SessionXAPrepareMessage);
      SessionXAPrepareMessage decodedMessage = (SessionXAPrepareMessage) decodedPacket;
      assertEquals(SESS_XA_PREPARE, decodedMessage.getType());
      assertEquals(message.getXid(), decodedMessage.getXid());
   }

   public void testSessionXAResponseMessage() throws Exception
   {
      SessionXAResponseMessage message = new SessionXAResponseMessage(
            randomBoolean(), randomInt(), randomString());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .isError(), message.getResponseCode(), message.getMessage());

      assertTrue(decodedPacket instanceof SessionXAResponseMessage);
      SessionXAResponseMessage decodedMessage = (SessionXAResponseMessage) decodedPacket;
      assertEquals(SESS_XA_RESP, decodedMessage.getType());
      assertEquals(message.isError(), decodedMessage.isError());
      assertEquals(message.getResponseCode(), decodedMessage.getResponseCode());
      assertEquals(message.getMessage(), decodedMessage.getMessage());
   }

   public void testSessionXAResumeMessage() throws Exception
   {
      SessionXAResumeMessage message = new SessionXAResumeMessage(randomXid());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getXid());

      assertTrue(decodedPacket instanceof SessionXAResumeMessage);
      SessionXAResumeMessage decodedMessage = (SessionXAResumeMessage) decodedPacket;
      assertEquals(SESS_XA_RESUME, decodedMessage.getType());
      assertEquals(message.getXid(), decodedMessage.getXid());
   }

   public void testSessionXARollbackMessage() throws Exception
   {
      SessionXARollbackMessage message = new SessionXARollbackMessage(
            randomXid());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getXid());

      assertTrue(decodedPacket instanceof SessionXARollbackMessage);
      SessionXARollbackMessage decodedMessage = (SessionXARollbackMessage) decodedPacket;
      assertEquals(SESS_XA_ROLLBACK, decodedMessage.getType());
      assertEquals(message.getXid(), decodedMessage.getXid());
   }

   public void testSessionXASetTimeoutMessage() throws Exception
   {
      SessionXASetTimeoutMessage message = new SessionXASetTimeoutMessage(
            randomInt());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getTimeoutSeconds());

      assertTrue(decodedPacket instanceof SessionXASetTimeoutMessage);
      SessionXASetTimeoutMessage decodedMessage = (SessionXASetTimeoutMessage) decodedPacket;
      assertEquals(SESS_XA_SET_TIMEOUT, decodedMessage.getType());
      assertEquals(message.getTimeoutSeconds(), decodedMessage
            .getTimeoutSeconds());
   }

   public void testSessionXASetTimeoutResponseMessage() throws Exception
   {
      SessionXASetTimeoutResponseMessage message = new SessionXASetTimeoutResponseMessage(
            randomBoolean());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .isOK());

      assertTrue(decodedPacket instanceof SessionXASetTimeoutResponseMessage);
      SessionXASetTimeoutResponseMessage decodedMessage = (SessionXASetTimeoutResponseMessage) decodedPacket;
      assertEquals(SESS_XA_SET_TIMEOUT_RESP, decodedMessage.getType());
      assertEquals(message.isOK(), decodedMessage.isOK());
   }

   public void testSessionXAStartMessage() throws Exception
   {
      SessionXAStartMessage message = new SessionXAStartMessage(randomXid());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getXid());

      assertTrue(decodedPacket instanceof SessionXAStartMessage);
      SessionXAStartMessage decodedMessage = (SessionXAStartMessage) decodedPacket;
      assertEquals(SESS_XA_START, decodedMessage.getType());
      assertEquals(message.getXid(), decodedMessage.getXid());
   }

   public void testSessionXASuspendMessage() throws Exception
   {
      Packet message = new EmptyPacket(SESS_XA_SUSPEND);

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message);

      assertEquals(SESS_XA_SUSPEND, decodedPacket.getType());
   }

   public void testSessionRemoveDestinationMessage() throws Exception
   {
      SessionRemoveDestinationMessage message = new SessionRemoveDestinationMessage(
            new SimpleString(randomString()), randomBoolean());

      Packet decodedPacket = encodeAndCheckBytesAndDecode(message, message
            .getAddress(), message.isTemporary());

      assertTrue(decodedPacket instanceof SessionRemoveDestinationMessage);
      SessionRemoveDestinationMessage decodedMessage = (SessionRemoveDestinationMessage) decodedPacket;
      assertEquals(SESS_REMOVE_DESTINATION, decodedMessage.getType());
      assertEquals(message.getAddress(), decodedMessage.getAddress());
      assertEquals(message.isTemporary(), decodedMessage.isTemporary());
   }

    public void testSessionCreateQueueMessage() throws Exception
    {
    SessionCreateQueueMessage message = new SessionCreateQueueMessage(
    new SimpleString(randomString()), new SimpleString(randomString()),
    new SimpleString(randomString()), randomBoolean(),
    randomBoolean());
   
    Packet decodedPacket = encodeAndCheckBytesAndDecode(message,
    message.getAddress(), message.getQueueName(), new
    NullableStringHolder(message.getFilterString()), message.isDurable(),
    message
    .isTemporary());
   
    assertTrue(decodedPacket instanceof SessionCreateQueueMessage);
    SessionCreateQueueMessage decodedMessage = (SessionCreateQueueMessage)
    decodedPacket;
    assertEquals(SESS_CREATEQUEUE, decodedMessage.getType());
   
    assertEquals(message.getAddress(), decodedMessage.getAddress());
    assertEquals(message.getQueueName(), decodedMessage.getQueueName());
    assertEquals(message.getFilterString(), decodedMessage.getFilterString());
    assertEquals(message.isDurable(), decodedMessage.isDurable());
    assertEquals(message.isTemporary(), decodedMessage.isTemporary());
   
    }
   
    public void testSessionQueueQueryMessage() throws Exception
    {
    SessionQueueQueryMessage message = new SessionQueueQueryMessage(
    new SimpleString(randomString()));
   
    Packet decodedPacket = encodeAndCheckBytesAndDecode(message,
    message.getQueueName());
   
    assertTrue(decodedPacket instanceof SessionQueueQueryMessage);
    SessionQueueQueryMessage decodedMessage = (SessionQueueQueryMessage)
    decodedPacket;
    assertEquals(SESS_QUEUEQUERY, decodedMessage.getType());
    assertEquals(message.getQueueName(), decodedMessage.getQueueName());
    }
   
    public void testSessionQueueQueryResponseMessage() throws Exception
    {
    SessionQueueQueryResponseMessage message = new
    SessionQueueQueryResponseMessage(
    randomBoolean(), randomBoolean(), randomInt(), randomInt(),
    randomInt(), new SimpleString(randomString()), new
    SimpleString(randomString()));
   
    Packet decodedPacket = encodeAndCheckBytesAndDecode(message,
    message.isExists(), message.isDurable(), message.isTemporary(),
    message.getMaxSize(), message.getConsumerCount(), message
    .getMessageCount(), new NullableStringHolder(message.getFilterString()),
    new NullableStringHolder(message.getAddress()));
   
    assertTrue(decodedPacket instanceof SessionQueueQueryResponseMessage);
    SessionQueueQueryResponseMessage decodedMessage =
    (SessionQueueQueryResponseMessage) decodedPacket;
    assertEquals(SESS_QUEUEQUERY_RESP, decodedMessage.getType());
   
    assertEquals(message.isExists(), decodedMessage.isExists());
    assertEquals(message.isDurable(), decodedMessage.isDurable());
    assertEquals(message.isTemporary(), decodedMessage.isTemporary());
    assertEquals(message.getConsumerCount(), decodedMessage
    .getConsumerCount());
    assertEquals(message.getMessageCount(), decodedMessage.getMessageCount());
    assertEquals(message.getFilterString(), decodedMessage.getFilterString());
    assertEquals(message.getAddress(), decodedMessage.getAddress());
    }
   
    public void testSessionAddAddressMessage() throws Exception
    {
    SessionAddDestinationMessage message = new SessionAddDestinationMessage(
    new SimpleString(randomString()), randomBoolean());
   
    Packet decodedPacket = encodeAndCheckBytesAndDecode(message,
    message.getAddress(), message.isTemporary());
   
    assertTrue(decodedPacket instanceof SessionAddDestinationMessage);
    SessionAddDestinationMessage decodedMessage =
    (SessionAddDestinationMessage) decodedPacket;
    assertEquals(SESS_ADD_DESTINATION, decodedMessage.getType());
    assertEquals(message.getAddress(), decodedMessage.getAddress());
    assertEquals(message.isTemporary(), decodedMessage.isTemporary());
    }
   
    public void testSessionBindingQueryMessage() throws Exception
    {
    SessionBindingQueryMessage message = new SessionBindingQueryMessage(
    new SimpleString(randomString()));
   
    Packet decodedPacket = encodeAndCheckBytesAndDecode(message,
    message.getAddress());
   
    assertTrue(decodedPacket instanceof SessionBindingQueryMessage);
    SessionBindingQueryMessage decodedMessage = (SessionBindingQueryMessage)
    decodedPacket;
    assertEquals(SESS_BINDINGQUERY, decodedMessage.getType());
   
    assertEquals(message.getAddress(), decodedMessage.getAddress());
    }
   
    public void testSessionBindingQueryResponseMessage() throws Exception
    {
    boolean exists = true;
    List<SimpleString> queueNames = new ArrayList<SimpleString>();
    queueNames.add(new SimpleString(randomString()));
    queueNames.add(new SimpleString(randomString()));
    queueNames.add(new SimpleString(randomString()));
    SessionBindingQueryResponseMessage message = new
    SessionBindingQueryResponseMessage(
    exists, queueNames);
   
    Packet decodedPacket = encodeAndCheckBytesAndDecode(message,
    message.isExists(), message.getQueueNames());
   
    assertTrue(decodedPacket instanceof SessionBindingQueryResponseMessage);
    SessionBindingQueryResponseMessage decodedMessage =
    (SessionBindingQueryResponseMessage) decodedPacket;
    assertEquals(SESS_BINDINGQUERY_RESP, decodedMessage.getType());
    assertEquals(message.isExists(), decodedMessage.isExists());
   
    List<SimpleString> decodedNames = decodedMessage.getQueueNames();
    assertEquals(queueNames.size(), decodedNames.size());
    for (int i = 0; i < queueNames.size(); i++)
    {
    assertEquals(queueNames.get(i), decodedNames.get(i));
    }
    }
   
    public void testDeleteQueueRequest() throws Exception
    {
    SessionDeleteQueueMessage message = new SessionDeleteQueueMessage(
    new SimpleString(randomString()));
   
    Packet decodedPacket = encodeAndCheckBytesAndDecode(message,
    message.getQueueName());
   
    assertTrue(decodedPacket instanceof SessionDeleteQueueMessage);
    SessionDeleteQueueMessage decodedMessage = (SessionDeleteQueueMessage)
    decodedPacket;
    assertEquals(SESS_DELETE_QUEUE, decodedMessage.getType());
    assertEquals(message.getQueueName(), decodedMessage.getQueueName());
    }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static class SimpleProtocolDecoderOutput implements
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
