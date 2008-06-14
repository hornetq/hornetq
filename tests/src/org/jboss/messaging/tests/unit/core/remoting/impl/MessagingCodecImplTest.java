/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
   * by the @authors tag. See the copyright.txt in the distribution for a
   * full listing of individual contributors.
   *
   * This is free software; you can redistribute it and/or modify it
   * under the terms of the GNU Lesser General Public License as
   * published by the Free Software Foundation; either version 2.1 of
   * the License, or (at your option) any later version.
   *
   * This software is distributed in the hope that it will be useful,
   * but WITHOUT ANY WARRANTY; without even the implied warranty of
   * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   * Lesser General Public License for more details.
   *
   * You should have received a copy of the GNU Lesser General Public
   * License along with this software; if not, write to the Free
   * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
   * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
   */
package org.jboss.messaging.tests.unit.core.remoting.impl;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.MessagingCodec;
import org.jboss.messaging.core.remoting.impl.MessagingCodecImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.*;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.core.version.impl.VersionImpl;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.ByteBufferWrapper;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MessagingCodecImplTest extends UnitTestCase
{
   MessagingCodec codec = null;
   private MessagingBuffer buff;

   protected void setUp() throws Exception
   {
      super.setUp();
      codec = new MessagingCodecImpl();
      buff = new ByteBufferWrapper(ByteBuffer.allocateDirect(1024));
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      codec = null;
      buff = null;
   }
   public void testEmptyPacket() throws Exception
   {
      EmptyPacket message = new EmptyPacket(EmptyPacket.NULL);
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      EmptyPacket copy = (EmptyPacket) codec.decode(buff);
      checkHeaders(message, copy);
   }

   public void testConnectionCreateSessionPacket() throws Exception
   {
      ConnectionCreateSessionMessage message = new ConnectionCreateSessionMessage(true, true, true);
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      ConnectionCreateSessionMessage copy = (ConnectionCreateSessionMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.isAutoCommitAcks(), copy.isAutoCommitAcks());
      assertEquals(message.isAutoCommitSends(), copy.isAutoCommitSends());
      assertEquals(message.isXA(), copy.isXA());
   }

   public void testCreateConnectionRequestPacket() throws Exception
   {
      CreateConnectionRequest message = new CreateConnectionRequest(RandomUtil.randomInt(), RandomUtil.randomLong(), RandomUtil.randomString(), RandomUtil.randomString());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      CreateConnectionRequest copy = (CreateConnectionRequest) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getUsername(), copy.getUsername());
      assertEquals(message.getVersion(), copy.getVersion());
      assertEquals(message.getPassword(), copy.getPassword());
      assertEquals(message.getRemotingSessionID(), copy.getRemotingSessionID());
   }

   public void testConsumerFlowCreditMessagePacket() throws Exception
   {
      ConsumerFlowCreditMessage message = new ConsumerFlowCreditMessage(RandomUtil.randomInt());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      ConsumerFlowCreditMessage copy = (ConsumerFlowCreditMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getTokens(), copy.getTokens());
   }

   public void testConnectionCreateSessionResponseMessagePacket() throws Exception
   {
      ConnectionCreateSessionResponseMessage message = new ConnectionCreateSessionResponseMessage(RandomUtil.randomLong());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      ConnectionCreateSessionResponseMessage copy = (ConnectionCreateSessionResponseMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getSessionID(), copy.getSessionID());
   }

   public void testCreateConnectionResponsePacket() throws Exception
   {
      CreateConnectionResponse message = new CreateConnectionResponse(RandomUtil.randomLong(), new VersionImpl(
              RandomUtil.randomString(),
              RandomUtil.randomInt(),
              RandomUtil.randomInt(),
              RandomUtil.randomInt(),RandomUtil.randomInt(),RandomUtil.randomString()));
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      CreateConnectionResponse copy = (CreateConnectionResponse) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getConnectionTargetID(), copy.getConnectionTargetID());
      assertEquals(message.getServerVersion().getFullVersion(), copy.getServerVersion().getFullVersion());
   }

   public void testMessagingExceptionMessagePacket() throws Exception
   {
      MessagingExceptionMessage message = new MessagingExceptionMessage(new MessagingException(RandomUtil.randomInt()));
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      MessagingExceptionMessage copy = (MessagingExceptionMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getException().getCode(), copy.getException().getCode());
   }

   public void testPingPacket() throws Exception
   {
      Ping message = new Ping(RandomUtil.randomLong());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      Ping copy = (Ping) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getSessionID(), copy.getSessionID());
   }

   public void testPongPacket() throws Exception
   {
      Pong message = new Pong(RandomUtil.randomLong(), RandomUtil.randomBoolean());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      Pong copy = (Pong) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getSessionID(), copy.getSessionID());
      assertEquals(message.isSessionFailed(), copy.isSessionFailed());
   }

   public void testProducerFlowCreditMessagePacket() throws Exception
   {
      ProducerFlowCreditMessage message = new ProducerFlowCreditMessage(RandomUtil.randomInt());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      ProducerFlowCreditMessage copy = (ProducerFlowCreditMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getTokens(), copy.getTokens());
   }

   public void testSessionAcknowledgeMessagePacket() throws Exception
   {
      SessionAcknowledgeMessage message = new SessionAcknowledgeMessage(RandomUtil.randomLong(), RandomUtil.randomBoolean());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionAcknowledgeMessage copy = (SessionAcknowledgeMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getDeliveryID(), copy.getDeliveryID());
      assertEquals(message.isAllUpTo(), copy.isAllUpTo());
   }

   public void testSessionAddDestinationMessagePacket() throws Exception
   {
      SessionAddDestinationMessage message = new SessionAddDestinationMessage(RandomUtil.randomSimpleString(), RandomUtil.randomBoolean());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionAddDestinationMessage copy = (SessionAddDestinationMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
      assertEquals(message.isTemporary(), copy.isTemporary());
   }

   public void testSessionBindingQueryMessagePacket() throws Exception
   {
      SessionBindingQueryMessage message = new SessionBindingQueryMessage(RandomUtil.randomSimpleString());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionBindingQueryMessage copy = (SessionBindingQueryMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
   }

   public void testSessionBindingQueryResponseMessagePacket() throws Exception
   {

      ArrayList<SimpleString> list = new ArrayList<SimpleString>();
      list.add(RandomUtil.randomSimpleString());
      SessionBindingQueryResponseMessage message = new SessionBindingQueryResponseMessage(RandomUtil.randomBoolean(), list);
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionBindingQueryResponseMessage copy = (SessionBindingQueryResponseMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.isExists(), copy.isExists());
      assertEquals(message.getQueueNames().get(0), copy.getQueueNames().get(0));
      assertEquals(1, message.getQueueNames().size());

   }

    public void testSessionBrowserHasNextMessageResponseMessagePacket() throws Exception
   {
      SessionBrowserHasNextMessageResponseMessage message = new SessionBrowserHasNextMessageResponseMessage(RandomUtil.randomBoolean());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionBrowserHasNextMessageResponseMessage copy = (SessionBrowserHasNextMessageResponseMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.hasNext(), copy.hasNext());

   }

    public void testSessionCancelMessagePacket() throws Exception
   {
      SessionCancelMessage message = new SessionCancelMessage(RandomUtil.randomLong(), RandomUtil.randomBoolean());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionCancelMessage copy = (SessionCancelMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getDeliveryID(), copy.getDeliveryID());
      assertEquals(message.isExpired(), copy.isExpired());

   }

    public void testSessionCreateBrowserMessagePacket() throws Exception
   {
      SessionCreateBrowserMessage message = new SessionCreateBrowserMessage(RandomUtil.randomSimpleString(), RandomUtil.randomSimpleString());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionCreateBrowserMessage copy = (SessionCreateBrowserMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getFilterString(), copy.getFilterString());
      assertEquals(message.getQueueName(), copy.getQueueName());

   }

   public void testSessionCreateBrowserResponseMessagePacket() throws Exception
   {
      SessionCreateBrowserResponseMessage message = new SessionCreateBrowserResponseMessage(RandomUtil.randomLong());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionCreateBrowserResponseMessage copy = (SessionCreateBrowserResponseMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getBrowserTargetID(), copy.getBrowserTargetID());

   }

    public void testSessionCreateConsumerMessagePacket() throws Exception
   {
      SessionCreateConsumerMessage message = new SessionCreateConsumerMessage(RandomUtil.randomLong(),
              RandomUtil.randomSimpleString(), RandomUtil.randomSimpleString(), RandomUtil.randomBoolean(), RandomUtil.randomBoolean(),
              RandomUtil.randomInt(), RandomUtil.randomInt());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionCreateConsumerMessage copy = (SessionCreateConsumerMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getClientTargetID(), copy.getClientTargetID());
      assertEquals(message.getFilterString(), copy.getFilterString());
      assertEquals(message.getMaxRate(), copy.getMaxRate());
      assertEquals(message.getQueueName(), copy.getQueueName());
      assertEquals(message.getWindowSize(), copy.getWindowSize());
      assertEquals(message.isAutoDeleteQueue(), copy.isAutoDeleteQueue());
      assertEquals(message.isNoLocal(), copy.isNoLocal());

   }

   public void testSessionCreateConsumerResponseMessagePacket() throws Exception
   {
      SessionCreateConsumerResponseMessage message = new SessionCreateConsumerResponseMessage(RandomUtil.randomLong(), RandomUtil.randomInt());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionCreateConsumerResponseMessage copy = (SessionCreateConsumerResponseMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getConsumerTargetID(), copy.getConsumerTargetID());
      assertEquals(message.getWindowSize(), copy.getWindowSize());

   }

   public void testSessionCreateProducerMessagePacket() throws Exception
   {
      SessionCreateProducerMessage message = new SessionCreateProducerMessage(RandomUtil.randomLong(),
              RandomUtil.randomSimpleString(), RandomUtil.randomInt(), RandomUtil.randomInt());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionCreateProducerMessage copy = (SessionCreateProducerMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
      assertEquals(message.getWindowSize(), copy.getWindowSize());
      assertEquals(message.getClientTargetID(), copy.getClientTargetID());
      assertEquals(message.getMaxRate(), copy.getMaxRate());

   }

   public void testSessionCreateProducerResponseMessagePacket() throws Exception
   {
      SessionCreateProducerResponseMessage message = new SessionCreateProducerResponseMessage(RandomUtil.randomLong(), RandomUtil.randomInt(),
              RandomUtil.randomInt());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionCreateProducerResponseMessage copy = (SessionCreateProducerResponseMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getInitialCredits(), copy.getInitialCredits());
      assertEquals(message.getProducerTargetID(), copy.getProducerTargetID());
      assertEquals(message.getMaxRate(), copy.getMaxRate());

   }

   public void testSessionCreateQueueMessagePacket() throws Exception
   {
      SessionCreateQueueMessage message = new SessionCreateQueueMessage(RandomUtil.randomSimpleString(),
              RandomUtil.randomSimpleString(),RandomUtil.randomSimpleString(), RandomUtil.randomBoolean(),RandomUtil.randomBoolean());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionCreateQueueMessage copy = (SessionCreateQueueMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
      assertEquals(message.getFilterString(), copy.getFilterString());
      assertEquals(message.getQueueName(), copy.getQueueName());
      assertEquals(message.isDurable(), copy.isDurable());
      assertEquals(message.isTemporary(), copy.isTemporary());
   }

   public void testSessionDeleteQueueMessagePacket() throws Exception
   {
      SessionDeleteQueueMessage message = new SessionDeleteQueueMessage(RandomUtil.randomSimpleString());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionDeleteQueueMessage copy = (SessionDeleteQueueMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getQueueName(), copy.getQueueName());
   }

   public void testSessionQueueQueryMessagePacket() throws Exception
   {
      SessionQueueQueryMessage message = new SessionQueueQueryMessage(RandomUtil.randomSimpleString());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionQueueQueryMessage copy = (SessionQueueQueryMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getQueueName(), copy.getQueueName());
   }

   public void testSessionQueueQueryResponseMessagePacket() throws Exception
   {
      SessionQueueQueryResponseMessage message = new SessionQueueQueryResponseMessage(RandomUtil.randomBoolean(),
              RandomUtil.randomBoolean(), RandomUtil.randomInt(), RandomUtil.randomInt(), RandomUtil.randomInt(),
              RandomUtil.randomSimpleString(),RandomUtil.randomSimpleString());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionQueueQueryResponseMessage copy = (SessionQueueQueryResponseMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
      assertEquals(message.getConsumerCount(), copy.getConsumerCount());
      assertEquals(message.getFilterString(), copy.getFilterString());
      assertEquals(message.getMaxSize(), copy.getMaxSize());
      assertEquals(message.getMessageCount(), copy.getMessageCount());
      assertEquals(message.isDurable(), copy.isDurable());
      assertEquals(message.isExists(), copy.isExists());
      assertEquals(message.isTemporary(), copy.isTemporary());
   }

   public void testSessionRemoveDestinationMessagePacket() throws Exception
   {
      SessionRemoveDestinationMessage message = new SessionRemoveDestinationMessage(RandomUtil.randomSimpleString(), RandomUtil.randomBoolean());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionRemoveDestinationMessage copy = (SessionRemoveDestinationMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getAddress(), copy.getAddress());
      assertEquals(message.isTemporary(), copy.isTemporary());
   }

   public void testSessionXACommitMessagePacket() throws Exception
   {
      SessionXACommitMessage message = new SessionXACommitMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()),
              RandomUtil.randomBoolean());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXACommitMessage copy = (SessionXACommitMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.isOnePhase(), copy.isOnePhase());
      assertByteArraysEquivalent(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertByteArraysEquivalent(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXAEndMessagePacket() throws Exception
   {
      SessionXAEndMessage message = new SessionXAEndMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()),
              RandomUtil.randomBoolean());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXAEndMessage copy = (SessionXAEndMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.isFailed(), copy.isFailed());
      assertByteArraysEquivalent(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertByteArraysEquivalent(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXAForgetMessagePacket() throws Exception
   {
      SessionXAForgetMessage message = new SessionXAForgetMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXAForgetMessage copy = (SessionXAForgetMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertByteArraysEquivalent(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertByteArraysEquivalent(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXAGetInDoubtXidsResponseMessagePacket() throws Exception
   {
      XidImpl xids = new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes());
      ArrayList<Xid> list = new ArrayList<Xid>();
      SessionXAGetInDoubtXidsResponseMessage message = new SessionXAGetInDoubtXidsResponseMessage(list);
      list.add(xids);
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXAGetInDoubtXidsResponseMessage copy = (SessionXAGetInDoubtXidsResponseMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertByteArraysEquivalent(message.getXids().get(0).getBranchQualifier(), copy.getXids().get(0).getBranchQualifier());
      assertEquals(message.getXids().get(0).getFormatId(), copy.getXids().get(0).getFormatId());
      assertByteArraysEquivalent(message.getXids().get(0).getGlobalTransactionId(), copy.getXids().get(0).getGlobalTransactionId());
      assertEquals(1, message.getXids().size());
   }

   public void testSessionXAGetTimeoutResponseMessagePacket() throws Exception
     {
        SessionXAGetTimeoutResponseMessage message = new SessionXAGetTimeoutResponseMessage(RandomUtil.randomInt());
        setHeaders(message);
        codec.encode(buff, message);
        buff.rewind();
        SessionXAGetTimeoutResponseMessage copy = (SessionXAGetTimeoutResponseMessage) codec.decode(buff);
        checkHeaders(message, copy);
        assertEquals(message.getTimeoutSeconds(), copy.getTimeoutSeconds());
     }

   public void testSessionXAJoinMessagePacket() throws Exception
   {
      SessionXAJoinMessage message = new SessionXAJoinMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXAJoinMessage copy = (SessionXAJoinMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertByteArraysEquivalent(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertByteArraysEquivalent(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXAPrepareMessagePacket() throws Exception
   {
      SessionXAPrepareMessage message = new SessionXAPrepareMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXAPrepareMessage copy = (SessionXAPrepareMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertByteArraysEquivalent(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertByteArraysEquivalent(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXAResponseMessagePacket() throws Exception
   {
      SessionXAResponseMessage message = new SessionXAResponseMessage(RandomUtil.randomBoolean(), RandomUtil.randomInt(), RandomUtil.randomString());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXAResponseMessage copy = (SessionXAResponseMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getMessage(), copy.getMessage());
      assertEquals(message.getResponseCode(), copy.getResponseCode());
      assertEquals(message.isError(), copy.isError());
   }

   public void testSessionXAResumeMessagePacket() throws Exception
   {
      SessionXAResumeMessage message = new SessionXAResumeMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXAResumeMessage copy = (SessionXAResumeMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertByteArraysEquivalent(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertByteArraysEquivalent(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXARollbackMessagePacket() throws Exception
   {
      SessionXARollbackMessage message = new SessionXARollbackMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXARollbackMessage copy = (SessionXARollbackMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertByteArraysEquivalent(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertByteArraysEquivalent(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   public void testSessionXASetTimeoutMessagePacket() throws Exception
   {
      SessionXASetTimeoutMessage message = new SessionXASetTimeoutMessage(RandomUtil.randomInt());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXASetTimeoutMessage copy = (SessionXASetTimeoutMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.getTimeoutSeconds(), copy.getTimeoutSeconds());
   }

   public void testSessionXASetTimeoutResponseMessagePacket() throws Exception
   {
      SessionXASetTimeoutResponseMessage message = new SessionXASetTimeoutResponseMessage(RandomUtil.randomBoolean());
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXASetTimeoutResponseMessage copy = (SessionXASetTimeoutResponseMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertEquals(message.isOK(), copy.isOK());
   }

   public void testSessionXAStartMessagePacket() throws Exception
   {
      SessionXAStartMessage message = new SessionXAStartMessage(new XidImpl(RandomUtil.randomBytes(), RandomUtil.randomInt(), RandomUtil.randomBytes()));
      setHeaders(message);
      codec.encode(buff, message);
      buff.rewind();
      SessionXAStartMessage copy = (SessionXAStartMessage) codec.decode(buff);
      checkHeaders(message, copy);
      assertByteArraysEquivalent(message.getXid().getBranchQualifier(), copy.getXid().getBranchQualifier());
      assertEquals(message.getXid().getFormatId(), copy.getXid().getFormatId());
      assertByteArraysEquivalent(message.getXid().getGlobalTransactionId(), copy.getXid().getGlobalTransactionId());
   }

   private void setHeaders(EmptyPacket packet)
   {
      packet.setExecutorID(RandomUtil.randomLong());
      packet.setResponseTargetID(RandomUtil.randomLong());
      packet.setTargetID(RandomUtil.randomLong());
   }

   private void checkHeaders(EmptyPacket emptyPacket, EmptyPacket emptyPacket2)
   {
      assertEquals(emptyPacket.getExecutorID(), emptyPacket2.getExecutorID());
      assertEquals(emptyPacket.getResponseTargetID(), emptyPacket2.getResponseTargetID());
      assertEquals(emptyPacket.getTargetID(), emptyPacket2.getTargetID());
   }
}
