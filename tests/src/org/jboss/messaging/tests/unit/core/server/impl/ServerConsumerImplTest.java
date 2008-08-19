/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.tests.unit.core.server.impl;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.easymock.IAnswer;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.server.impl.ServerConsumerImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ServerConsumerImplTest extends UnitTestCase
{
   private ServerSession serverSession;
   private Queue queue;
   private Filter filter;
   private StorageManager storageManager;
   private HierarchicalRepository<QueueSettings> repository;
   private PostOffice postOffice;
   private PacketDispatcher dispatcher;

   public void testStarted()
   {
      ServerConsumerImpl consumer = create(1, 999l, false, false, false);
      serverSession.promptDelivery(queue);
      replay(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher);
      consumer.setStarted(true);
      verify(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher);
      assertEquals(999l, consumer.getID());
      assertEquals(1, consumer.getClientTargetID());
      assertEquals(queue, consumer.getQueue());
   }

   public void testClose() throws Exception
   {
      ServerConsumerImpl consumer = create(1, 999l, false, true, false);
      expect(queue.removeConsumer(consumer)).andReturn(true);
      serverSession.removeConsumer(consumer);
      replay(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher);
      consumer.close();
      verify(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher);
      assertEquals(999l, consumer.getID());
      assertEquals(1, consumer.getClientTargetID());
      assertEquals(queue, consumer.getQueue());
   }

   public void testHandleNoAvailableCredits() throws Exception
   {
      MessageReference messageReference = createStrictMock(MessageReference.class);
      ServerConsumerImpl consumer = create(1, 999l, false, true, false);
      replay(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher);
      assertEquals(HandleStatus.BUSY, consumer.handle(messageReference));
      verify(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher);
      assertEquals(999l, consumer.getID());
      assertEquals(1, consumer.getClientTargetID());
      assertEquals(queue, consumer.getQueue());
   }

   public void testHandleExpiredMessage() throws Exception
   {
      MessageReference messageReference = createStrictMock(MessageReference.class);
      ServerMessage message = createStrictMock(ServerMessage.class);
      expect(messageReference.getMessage()).andStubReturn(message);
      ServerConsumerImpl consumer = create(1, 999l, false, true, false);
      serverSession.promptDelivery(queue);
      expect(message.isExpired()).andReturn(true);
      messageReference.expire(storageManager, postOffice, repository);
      replay(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message);
      consumer.receiveCredits(1);
      assertEquals(HandleStatus.HANDLED, consumer.handle(messageReference));
      verify(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message);
      assertEquals(999l, consumer.getID());
      assertEquals(1, consumer.getClientTargetID());
      assertEquals(queue, consumer.getQueue());
   }

   public void testHandleOnInstartedConsumer() throws Exception
   {
      MessageReference messageReference = createStrictMock(MessageReference.class);
      ServerMessage message = createStrictMock(ServerMessage.class);
      expect(messageReference.getMessage()).andStubReturn(message);
      ServerConsumerImpl consumer = create(1, 999l, false, false, false);
      serverSession.promptDelivery(queue);
      expect(message.isExpired()).andReturn(false);
      replay(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message);
      consumer.receiveCredits(1);
      assertEquals(HandleStatus.BUSY, consumer.handle(messageReference));
      verify(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message);
      assertEquals(999l, consumer.getID());
      assertEquals(1, consumer.getClientTargetID());
      assertEquals(queue, consumer.getQueue());
   }

   public void testHandleOnNoMatch() throws Exception
   {
      MessageReference messageReference = createStrictMock(MessageReference.class);
      ServerMessage message = createStrictMock(ServerMessage.class);
      expect(messageReference.getMessage()).andStubReturn(message);
      ServerConsumerImpl consumer = create(1, 999l, false, true, false);
      serverSession.promptDelivery(queue);
      expect(message.isExpired()).andReturn(false);
      expect(filter.match(message)).andReturn(false);
      replay(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message);
      consumer.receiveCredits(1);
      assertEquals(HandleStatus.NO_MATCH, consumer.handle(messageReference));
      verify(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message);
      assertEquals(999l, consumer.getID());
      assertEquals(1, consumer.getClientTargetID());
      assertEquals(queue, consumer.getQueue());
   }

   public void testHandleDelivery() throws Exception
   {
      MessageReference messageReference = createStrictMock(MessageReference.class);
      ServerMessage message = createStrictMock(ServerMessage.class);
      expect(messageReference.getMessage()).andStubReturn(message);
      ServerConsumerImpl consumer = create(1, 999l, false, true, false);
      serverSession.promptDelivery(queue);
      expect(message.isExpired()).andReturn(false);
      expect(filter.match(message)).andReturn(true);
      expect(message.getEncodeSize()).andReturn(1);
      serverSession.handleDelivery(messageReference, consumer);
      replay(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message);
      consumer.receiveCredits(1);
      assertEquals(HandleStatus.HANDLED, consumer.handle(messageReference));
      verify(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message);
      assertEquals(999l, consumer.getID());
      assertEquals(1, consumer.getClientTargetID());
      assertEquals(queue, consumer.getQueue());
   }

   public void testHandle2DeliveriesFirstUsesTokens() throws Exception
   {
      MessageReference messageReference = createStrictMock(MessageReference.class);
      ServerMessage message = createStrictMock(ServerMessage.class);
      expect(messageReference.getMessage()).andStubReturn(message);
      MessageReference messageReference2 = createStrictMock(MessageReference.class);
      ServerMessage message2 = createStrictMock(ServerMessage.class);
      expect(messageReference2.getMessage()).andStubReturn(message2);
      ServerConsumerImpl consumer = create(1, 999l, false, true, false);
      serverSession.promptDelivery(queue);
      expect(message.isExpired()).andReturn(false);
      expect(filter.match(message)).andReturn(true);
      expect(message.getEncodeSize()).andReturn(1);
      serverSession.handleDelivery(messageReference, consumer);
      replay(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message);
      consumer.receiveCredits(1);
      assertEquals(HandleStatus.HANDLED, consumer.handle(messageReference));
      assertEquals(HandleStatus.BUSY, consumer.handle(messageReference2));
      verify(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message);
      assertEquals(999l, consumer.getID());
      assertEquals(1, consumer.getClientTargetID());
      assertEquals(queue, consumer.getQueue());
   }

   public void testHandle2DeliveriesFirstUsesTokensAddTokenThenRedeliver() throws Exception
   {
      MessageReference messageReference = createStrictMock(MessageReference.class);
      ServerMessage message = createStrictMock(ServerMessage.class);
      expect(messageReference.getMessage()).andStubReturn(message);
      MessageReference messageReference2 = createStrictMock(MessageReference.class);
      ServerMessage message2 = createStrictMock(ServerMessage.class);
      expect(messageReference2.getMessage()).andStubReturn(message2);
      ServerConsumerImpl consumer = create(1, 999l, false, true, false);
      serverSession.promptDelivery(queue);
      expect(message.isExpired()).andReturn(false);
      expect(filter.match(message)).andReturn(true);
      expect(message.getEncodeSize()).andReturn(1);
      serverSession.handleDelivery(messageReference, consumer);
      serverSession.promptDelivery(queue);
      expect(message2.isExpired()).andReturn(false);
      expect(filter.match(message2)).andReturn(true);
      expect(message2.getEncodeSize()).andReturn(1);
      serverSession.handleDelivery(messageReference2, consumer);
      replay(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message, messageReference2, message2);
      consumer.receiveCredits(1);
      assertEquals(HandleStatus.HANDLED, consumer.handle(messageReference));
      assertEquals(HandleStatus.BUSY, consumer.handle(messageReference2));
      consumer.receiveCredits(1);
      assertEquals(HandleStatus.HANDLED, consumer.handle(messageReference2));
      verify(serverSession, queue, filter, storageManager, repository, postOffice, dispatcher, messageReference, message, messageReference2, message2);
      assertEquals(999l, consumer.getID());
      assertEquals(1, consumer.getClientTargetID());
      assertEquals(queue, consumer.getQueue());
   }

   private ServerConsumerImpl create(int clientId, long consumerId, boolean autoDeleteQueue, boolean started, boolean noLocal)
   {
      serverSession = createStrictMock(ServerSession.class);
      queue = createStrictMock(Queue.class);
      filter = createStrictMock(Filter.class);
      storageManager = createStrictMock(StorageManager.class);
      repository = createStrictMock(HierarchicalRepository.class);
      postOffice = createStrictMock(PostOffice.class);
      dispatcher = createStrictMock(PacketDispatcher.class);
      expect(dispatcher.generateID()).andReturn(consumerId);
      queue.addConsumer((Consumer) anyObject());
      replay(dispatcher, queue);
      ServerConsumerImpl consumer =
         new ServerConsumerImpl(serverSession, clientId, queue, filter, true, 0, started, storageManager,
              repository, postOffice, dispatcher);
      verify(dispatcher, queue);
      reset(dispatcher, queue);
      return consumer;
   }

   class promptDeliveryAnswer implements IAnswer
   {
      volatile boolean delivering;
      private CountDownLatch countDownLatch;

      public promptDeliveryAnswer(CountDownLatch countDownLatch)
      {
         this.countDownLatch = countDownLatch;
      }

      public Object answer() throws Throwable
      {
         countDownLatch.await(10000, TimeUnit.MILLISECONDS);
         return null;
      }
   }
}
