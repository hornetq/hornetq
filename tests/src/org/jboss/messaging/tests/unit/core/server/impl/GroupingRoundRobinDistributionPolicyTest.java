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

import org.easymock.EasyMock;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.DistributionPolicy;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.GroupingRoundRobinDistributionPolicy;
import org.jboss.messaging.core.server.impl.RoundRobinDistributionPolicy;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class GroupingRoundRobinDistributionPolicyTest extends UnitTestCase
{
   GroupingRoundRobinDistributionPolicy policy = null;

   protected void setUp() throws Exception
   {
      policy = new GroupingRoundRobinDistributionPolicy();
   }

   protected void tearDown() throws Exception
   {
      policy = null;
   }

   public void testSingleConsumerSingleGroup() throws Exception
   {
      MessageReference reference = EasyMock.createStrictMock(MessageReference.class);
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference.getMessage()).andStubReturn(serverMessage);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(consumer, serverMessage, reference);
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      EasyMock.verify(consumer, serverMessage, reference);
   }

   public void testRunOutOfConsumers() throws Exception
   {
      MessageReference reference = EasyMock.createStrictMock(MessageReference.class);
      Consumer c1 = EasyMock.createStrictMock(Consumer.class);
      Consumer c2 = EasyMock.createStrictMock(Consumer.class);
      Consumer c3 = EasyMock.createStrictMock(Consumer.class);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference.getMessage()).andStubReturn(serverMessage);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      DistributionPolicy dp = new RoundRobinDistributionPolicy();
      dp.addConsumer(c1);
      dp.addConsumer(c2);
      dp.addConsumer(c3);
      EasyMock.expect(c1.handle(reference)).andReturn(HandleStatus.BUSY);
      EasyMock.expect(c2.handle(reference)).andReturn(HandleStatus.BUSY);
      EasyMock.expect(c3.handle(reference)).andReturn(HandleStatus.BUSY);
      EasyMock.replay(reference, c1, c2, c3, serverMessage);

      HandleStatus status = dp.distribute(reference);
      assertEquals(status, HandleStatus.BUSY);
      EasyMock.verify(reference, c1, c2, c3, serverMessage);
   }

   public void testRunOutOfConsumersNoMatch() throws Exception
   {
      MessageReference reference = EasyMock.createStrictMock(MessageReference.class);
      Consumer c1 = EasyMock.createStrictMock(Consumer.class);
      Consumer c2 = EasyMock.createStrictMock(Consumer.class);
      Consumer c3 = EasyMock.createStrictMock(Consumer.class);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference.getMessage()).andStubReturn(serverMessage);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      DistributionPolicy dp = new RoundRobinDistributionPolicy();
      dp.addConsumer(c1);
      dp.addConsumer(c2);
      dp.addConsumer(c3);
      EasyMock.expect(c1.handle(reference)).andReturn(HandleStatus.NO_MATCH);
      EasyMock.expect(c2.handle(reference)).andReturn(HandleStatus.NO_MATCH);
      EasyMock.expect(c3.handle(reference)).andReturn(HandleStatus.NO_MATCH);
      EasyMock.replay(reference, c1, c2, c3, serverMessage);

      HandleStatus status = dp.distribute(reference);
      assertEquals(status, HandleStatus.NO_MATCH);
      EasyMock.verify(reference, c1, c2, c3, serverMessage);
   }

   public void testMultipleConsumersSingleGroup() throws Exception
   {
      MessageReference reference = EasyMock.createStrictMock(MessageReference.class);
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference.getMessage()).andStubReturn(serverMessage);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(consumer, consumer2, consumer3, serverMessage, reference);
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      EasyMock.verify(consumer, consumer2, consumer3, serverMessage, reference);
   }

   public void testSingleConsumerTwoGroups() throws Exception
   {
      MessageReference reference = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference2 = EasyMock.createStrictMock(MessageReference.class);
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference.getMessage()).andStubReturn(serverMessage);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      ServerMessage serverMessage2 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference2.getMessage()).andStubReturn(serverMessage2);
      EasyMock.expect(serverMessage2.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid2"));
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(consumer, serverMessage, serverMessage2, reference, reference2);
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      EasyMock.verify(consumer, serverMessage2, reference, reference2);
   }

   public void testMultipleConsumersTwoGroups() throws Exception
   {
      MessageReference reference = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference2 = EasyMock.createStrictMock(MessageReference.class);
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference.getMessage()).andStubReturn(serverMessage);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      ServerMessage serverMessage2 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference2.getMessage()).andStubReturn(serverMessage2);
      EasyMock.expect(serverMessage2.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid2"));
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer2.handle(reference2)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer2.handle(reference2)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(consumer, consumer2, consumer3, serverMessage, serverMessage2, reference, reference2);
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference2));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference2));
      EasyMock.verify(consumer, consumer2, consumer3, serverMessage, serverMessage2, reference, reference2);
   }

   public void testMultipleConsumersSingleGroupFirstDeliveryFailed() throws Exception
   {
      MessageReference reference = EasyMock.createStrictMock(MessageReference.class);
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference.getMessage()).andStubReturn(serverMessage);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.BUSY);
      EasyMock.expect(consumer2.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(consumer, consumer2, consumer3, serverMessage, reference);
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      EasyMock.verify(consumer, consumer2, consumer3, serverMessage, reference);
   }

   public void testMultipleConsumersSingleGroupSecondDeliveryFailed() throws Exception
   {
      MessageReference reference = EasyMock.createStrictMock(MessageReference.class);
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference.getMessage()).andStubReturn(serverMessage);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.BUSY);
      EasyMock.replay(consumer, consumer2, consumer3, serverMessage, reference);
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      assertEquals(HandleStatus.BUSY, policy.distribute(reference));
      EasyMock.verify(consumer, consumer2, consumer3, serverMessage, reference);
   }

   public void testMultipleConsumersMultipleGroupMultipleGroupsEach() throws Exception
   {
      MessageReference reference = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference2 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference3 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference4 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference5 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference6 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference7 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference8 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference9 = EasyMock.createStrictMock(MessageReference.class);
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference.getMessage()).andStubReturn(serverMessage);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      ServerMessage serverMessage2 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference2.getMessage()).andStubReturn(serverMessage2);
      EasyMock.expect(serverMessage2.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid2"));
      ServerMessage serverMessage3 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference3.getMessage()).andStubReturn(serverMessage3);
      EasyMock.expect(serverMessage3.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid3"));
      ServerMessage serverMessage4 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference4.getMessage()).andStubReturn(serverMessage4);
      EasyMock.expect(serverMessage4.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid4"));
      ServerMessage serverMessage5 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference5.getMessage()).andStubReturn(serverMessage5);
      EasyMock.expect(serverMessage5.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid5"));
      ServerMessage serverMessage6 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference6.getMessage()).andStubReturn(serverMessage6);
      EasyMock.expect(serverMessage6.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid6"));
      ServerMessage serverMessage7 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference7.getMessage()).andStubReturn(serverMessage7);
      EasyMock.expect(serverMessage7.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid7"));
      ServerMessage serverMessage8 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference8.getMessage()).andStubReturn(serverMessage8);
      EasyMock.expect(serverMessage8.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid8"));
      ServerMessage serverMessage9 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference9.getMessage()).andStubReturn(serverMessage9);
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer2.handle(reference2)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer3.handle(reference3)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(reference4)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer2.handle(reference5)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer3.handle(reference6)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(reference7)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer2.handle(reference8)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer3.handle(reference9)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(serverMessage9.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid9"));
      EasyMock.replay(consumer, consumer2, consumer3, serverMessage, serverMessage2, serverMessage3, serverMessage4,
                      serverMessage5, serverMessage6, serverMessage7, serverMessage8, serverMessage9, reference,
                      reference2, reference3, reference4, reference5, reference6, reference7, reference8, reference9);
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference2));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference3));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference4));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference5));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference6));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference7));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference8));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference9));

      EasyMock.verify(consumer, consumer2, consumer3, serverMessage, serverMessage2, serverMessage3, serverMessage4,
                      serverMessage5, serverMessage6, serverMessage7, serverMessage8, serverMessage9, reference,
                      reference2, reference3, reference4, reference5, reference6, reference7, reference8, reference9);
   }

   public void testMultipleConsumersConsumerRemoved() throws Exception
   {
      MessageReference reference = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference2 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference reference3 = EasyMock.createStrictMock(MessageReference.class);
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer4 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      policy.addConsumer(consumer4);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference.getMessage()).andStubReturn(serverMessage);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      ServerMessage serverMessage2 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference2.getMessage()).andStubReturn(serverMessage2);
      EasyMock.expect(serverMessage2.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid2"));
      ServerMessage serverMessage3 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(reference3.getMessage()).andStubReturn(serverMessage3);
      EasyMock.expect(serverMessage3.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid3"));
      EasyMock.expect(consumer.handle(reference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer2.handle(reference2)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer3.handle(reference3)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(consumer.handle(reference2)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(consumer, consumer2, consumer3, consumer4, serverMessage, serverMessage2, serverMessage3,
                      reference, reference2, reference3);
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference2));
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference3));
      policy.removeConsumer(consumer2);
      assertEquals(HandleStatus.HANDLED, policy.distribute(reference2));
      EasyMock.verify(consumer, consumer2, consumer3, consumer4, serverMessage, serverMessage2, serverMessage3,
                      reference, reference2, reference3);
   }


}
