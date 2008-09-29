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
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.GroupingRoundRobinDistributionPolicy;
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

   public void testSingleConsumerSingleGroup()
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      EasyMock.replay(consumer, serverMessage);
      assertEquals(consumer, policy.select( serverMessage, false));
      assertEquals(consumer, policy.select(serverMessage, false));
      EasyMock.verify(consumer, serverMessage);
   }

   public void testMultipleConsumersSingleGroup()
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      EasyMock.replay(consumer, consumer2, consumer3, serverMessage);
      assertEquals(consumer, policy.select(serverMessage, false));
      assertEquals(consumer, policy.select(serverMessage, false));
      EasyMock.verify(consumer, consumer2, consumer3, serverMessage);
   }

   public void testSingleConsumerTwoGroups()
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      ServerMessage serverMessage2 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage2.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid2"));
      EasyMock.replay(consumer, serverMessage, serverMessage2);
      assertEquals(consumer, policy.select(serverMessage, false));
      assertEquals(consumer, policy.select(serverMessage2, false));
      EasyMock.verify(consumer, serverMessage2);
   }

   public void testMultipleConsumersTwoGroups()
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      ServerMessage serverMessage2 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage2.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid2"));
      EasyMock.replay(consumer, consumer2, consumer3, serverMessage, serverMessage2);
      assertEquals(consumer, policy.select(serverMessage, false));
      assertEquals(consumer2, policy.select(serverMessage2, false));
      assertEquals(consumer, policy.select(serverMessage, false));
      assertEquals(consumer2, policy.select(serverMessage2, false));
      EasyMock.verify(consumer, consumer2, consumer3, serverMessage, serverMessage2);
   }

   public void testMultipleConsumersSingleGroupFirstDeliveryFailed()
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      EasyMock.replay(consumer, consumer2, consumer3, serverMessage);
      assertEquals(consumer, policy.select(serverMessage, false));
      assertEquals(consumer2, policy.select(serverMessage, true));
      EasyMock.verify(consumer, consumer2, consumer3, serverMessage);
   }

   public void testMultipleConsumersSingleGroupSecondDeliveryFailed()
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      EasyMock.replay(consumer, consumer2, consumer3, serverMessage);
      assertEquals(consumer, policy.select(serverMessage, false));
      assertEquals(consumer, policy.select(serverMessage, false));
      assertEquals(null, policy.select(serverMessage, true));
      EasyMock.verify(consumer, consumer2, consumer3, serverMessage);
   }

   public void testMultipleConsumersMultipleGroupMultipleGroupsEach()
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      ServerMessage serverMessage2 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage2.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid2"));
      ServerMessage serverMessage3 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage3.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid3"));
      ServerMessage serverMessage4 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage4.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid4"));
      ServerMessage serverMessage5 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage5.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid5"));
      ServerMessage serverMessage6 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage6.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid6"));
      ServerMessage serverMessage7 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage7.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid7"));
      ServerMessage serverMessage8 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage8.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid8"));
      ServerMessage serverMessage9 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage9.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid9"));
      EasyMock.replay(consumer, consumer2, consumer3, serverMessage, serverMessage2, serverMessage3, serverMessage4,
                      serverMessage5, serverMessage6, serverMessage7, serverMessage8, serverMessage9);
      assertEquals(consumer, policy.select(serverMessage, false));
      assertEquals(consumer2, policy.select(serverMessage2, false));
      assertEquals(consumer3, policy.select(serverMessage3, false));
      assertEquals(consumer, policy.select(serverMessage4, false));
      assertEquals(consumer2, policy.select(serverMessage5, false));
      assertEquals(consumer3, policy.select(serverMessage6, false));
      assertEquals(consumer, policy.select(serverMessage7, false));
      assertEquals(consumer2, policy.select(serverMessage8, false));
      assertEquals(consumer3, policy.select(serverMessage9, false));

      EasyMock.verify(consumer, consumer2, consumer3, serverMessage, serverMessage2, serverMessage3, serverMessage4,
                      serverMessage5, serverMessage6, serverMessage7, serverMessage8, serverMessage9);
   }

   public void testMultipleConsumersConsumerRemoved()
   {
      Consumer consumer = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer2 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer3 = EasyMock.createStrictMock(Consumer.class);
      Consumer consumer4 = EasyMock.createStrictMock(Consumer.class);
      policy.addConsumer(consumer);
      policy.addConsumer(consumer2);
      policy.addConsumer(consumer3);
      policy.addConsumer(consumer4);
      ServerMessage serverMessage = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid1"));
      ServerMessage serverMessage2 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage2.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid2"));
      ServerMessage serverMessage3 = EasyMock.createStrictMock(ServerMessage.class);
      EasyMock.expect(serverMessage3.getProperty(MessageImpl.GROUP_ID)).andStubReturn(new SimpleString("gid3"));
      EasyMock.replay(consumer, consumer2, consumer3, consumer4, serverMessage, serverMessage2, serverMessage3);
      assertEquals(consumer, policy.select(serverMessage, false));
      assertEquals(consumer2, policy.select(serverMessage2, false));
      assertEquals(consumer3, policy.select(serverMessage3, false));
      policy.removeConsumer(consumer2);
      assertEquals(consumer, policy.select(serverMessage2, false));
      EasyMock.verify(consumer, consumer2, consumer3, consumer4, serverMessage, serverMessage2, serverMessage3);
   }


}
