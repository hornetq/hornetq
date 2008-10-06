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

import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.DistributionPolicy;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.impl.RoundRobinDistributionPolicy;
import org.jboss.messaging.tests.unit.core.server.impl.fakes.FakeConsumer;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.easymock.EasyMock;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * A RoundRobinDistributionPolicyTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RoundRobinDistributionPolicyTest extends UnitTestCase
{

   public void testNoConsumers()
   {
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
      
      DistributionPolicy dp = new RoundRobinDistributionPolicy();

      EasyMock.replay(messageReference);
      HandleStatus status = dp.distribute(messageReference);
      EasyMock.verify(messageReference);
      assertEquals(status, HandleStatus.BUSY);
   }
   
   public void testConsumers() throws Exception
   {
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
      Consumer c1 = EasyMock.createStrictMock(Consumer.class);
      Consumer c2 = EasyMock.createStrictMock(Consumer.class);
      Consumer c3 = EasyMock.createStrictMock(Consumer.class);
      
      DistributionPolicy dp = new RoundRobinDistributionPolicy();
      dp.addConsumer(c1);
      dp.addConsumer(c2);
      dp.addConsumer(c3);
      EasyMock.expect(c1.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(c2.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(c3.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(c1.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(c2.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(c3.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.expect(c1.handle(messageReference)).andReturn(HandleStatus.HANDLED);
      EasyMock.replay(messageReference, c1, c2, c3);
      
      dp.distribute(messageReference);
      dp.distribute(messageReference);
      dp.distribute(messageReference);
      dp.distribute(messageReference);
      dp.distribute(messageReference);
      dp.distribute(messageReference);
      dp.distribute(messageReference);
      EasyMock.verify(messageReference, c1, c2, c3);
   }

   public void testRunOutOfConsumers() throws Exception
   {
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
      Consumer c1 = EasyMock.createStrictMock(Consumer.class);
      Consumer c2 = EasyMock.createStrictMock(Consumer.class);
      Consumer c3 = EasyMock.createStrictMock(Consumer.class);

      DistributionPolicy dp = new RoundRobinDistributionPolicy();
      dp.addConsumer(c1);
      dp.addConsumer(c2);
      dp.addConsumer(c3);
      EasyMock.expect(c1.handle(messageReference)).andReturn(HandleStatus.BUSY);
      EasyMock.expect(c2.handle(messageReference)).andReturn(HandleStatus.BUSY);
      EasyMock.expect(c3.handle(messageReference)).andReturn(HandleStatus.BUSY);
      EasyMock.replay(messageReference, c1, c2, c3);

      HandleStatus status = dp.distribute(messageReference);
      assertEquals(status, HandleStatus.BUSY);
      EasyMock.verify(messageReference, c1, c2, c3);
   }
   public void testRunOutOfConsumersNoMatch() throws Exception
   {
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
      Consumer c1 = EasyMock.createStrictMock(Consumer.class);
      Consumer c2 = EasyMock.createStrictMock(Consumer.class);
      Consumer c3 = EasyMock.createStrictMock(Consumer.class);

      DistributionPolicy dp = new RoundRobinDistributionPolicy();
      dp.addConsumer(c1);
      dp.addConsumer(c2);
      dp.addConsumer(c3);
      EasyMock.expect(c1.handle(messageReference)).andReturn(HandleStatus.NO_MATCH);
      EasyMock.expect(c2.handle(messageReference)).andReturn(HandleStatus.NO_MATCH);
      EasyMock.expect(c3.handle(messageReference)).andReturn(HandleStatus.NO_MATCH);
      EasyMock.replay(messageReference, c1, c2, c3);

      HandleStatus status = dp.distribute(messageReference);
      assertEquals(status, HandleStatus.NO_MATCH);
      EasyMock.verify(messageReference, c1, c2, c3);
   }

   
}
