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

import java.util.concurrent.ScheduledExecutorService;

import org.easymock.EasyMock;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.impl.QueueFactoryImpl;
import org.jboss.messaging.core.server.impl.RoundRobinDistributionPolicy;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class QueueFactoryImplTest extends UnitTestCase
{
   public void testCreateQueue()
   {
      ScheduledExecutorService scheduledExecutor = EasyMock.createStrictMock(ScheduledExecutorService.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = EasyMock.createStrictMock(HierarchicalRepository.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setClustered(true);
      queueSettings.setMaxSizeBytes(9999);
      queueSettings.setDistributionPolicyClass("org.jboss.messaging.core.server.impl.RoundRobinDistributionPolicy");
      EasyMock.expect(queueSettingsRepository.getMatch("testQ")).andReturn(queueSettings);
      EasyMock.replay(scheduledExecutor, queueSettingsRepository);
      QueueFactoryImpl queueFactory = new QueueFactoryImpl(scheduledExecutor, queueSettingsRepository);
      SimpleString qName = new SimpleString("testQ");
      Queue queue = queueFactory.createQueue(123, qName, filter, true, true);
      EasyMock.verify(scheduledExecutor, queueSettingsRepository);
      assertEquals(queue.getDistributionPolicy().getClass(), RoundRobinDistributionPolicy.class);
      assertEquals(queue.isClustered(), true);
      assertEquals(queue.getMaxSizeBytes(), 9999);
      assertEquals(queue.getName(), qName);
      assertEquals(queue.getPersistenceID(), 123);
      assertEquals(queue.getFilter(), filter);
      assertEquals(queue.isDurable(), true);
      assertEquals(queue.isTemporary(), true);
   }

   public void testCreateQueue2()
   {
      ScheduledExecutorService scheduledExecutor = EasyMock.createStrictMock(ScheduledExecutorService.class);
      HierarchicalRepository<QueueSettings> queueSettingsRepository = EasyMock.createStrictMock(HierarchicalRepository.class);
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setClustered(false);
      queueSettings.setMaxSizeBytes(8888);
      queueSettings.setDistributionPolicyClass(null);
      EasyMock.expect(queueSettingsRepository.getMatch("testQ2")).andReturn(queueSettings);
      EasyMock.replay(scheduledExecutor, queueSettingsRepository);
      QueueFactoryImpl queueFactory = new QueueFactoryImpl(scheduledExecutor, queueSettingsRepository);
      SimpleString qName = new SimpleString("testQ2");
      Queue queue = queueFactory.createQueue(456, qName, null, false, false);
      EasyMock.verify(scheduledExecutor, queueSettingsRepository);
      assertEquals(queue.getDistributionPolicy().getClass(), RoundRobinDistributionPolicy.class);
      assertEquals(queue.isClustered(), false);
      assertEquals(queue.getMaxSizeBytes(), 8888);
      assertEquals(queue.getName(), qName);
      assertEquals(queue.getPersistenceID(), 456);
      assertEquals(queue.getFilter(), null);
      assertEquals(queue.isDurable(), false);
      assertEquals(queue.isTemporary(), false);
   }
}
