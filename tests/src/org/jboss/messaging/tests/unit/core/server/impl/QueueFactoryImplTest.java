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
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.impl.QueueFactoryImpl;
import org.jboss.messaging.core.server.impl.RoundRobinDistributor;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class QueueFactoryImplTest extends UnitTestCase
{
   public void testCreateQueue()
   {
      ScheduledExecutorService scheduledExecutor = EasyMock.createStrictMock(ScheduledExecutorService.class);
      HierarchicalRepository<AddressSettings> addressSettingsRepository = EasyMock.createStrictMock(HierarchicalRepository.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      Filter filter = EasyMock.createStrictMock(Filter.class);
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(9999);
      addressSettings.setDistributionPolicyClass("org.jboss.messaging.core.server.impl.RoundRobinDistributor");
      EasyMock.expect(addressSettingsRepository.getMatch("testQ")).andReturn(addressSettings);
      EasyMock.replay(scheduledExecutor, addressSettingsRepository);
      QueueFactoryImpl queueFactory = new QueueFactoryImpl(scheduledExecutor, addressSettingsRepository, sm);
      SimpleString qName = new SimpleString("testQ");
      Queue queue = queueFactory.createQueue(123, qName, qName, filter, true, false);
      EasyMock.verify(scheduledExecutor, addressSettingsRepository);
      assertEquals(queue.getDistributionPolicy().getClass(), RoundRobinDistributor.class);
      assertEquals(queue.getName(), qName);
      assertEquals(queue.getPersistenceID(), 123);
      assertEquals(queue.getFilter(), filter);
      assertEquals(queue.isDurable(), true);
      assertEquals(queue.isDurable(), true);
   }

   public void testCreateQueue2()
   {
      ScheduledExecutorService scheduledExecutor = EasyMock.createStrictMock(ScheduledExecutorService.class);
      HierarchicalRepository<AddressSettings> addressSettingsRepository = EasyMock.createStrictMock(HierarchicalRepository.class);
      StorageManager sm = EasyMock.createStrictMock(StorageManager.class);
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(8888);
      addressSettings.setDistributionPolicyClass(null);
      EasyMock.expect(addressSettingsRepository.getMatch("testQ2")).andReturn(addressSettings);
      EasyMock.replay(scheduledExecutor, addressSettingsRepository);
      QueueFactoryImpl queueFactory = new QueueFactoryImpl(scheduledExecutor, addressSettingsRepository, sm);
      SimpleString qName = new SimpleString("testQ2");
      Queue queue = queueFactory.createQueue(456, qName, qName, null, false, false);
      EasyMock.verify(scheduledExecutor, addressSettingsRepository);
      assertEquals(queue.getDistributionPolicy().getClass(), RoundRobinDistributor.class);
      assertEquals(queue.getName(), qName);
      assertEquals(queue.getPersistenceID(), 456);
      assertEquals(queue.getFilter(), null);
      assertEquals(queue.isDurable(), false);
      assertEquals(queue.isDurable(), false);
   }
}
