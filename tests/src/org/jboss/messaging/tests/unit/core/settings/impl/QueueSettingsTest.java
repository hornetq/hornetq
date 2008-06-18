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

package org.jboss.messaging.tests.unit.core.settings.impl;

import junit.framework.TestCase;

import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class QueueSettingsTest extends TestCase
{
   public void testDefaults()
   {
      QueueSettings queueSettings = new QueueSettings();
      assertEquals(queueSettings.getDistributionPolicy().getClass(), QueueSettings.DEFAULT_DISTRIBUTION_POLICY.getClass());
      assertEquals(queueSettings.getDistributionPolicyClass(), null);
      assertEquals(queueSettings.getDLQ(), null);
      assertEquals(queueSettings.isClustered(), Boolean.valueOf(false));
      assertEquals(queueSettings.getExpiryQueue(), null);
      assertEquals(queueSettings.getMaxDeliveryAttempts(), QueueSettings.DEFAULT_MAX_DELIVERY_ATTEMPTS);
      assertEquals(queueSettings.getMaxSizeBytes(), QueueSettings.DEFAULT_MAX_SIZE_BYTES);
      assertEquals(queueSettings.getMessageCounterHistoryDayLimit(), QueueSettings.DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT);
      assertEquals(queueSettings.getRedeliveryDelay(), QueueSettings.DEFAULT_REDELIVER_DELAY);

   }

   public void testSingleMerge()
   {
      QueueSettings queueSettings = new QueueSettings();
      QueueSettings queueSettingsToMerge = new QueueSettings();
      queueSettingsToMerge.setClustered(true);
      SimpleString DLQ = new SimpleString("testDLQ");
      SimpleString exp = new SimpleString("testExpiryQueue");
      queueSettingsToMerge.setDLQ(DLQ);
      queueSettingsToMerge.setExpiryQueue(exp);
      queueSettingsToMerge.setMaxDeliveryAttempts(1000);
      queueSettingsToMerge.setMaxSizeBytes(1001);
      queueSettingsToMerge.setMessageCounterHistoryDayLimit(1002);
      queueSettingsToMerge.setRedeliveryDelay((long)1003);
      queueSettings.merge(queueSettingsToMerge);
      assertEquals(queueSettings.getDistributionPolicy().getClass(), QueueSettings.DEFAULT_DISTRIBUTION_POLICY.getClass());
      assertEquals(queueSettings.getDistributionPolicyClass(), null);
      assertEquals(queueSettings.isClustered(), Boolean.valueOf(true));
      assertEquals(queueSettings.getDLQ(), DLQ);
      assertEquals(queueSettings.getExpiryQueue(), exp);
      assertEquals(queueSettings.getMaxDeliveryAttempts(), Integer.valueOf(1000));
      assertEquals(queueSettings.getMaxSizeBytes(), Integer.valueOf(1001));
      assertEquals(queueSettings.getMessageCounterHistoryDayLimit(), Integer.valueOf(1002));
      assertEquals(queueSettings.getRedeliveryDelay(), Long.valueOf(1003));
   }

   public void testMultipleMerge()
   {
      QueueSettings queueSettings = new  QueueSettings();
      QueueSettings queueSettingsToMerge = new QueueSettings();
      queueSettingsToMerge.setClustered(true);
      SimpleString DLQ = new SimpleString("testDLQ");
      SimpleString exp = new SimpleString("testExpiryQueue");
      queueSettingsToMerge.setDLQ(DLQ);
      queueSettingsToMerge.setExpiryQueue(exp);
      queueSettingsToMerge.setMaxDeliveryAttempts(1000);
      queueSettingsToMerge.setMaxSizeBytes(1001);
      queueSettingsToMerge.setMessageCounterHistoryDayLimit(1002);
      queueSettings.merge(queueSettingsToMerge);

      QueueSettings queueSettingsToMerge2 = new QueueSettings();
      queueSettingsToMerge2.setClustered(true);
      SimpleString exp2 = new SimpleString("testExpiryQueue2");
      queueSettingsToMerge2.setExpiryQueue(exp2);
      queueSettingsToMerge2.setMaxSizeBytes(2001);
      queueSettingsToMerge2.setRedeliveryDelay((long)2003);
      queueSettings.merge(queueSettingsToMerge2);

      assertEquals(queueSettings.getDistributionPolicy().getClass(), QueueSettings.DEFAULT_DISTRIBUTION_POLICY.getClass());
      assertEquals(queueSettings.getDistributionPolicyClass(), null);
      assertEquals(queueSettings.isClustered(), Boolean.valueOf(true));
      assertEquals(queueSettings.getDLQ(), DLQ);
      assertEquals(queueSettings.getExpiryQueue(), exp);
      assertEquals(queueSettings.getMaxDeliveryAttempts(), Integer.valueOf(1000));
      assertEquals(queueSettings.getMaxSizeBytes(), Integer.valueOf(1001));
      assertEquals(queueSettings.getMessageCounterHistoryDayLimit(), Integer.valueOf(1002));
      assertEquals(queueSettings.getRedeliveryDelay(), Long.valueOf(2003));
   }

   public void testMultipleMergeAll()
   {
      QueueSettings queueSettings = new  QueueSettings();
      QueueSettings queueSettingsToMerge = new QueueSettings();
      queueSettingsToMerge.setClustered(true);
      SimpleString DLQ = new SimpleString("testDLQ");
      SimpleString exp = new SimpleString("testExpiryQueue");
      queueSettingsToMerge.setDLQ(DLQ);
      queueSettingsToMerge.setExpiryQueue(exp);
      queueSettingsToMerge.setMaxSizeBytes(1001);
      queueSettingsToMerge.setRedeliveryDelay((long)1003);
      queueSettings.merge(queueSettingsToMerge);

      QueueSettings queueSettingsToMerge2 = new QueueSettings();
      queueSettingsToMerge2.setClustered(false);
      SimpleString exp2 = new SimpleString("testExpiryQueue2");
      SimpleString DLQ2 = new SimpleString("testDlq2");
      queueSettingsToMerge2.setExpiryQueue(exp2);
      queueSettingsToMerge2.setDLQ(DLQ2);
      queueSettingsToMerge2.setMaxDeliveryAttempts(2000);
      queueSettingsToMerge2.setMaxSizeBytes(2001);
      queueSettingsToMerge2.setMessageCounterHistoryDayLimit(2002);
      queueSettingsToMerge2.setRedeliveryDelay((long)2003);
      queueSettings.merge(queueSettingsToMerge2);

      assertEquals(queueSettings.getDistributionPolicy().getClass(), QueueSettings.DEFAULT_DISTRIBUTION_POLICY.getClass());
      assertEquals(queueSettings.getDistributionPolicyClass(), null);
      assertEquals(queueSettings.isClustered(), Boolean.valueOf(true));
      assertEquals(queueSettings.getDLQ(), DLQ);
      assertEquals(queueSettings.getExpiryQueue(), exp);
      assertEquals(queueSettings.getMaxDeliveryAttempts(), Integer.valueOf(2000));
      assertEquals(queueSettings.getMaxSizeBytes(), Integer.valueOf(1001));
      assertEquals(queueSettings.getMessageCounterHistoryDayLimit(), Integer.valueOf(2002));
      assertEquals(queueSettings.getRedeliveryDelay(), Long.valueOf(1003));
   }
}
