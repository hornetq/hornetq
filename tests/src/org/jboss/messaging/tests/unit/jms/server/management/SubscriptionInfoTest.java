/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.jms.server.management;

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.jboss.messaging.jms.server.management.SubscriptionInfo;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SubscriptionInfoTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void assertEquals(SubscriptionInfo expected,
         CompositeData actual)
   {
      assertTrue(actual.getCompositeType().equals(SubscriptionInfo.TYPE));

      assertEquals(expected.getQueueName(), actual.get("queueName"));
      assertEquals(expected.getClientID(), actual.get("clientID"));
      assertEquals(expected.getName(), actual.get("name"));
      assertEquals(expected.isDurable(), actual.get("durable"));
      assertEquals(expected.getSelector(), actual.get("selector"));
      assertEquals(expected.getMessageCount(), actual.get("messageCount"));
      assertEquals(expected.getMaxSizeBytes(), actual.get("maxSizeBytes"));
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testToCompositeData() throws Exception
   {
      SubscriptionInfo info = new SubscriptionInfo(randomString(), randomString(),
            randomString(), randomBoolean(), randomString(), randomInt(),
            randomInt());
      CompositeData data = info.toCompositeData();

      assertEquals(info, data);
   }

   public void testToTabularData() throws Exception
   {
      SubscriptionInfo info_1 = new SubscriptionInfo(randomString(), randomString(),
            randomString(), randomBoolean(), randomString(), randomInt(),
            randomInt());
      SubscriptionInfo info_2 = new SubscriptionInfo(randomString(), randomString(),
            randomString(), randomBoolean(), randomString(), randomInt(),
            randomInt());
      SubscriptionInfo[] infos = new SubscriptionInfo[] { info_1, info_2 };

      TabularData data = SubscriptionInfo.toTabularData(infos);
      assertEquals(2, data.size());
      CompositeData data_1 = data.get(new Object[] { info_1.getQueueName() });
      CompositeData data_2 = data.get(new Object[] { info_2.getQueueName() });

      assertEquals(info_1, data_1);
      assertEquals(info_2, data_2);
   }

   public void testToTabularDataWithEmptyMessages() throws Exception
   {
      TabularData data = SubscriptionInfo.toTabularData(new SubscriptionInfo[0]);
      assertEquals(0, data.size());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
