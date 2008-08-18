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

package org.jboss.messaging.tests.unit.core.management;

import static org.jboss.messaging.tests.util.RandomUtil.*;

import java.util.Collection;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.jboss.messaging.core.management.MessageInfo;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MessageInfoTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static void assertEquals(MessageInfo expected, CompositeData actual)
   {
      assertTrue(actual.getCompositeType().equals(MessageInfo.TYPE));

      assertEquals(expected.getID(), actual.get("id"));
      assertEquals(expected.getDestination(), actual.get("destination"));
      assertEquals(expected.isDurable(), actual.get("durable"));
      assertEquals(expected.getTimestamp(), actual.get("timestamp"));
      assertEquals(expected.getType(), actual.get("type"));
      assertEquals(expected.getSize(), actual.get("size"));
      assertEquals(expected.getPriority(), actual.get("priority"));
      assertEquals(expected.isExpired(), actual.get("expired"));
      assertEquals(expected.getExpiration(), actual.get("expiration"));

      TabularData propsDatas = (TabularData) actual.get("properties");
      Collection<CompositeData> props =
         (Collection<CompositeData>) propsDatas.values();
      assertEquals(expected.getProperties().size(), props.size());
      for (CompositeData prop : props)
      {
         String actualKey = (String) prop.get("key");
         String actualValue = (String) prop.get("value");

         assertTrue(expected.getProperties().containsKey(actualKey));
         assertEquals(expected.getProperties().get(actualKey), actualValue);
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testToCompositeData() throws Exception
   {
      MessageInfo info = new MessageInfo(randomLong(), randomString(),
            randomBoolean(), randomLong(), randomByte(), randomInt(),
            randomByte(), randomBoolean(), randomLong());
      CompositeData data = info.toCompositeData();

      assertEquals(info, data);
   }

   public void testToTabularData() throws Exception
   {
      MessageInfo info_1 = new MessageInfo(randomLong(), randomString(),
            randomBoolean(), randomLong(), randomByte(), randomInt(),
            randomByte(), randomBoolean(), randomLong());
      info_1.putProperty(randomString(), randomString());
      info_1.putProperty(randomString(), randomString());
      MessageInfo info_2 = new MessageInfo(randomLong(), randomString(),
            randomBoolean(), randomLong(), randomByte(), randomInt(),
            randomByte(), randomBoolean(), randomLong());
      info_2.putProperty(randomString(), randomString());
      MessageInfo[] messages = new MessageInfo[] { info_1, info_2 };

      TabularData data = MessageInfo.toTabularData(messages);
      assertEquals(2, data.size());
      CompositeData data_1 = data.get(new Object[] { info_1.getID() });
      CompositeData data_2 = data.get(new Object[] { info_2.getID() });

      assertEquals(info_1, data_1);
      assertEquals(info_2, data_2);
   }

   public void testToTabularDataWithEmptyMessages() throws Exception
   {
      TabularData data = MessageInfo.toTabularData(new MessageInfo[0]);
      assertEquals(0, data.size());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
