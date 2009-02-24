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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomByte;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.jms.server.management.JMSMessageInfo;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class JMSMessageInfoTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void assertJMSMessageInfoEquals(JMSMessageInfo expected,
         CompositeData actual)
   {
      assertTrue(actual.getCompositeType().equals(JMSMessageInfo.TYPE));

      assertEquals(expected.getJMSMessageID(), actual.get("JMSMessageID"));
      assertEquals(expected.getJMSCorrelationID(), actual
            .get("JMSCorrelationID"));
      assertEquals(expected.getJMSDeliveryMode(), actual.get("JMSDeliveryMode"));
      assertEquals(expected.getJMSPriority(), actual.get("JMSPriority"));
      assertEquals(expected.getJMSReplyTo(), actual.get("JMSReplyTo"));
      assertEquals(expected.getJMSTimestamp(), actual.get("JMSTimestamp"));
      assertEquals(expected.getJMSType(), actual.get("JMSType"));
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
      JMSMessageInfo info = new JMSMessageInfo(randomString(), randomString(),
            randomString(), randomInt(), randomString(), randomLong(),
            randomLong(), randomString());
      info.putProperty(randomString(), randomString());
      CompositeData data = info.toCompositeData();

      assertJMSMessageInfoEquals(info, data);
   }

   public void testToTabularData() throws Exception
   {
      JMSMessageInfo info_1 = new JMSMessageInfo(randomString(),
            randomString(), randomString(), randomInt(), randomString(),
            randomLong(), randomLong(), randomString());
      info_1.putProperty(randomString(), randomString());
      info_1.putProperty(randomString(), randomString());
      JMSMessageInfo info_2 = new JMSMessageInfo(randomString(),
            randomString(), randomString(), randomInt(), randomString(),
            randomLong(), randomLong(), randomString());
      info_2.putProperty(randomString(), randomString());
      JMSMessageInfo[] messages = new JMSMessageInfo[] { info_1, info_2 };

      TabularData data = JMSMessageInfo.toTabularData(messages);
      assertEquals(2, data.size());
      CompositeData data_1 = data
            .get(new Object[] { info_1.getJMSMessageID() });
      CompositeData data_2 = data
            .get(new Object[] { info_2.getJMSMessageID() });

      assertJMSMessageInfoEquals(info_1, data_1);
      assertJMSMessageInfoEquals(info_2, data_2);
   }

   public void testToTabularDataWithEmptyMessages() throws Exception
   {
      TabularData data = JMSMessageInfo.toTabularData(new JMSMessageInfo[0]);
      assertEquals(0, data.size());
   }

   public void testFromServerMessage() throws Exception
   {
      ServerMessage message = createMock(ServerMessage.class);
      expect(message.getProperty(new SimpleString("JMSMessageID"))).andStubReturn(randomSimpleString());
      expect(message.getProperty(new SimpleString("JMSCorrelationID"))).andStubReturn(randomSimpleString());
      expect(message.getProperty(new SimpleString("JMSType"))).andStubReturn(randomSimpleString());
      expect(message.isDurable()).andStubReturn(randomBoolean());
      expect(message.getPriority()).andStubReturn(randomByte());
      expect(message.getProperty(new SimpleString("JMSReplyTo"))).andStubReturn(randomSimpleString());
      expect(message.getTimestamp()).andStubReturn(randomLong());
      expect(message.getExpiration()).andStubReturn(randomLong());
      Set<SimpleString> propNames = new HashSet<SimpleString>();
      propNames.add(new SimpleString("foo"));
      expect(message.getPropertyNames()).andStubReturn(propNames);
      expect(message.getProperty(new SimpleString("foo"))).andStubReturn(randomSimpleString());

      replay(message);
      JMSMessageInfo info = JMSMessageInfo.fromServerMessage(message );

      verify(message);

   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
