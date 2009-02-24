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

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.management.RoleInfo;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class RoleInfoTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static void assertEquals(RoleInfo expected, CompositeData actual)
   {
      assertTrue(actual.getCompositeType().equals(RoleInfo.TYPE));

      assertEquals(expected.getName(), actual.get("name"));
      assertEquals(expected.isCreate(), actual.get("create"));
      assertEquals(expected.isRead(), actual.get("read"));
      assertEquals(expected.isWrite(), actual.get("write"));
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testToCompositeData() throws Exception
   {
      String name = randomString();
      boolean create = randomBoolean();
      boolean read = randomBoolean();
      boolean write = randomBoolean();

      RoleInfo info = new RoleInfo(name, create, read, write);
      CompositeData data = info.toCompositeData();

      assertEquals(info, data);
   }

   public void testToTabularData() throws Exception
   {
      RoleInfo info_1 = new RoleInfo(randomString(), randomBoolean(),
            randomBoolean(), randomBoolean());
      RoleInfo info_2 = new RoleInfo(randomString(), randomBoolean(),
            randomBoolean(), randomBoolean());
      RoleInfo[] roles = new RoleInfo[] { info_1, info_2 };

      TabularData data = RoleInfo.toTabularData(roles);
      assertEquals(2, data.size());
      CompositeData data_1 = data.get(new String[] { info_1.getName() });
      CompositeData data_2 = data.get(new String[] { info_2.getName() });

      assertEquals(info_1, data_1);
      assertEquals(info_2, data_2);
   }

   public void testToTabularDataWithEmptyRoles() throws Exception
   {
      TabularData data = RoleInfo.toTabularData(new RoleInfo[0]);
      assertEquals(0, data.size());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
