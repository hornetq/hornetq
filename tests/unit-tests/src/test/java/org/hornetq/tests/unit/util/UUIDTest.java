/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.unit.util;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.UUID;
import org.hornetq.utils.UUIDGenerator;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class UUIDTest extends UnitTestCase
{
   static final int MANY_TIMES = 100000;

   public void testManyUUIDs() throws Exception
   {
      Set<String> uuidsSet = new HashSet<String>();

      UUIDGenerator gen = UUIDGenerator.getInstance();
      for (int i = 0; i < getTimes(); i++)
      {
         uuidsSet.add(gen.generateStringUUID());
      }

      // we put them in a set to check duplicates
      Assert.assertEquals(getTimes(), uuidsSet.size());
   }

   protected int getTimes()
   {
      return MANY_TIMES;
   }

   public void testStringToUuidConversion()
   {
      UUIDGenerator gen = UUIDGenerator.getInstance();
      for (int i = 0; i < MANY_TIMES; i++)
      {
         final UUID uuid = gen.generateUUID();
         final String uuidString = uuid.toString();
         byte[] data2 = UUID.stringToBytes(uuidString);
         final UUID uuid2 = new UUID(UUID.TYPE_TIME_BASED, data2);
         assertEqualsByteArrays(uuid.asBytes(), data2);
         assertEquals(uuidString, uuid, uuid2);
         assertEquals(uuidString, uuidString, uuid2.toString());
      }
   }
}
