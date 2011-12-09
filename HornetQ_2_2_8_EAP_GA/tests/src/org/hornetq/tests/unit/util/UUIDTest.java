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
import org.hornetq.utils.UUIDGenerator;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class UUIDTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testManyUUIDs() throws Exception
   {
      final int MANY_TIMES = getTimes();
      Set<String> uuidsSet = new HashSet<String>();

      UUIDGenerator gen = UUIDGenerator.getInstance();
      for (int i = 0; i < MANY_TIMES; i++)
      {
         uuidsSet.add(gen.generateStringUUID());
      }

      // we put them in a set to check duplicates
      Assert.assertEquals(MANY_TIMES, uuidsSet.size());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected int getTimes()
   {
      return 100000;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
