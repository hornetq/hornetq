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

import junit.framework.Assert;

import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.UUIDGenerator;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class UUIDGeneratorTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetHardwareAddress() throws Exception
   {
      String javaVersion = System.getProperty("java.vm.version");
      if (javaVersion.startsWith("1.5"))
      {
         Assert.assertNull(UUIDGenerator.getHardwareAddress());
      }
      else if (javaVersion.startsWith("1.6"))
      {
         byte[] bytes = UUIDGenerator.getHardwareAddress();
         Assert.assertNotNull(bytes);
         Assert.assertTrue(bytes.length == 6);
      }
   }

   public void testZeroPaddedBytes() throws Exception
   {
      Assert.assertNull(UUIDGenerator.getZeroPaddedSixBytes(null));
      Assert.assertNull(UUIDGenerator.getZeroPaddedSixBytes(new byte[0]));
      Assert.assertNull(UUIDGenerator.getZeroPaddedSixBytes(new byte[7]));

      byte[] fiveBytes = new byte[] { 1, 2, 3, 4, 5 };
      byte[] zeroPaddedFiveBytes = UUIDGenerator.getZeroPaddedSixBytes(fiveBytes);
      UnitTestCase.assertEqualsByteArrays(new byte[] { 1, 2, 3, 4, 5, 0 }, zeroPaddedFiveBytes);

      byte[] fourBytes = new byte[] { 1, 2, 3, 4 };
      byte[] zeroPaddedFourBytes = UUIDGenerator.getZeroPaddedSixBytes(fourBytes);
      UnitTestCase.assertEqualsByteArrays(new byte[] { 1, 2, 3, 4, 0, 0 }, zeroPaddedFourBytes);

      byte[] threeBytes = new byte[] { 1, 2, 3 };
      byte[] zeroPaddedThreeBytes = UUIDGenerator.getZeroPaddedSixBytes(threeBytes);
      UnitTestCase.assertEqualsByteArrays(new byte[] { 1, 2, 3, 0, 0, 0 }, zeroPaddedThreeBytes);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
