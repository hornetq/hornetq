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

package org.jboss.messaging.tests.unit.util;

import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.UUIDGenerator;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
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
         assertNull(UUIDGenerator.getHardwareAddress());
      } else if (javaVersion.startsWith("1.6"))
      {
         byte[] bytes = UUIDGenerator.getHardwareAddress();
         assertNotNull(bytes);
         assertTrue(bytes.length == 6);
      }
   }
   
   public void testZeroPaddedBytes() throws Exception
   {
      assertNull(UUIDGenerator.getZeroPaddedSixBytes(null));
      assertNull(UUIDGenerator.getZeroPaddedSixBytes(new byte[0]));
      assertNull(UUIDGenerator.getZeroPaddedSixBytes(new byte[7]));

      byte[] fiveBytes = new byte[] {1, 2, 3, 4, 5};
      byte[] zeroPaddedFiveBytes = UUIDGenerator.getZeroPaddedSixBytes(fiveBytes);
      UnitTestCase.assertEqualsByteArrays(new byte[] {1, 2, 3, 4, 5, 0}, zeroPaddedFiveBytes);

      byte[] fourBytes = new byte[] {1, 2, 3, 4};
      byte[] zeroPaddedFourBytes = UUIDGenerator.getZeroPaddedSixBytes(fourBytes);
      UnitTestCase.assertEqualsByteArrays(new byte[] {1, 2, 3, 4, 0, 0}, zeroPaddedFourBytes);

      byte[] threeBytes = new byte[] {1, 2, 3};
      byte[] zeroPaddedThreeBytes = UUIDGenerator.getZeroPaddedSixBytes(threeBytes);
      UnitTestCase.assertEqualsByteArrays(new byte[] {1, 2, 3, 0, 0, 0}, zeroPaddedThreeBytes);
   }
   
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
