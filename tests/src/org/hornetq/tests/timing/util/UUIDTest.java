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

package org.hornetq.tests.timing.util;

import org.hornetq.utils.UUIDGenerator;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class UUIDTest extends org.hornetq.tests.unit.util.UUIDTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void main(String[] args)
   {
      long start = System.currentTimeMillis();
      int count = 10000;
      for (int i = 0; i < count; i++)
      {
         // System.out.println(i + " " + UUIDGenerator.asString(UUIDGenerator.getHardwareAddress()));
         byte[] address = UUIDGenerator.getHardwareAddress();
      }
      long end = System.currentTimeMillis();
      System.out.println("getHardwareAddress() => " + 1.0 * (end - start) / count + " ms");
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected int getTimes()
   {
      return 1000000;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
