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

package org.hornetq.tests.unit.util.sizeof;

import org.hornetq.tests.util.UnitTestCase;

/**
 * A Base class for tests that are calculating size of objects
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public abstract class SizeOfBase extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private final Runtime runtime = Runtime.getRuntime();

   private static final int numberOfObjects = 10000;

   public void testCalculateSize()
   {
      getMemorySize();
      newObject();
      
      int i = 0;
      long heap1 = 0;
      long heap2 = 0;
      long totalMemory1 = 0;
      long totalMemory2 = 0;


      final Object obj[] = new Object[numberOfObjects];
      // make sure we load the classes before

      heap1 = getMemorySize();

      totalMemory1 = runtime.totalMemory();

      for (i = 0; i < numberOfObjects; i++)
      {
         obj[i] = newObject();
      }

      heap2 = getMemorySize();

      totalMemory2 = runtime.totalMemory();

      final int size = Math.round(((float)(heap2 - heap1)) / numberOfObjects);

      if (totalMemory1 != totalMemory2)
      {
         System.out.println("Warning: JVM allocated more data what would make results invalid");
      }

      System.out.println("heap1 = " + heap1 + ", heap2 = " + heap2 + ", size = " + size);

   }

   private long getMemorySize()
   {
      for (int i = 0; i < 5; i++)
      {
         forceGC();
      }
      return runtime.totalMemory() - runtime.freeMemory();
   }

   protected abstract Object newObject();

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
