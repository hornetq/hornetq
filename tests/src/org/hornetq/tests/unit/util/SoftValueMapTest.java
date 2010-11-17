/*
 * Copyright 2010 Red Hat, Inc.
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

import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SoftValueHashMap;

/**
 * A SoftValueMapTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class SoftValueMapTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testEvictions()
   {
      forceGC();
      long maxMemory = Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory();
      
      // each buffer will be 1/10th of the maxMemory
      int bufferSize = (int)(maxMemory / 100);
      
      SoftValueHashMap<Long, byte[]> softCache = new SoftValueHashMap<Long, byte[]>();
      
      final int MAX_ELEMENTS = 1000;
      
      for (long i = 0 ; i < MAX_ELEMENTS; i++)
      {
         softCache.put(i, new byte[bufferSize]);
      }
      
      
      assertTrue(softCache.size() < MAX_ELEMENTS);
      
      System.out.println("Soft cache has " + softCache.size() + " elements");
   }

   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
