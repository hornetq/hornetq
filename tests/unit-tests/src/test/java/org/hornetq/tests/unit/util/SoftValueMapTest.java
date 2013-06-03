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

import org.junit.Test;

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

   @Test
   public void testEvictions()
   {
      forceGC();
      long maxMemory = Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory();

      // each buffer will be 1/10th of the maxMemory
      int bufferSize = (int)(maxMemory / 100);

      SoftValueHashMap<Long, Value> softCache = new SoftValueHashMap<Long, Value>(100);

      final int MAX_ELEMENTS = 1000;

      for (long i = 0; i < MAX_ELEMENTS; i++)
      {
         softCache.put(i, new Value(new byte[bufferSize]));
      }

      assertTrue(softCache.size() < MAX_ELEMENTS);

      System.out.println("SoftCache.size " + softCache.size());

      System.out.println("Soft cache has " + softCache.size() + " elements");
   }


   @Test
   public void testEvictionsLeastUsed()
   {
      forceGC();

      SoftValueHashMap<Long, Value> softCache = new SoftValueHashMap<Long, Value>(200);

      for (long i = 0 ; i < 100; i++)
      {
         Value v = new Value(new byte[1]);
         v.setLive(true);
         softCache.put(i, v);
      }

      for (long i = 100; i < 200; i++)
      {
         Value v = new Value(new byte[1]);
         softCache.put(i, v);
      }

      assertNotNull(softCache.get(100l));

      softCache.put(300l, new Value(new byte[1]));

      // these are live, so they shouldn't go

      for (long i = 0; i < 100; i++)
      {
         assertNotNull(softCache.get(i));
      }

      // this was accessed, so it shouldn't go
      assertNotNull(softCache.get(100l));

      // this is the next one, so it should go
      assertNull(softCache.get(101l));

      System.out.println("SoftCache.size " + softCache.size());

      System.out.println("Soft cache has " + softCache.size() + " elements");
   }

   @Test
   public void testEvictOldestElement()
   {
      Value one = new Value(new byte[100]);
      Value two = new Value(new byte[100]);
      Value three = new Value(new byte[100]);


      SoftValueHashMap<Integer, Value> softCache = new SoftValueHashMap<Integer, Value>(2);
      softCache.put(3, three);
      softCache.put(2, two);
      softCache.put(1, one);

      assertNull(softCache.get(3));
      assertEquals(two, softCache.get(2));
      assertEquals(one, softCache.get(1));



   }

   class Value implements SoftValueHashMap.ValueCache
   {
      byte[] payload;

      boolean live;

      Value(byte[] payload)
      {
         this.payload = payload;
      }

      /* (non-Javadoc)
       * @see org.hornetq.utils.SoftValueHashMap.ValueCache#isLive()
       */
      public boolean isLive()
      {
         return live;
      }

      public void setLive(boolean live)
      {
         this.live = live;
      }
   }



   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
