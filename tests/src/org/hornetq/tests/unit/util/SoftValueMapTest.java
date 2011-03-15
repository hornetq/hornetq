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

      class Value implements SoftValueHashMap.ValueCache
      {
         byte[] payload;

         Value(byte[] payload)
         {
            this.payload = payload;
         }

         /* (non-Javadoc)
          * @see org.hornetq.utils.SoftValueHashMap.ValueCache#isLive()
          */
         public boolean isLive()
         {
            return false;
         }
      }

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


   public void testEvictionsLeastUsed()
   {
      forceGC();

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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
