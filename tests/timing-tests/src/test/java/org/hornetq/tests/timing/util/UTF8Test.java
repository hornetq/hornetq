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
import org.junit.After;

import org.junit.Test;

import org.junit.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.UTF8Util;

/**
 *
 * A UTF8Test
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * Created Feb 23, 2009 11:57:55 AM
 *
 *
 */
public class UTF8Test extends UnitTestCase
{

   private final String str = "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5" + "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5"
                              + "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5"
                              + "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5"
                              + "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5"
                              + "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5"
                              + "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5"
                              + "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5"
                              + "abcdef&^*&!^ghijkl\uB5E2\uCAC7\uB2BB\uB7DD\uB7C7\uB3A3\uBCE4\uB5A5";

   final int TIMES = 5;

   final long numberOfIteractions = 1000000;

   @Test
   public void testWriteUTF() throws Exception
   {
      HornetQBuffer buffer = HornetQBuffers.fixedBuffer(10 * 1024);

      long start = System.currentTimeMillis();

      for (int c = 0; c < TIMES; c++)
      {
         for (long i = 0; i < numberOfIteractions; i++)
         {
            if (i == 10000)
            {
               start = System.currentTimeMillis();
            }

            buffer.clear();
            buffer.writeUTF(str);
         }

         long spentTime = System.currentTimeMillis() - start;

         System.out.println("Time WriteUTF = " + spentTime);
      }
   }

   @Test
   public void testReadUTF() throws Exception
   {
      HornetQBuffer buffer = HornetQBuffers.fixedBuffer(10 * 1024);

      buffer.writeUTF(str);

      long start = System.currentTimeMillis();

      for (int c = 0; c < TIMES; c++)
      {
         for (long i = 0; i < numberOfIteractions; i++)
         {
            if (i == 10000)
            {
               start = System.currentTimeMillis();
            }

            buffer.resetReaderIndex();
            String newstr = buffer.readUTF();
            Assert.assertEquals(str, newstr);
         }

         long spentTime = System.currentTimeMillis() - start;

         System.out.println("Time readUTF = " + spentTime);
      }

   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      UTF8Util.clearBuffer();
      super.tearDown();
   }
}
