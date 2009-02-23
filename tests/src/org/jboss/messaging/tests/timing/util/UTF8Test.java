/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.timing.util;

import junit.framework.TestCase;

import org.jboss.messaging.integration.transports.netty.ChannelBufferWrapper;
import org.jboss.messaging.util.UTF8Util;

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
public class UTF8Test extends TestCase
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


   public void testWriteUTF() throws Exception
   {
      ChannelBufferWrapper buffer = new ChannelBufferWrapper(10 * 1024);

      long start = System.currentTimeMillis();

      for (int c = 0; c < TIMES; c++)
      {
         for (long i = 0; i < numberOfIteractions; i++)
         {
            if (i == 10000)
            {
               start = System.currentTimeMillis();
            }

            buffer.rewind();
            buffer.putUTF(str);
         }

         long spentTime = System.currentTimeMillis() - start;

         System.out.println("Time WriteUTF = " + spentTime);
      }
   }

   public void testReadUTF() throws Exception
   {
      ChannelBufferWrapper buffer = new ChannelBufferWrapper(10 * 1024);

      buffer.putUTF(str);

      long start = System.currentTimeMillis();

      for (int c = 0; c < TIMES; c++)
      {
         for (long i = 0; i < numberOfIteractions; i++)
         {
            if (i == 10000)
            {
               start = System.currentTimeMillis();
            }

            buffer.rewind();
            String newstr = buffer.getUTF();
            assertEquals(str, newstr);
         }

         long spentTime = System.currentTimeMillis() - start;

         System.out.println("Time readUTF = " + spentTime);
      }

   }
   
   protected void tearDown() throws Exception
   {
      UTF8Util.clearBuffer();
      super.tearDown();
   }
}
