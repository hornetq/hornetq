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

package org.jboss.messaging.tests.unit.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.DataConstants;
import org.jboss.messaging.utils.Random;
import org.jboss.messaging.utils.UTF8Util;

/**
 * A UTF8Test
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Feb 23, 2009 12:50:57 PM
 *
 *
 */
public class UTF8Test extends UnitTestCase
{

   public void testValidateUTF() throws Exception
   {
      MessagingBuffer buffer = ChannelBuffers.buffer(60 * 1024); 

      byte[] bytes = new byte[20000];

      Random random = new Random();
      random.getRandom().nextBytes(bytes);

      String str = new String(bytes);

      UTF8Util.saveUTF(buffer, str);

      String newStr = UTF8Util.readUTF(buffer);

      assertEquals(str, newStr);
   }

   public void testValidateUTFOnDataInput() throws Exception
   {
      for (int i = 0; i < 100; i++)
      {
         Random random = new Random();

         // Random size between 15k and 20K
         byte[] bytes = new byte[15000 + RandomUtil.randomPositiveInt() % 5000];

         random.getRandom().nextBytes(bytes);

         String str = new String(bytes);
         
         // The maximum size the encoded UTF string would reach is str.length * 3 (look at the UTF8 implementation)
         testValidateUTFOnDataInputStream(str, ChannelBuffers.wrappedBuffer(ByteBuffer.allocate(str.length() * 3 + DataConstants.SIZE_SHORT))); 

         testValidateUTFOnDataInputStream(str, ChannelBuffers.dynamicBuffer(100));

         testValidateUTFOnDataInputStream(str, ChannelBuffers.buffer(100 * 1024));
      }
   }

   private void testValidateUTFOnDataInputStream(final String str, MessagingBuffer wrap) throws Exception
   {
      UTF8Util.saveUTF(wrap, str);

      DataInputStream data = new DataInputStream(new ByteArrayInputStream(wrap.array()));

      String newStr = data.readUTF();

      assertEquals(str, newStr);

      ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
      DataOutputStream outData = new DataOutputStream(byteOut);

      outData.writeUTF(str);

      MessagingBuffer buffer = ChannelBuffers.wrappedBuffer(byteOut.toByteArray());

      newStr = UTF8Util.readUTF(buffer);

      assertEquals(str, newStr);
   }

   public void testBigSize() throws Exception
   {

      char[] chars = new char[0xffff + 1];

      for (int i = 0; i < chars.length; i++)
      {
         chars[i] = ' ';
      }

      String str = new String(chars);

      MessagingBuffer buffer = ChannelBuffers.buffer(0xffff + 4);

      try
      {
         UTF8Util.saveUTF(buffer, str);
         fail("String is too big, supposed to throw an exception");
      }
      catch (Exception ignored)
      {
      }

      assertEquals("A buffer was supposed to be untouched since the string was too big", 0, buffer.writerIndex());

      chars = new char[25000];

      for (int i = 0; i < chars.length; i++)
      {
         chars[i] = 0x810;
      }

      str = new String(chars);

      try
      {
         UTF8Util.saveUTF(buffer, str);
         fail("Encoded String is too big, supposed to throw an exception");
      }
      catch (Exception ignored)
      {
      }

      assertEquals("A buffer was supposed to be untouched since the string was too big", 0, buffer.writerIndex());

      // Testing a string right on the limit
      chars = new char[0xffff];

      for (int i = 0; i < chars.length; i++)
      {
         chars[i] = (char)(i % 100 + 1);
      }

      str = new String(chars);

      UTF8Util.saveUTF(buffer, str);

      assertEquals(0xffff + DataConstants.SIZE_SHORT, buffer.writerIndex());

      String newStr = UTF8Util.readUTF(buffer);

      assertEquals(str, newStr);

   }

   @Override
   protected void tearDown() throws Exception
   {
      UTF8Util.clearBuffer();
      super.tearDown();
   }
}
