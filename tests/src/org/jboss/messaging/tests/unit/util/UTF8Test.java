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

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.ExpandingMessagingBuffer;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.integration.transports.netty.ChannelBufferWrapper;
import org.jboss.messaging.util.DataConstants;
import org.jboss.messaging.util.Random;
import org.jboss.messaging.util.UTF8Util;

/**
 * A UTF8Test
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Feb 23, 2009 12:50:57 PM
 *
 *
 */
public class UTF8Test extends TestCase
{

   public void testValidateUTF() throws Exception
   {
      ChannelBufferWrapper buffer = new ChannelBufferWrapper(10 * 1024);

      byte[] bytes = new byte[20000];

      Random random = new Random();
      random.getRandom().nextBytes(bytes);

      String str = new String(bytes);

      UTF8Util.saveUTF(buffer, str);

      buffer.rewind();

      String newStr = UTF8Util.readUTF(buffer);

      assertEquals(str, newStr);
   }

   public void testValidateUTFOnDataInput() throws Exception
   {
      for (int i = 0; i < 100; i++)
      {
         byte[] bytes = new byte[20000];

         Random random = new Random();
         random.getRandom().nextBytes(bytes);

         String str = new String(bytes);

         // The maximum size the encoded UTF string would reach is str.length * 3 (look at the UTF8 implementation)
         testValidateUTFOnDataInputStream(str, new ByteBufferWrapper(ByteBuffer.allocate(str.length() * 3 + DataConstants.SIZE_SHORT)));

         testValidateUTFOnDataInputStream(str, new ExpandingMessagingBuffer(100));

         testValidateUTFOnDataInputStream(str, new ChannelBufferWrapper(100 * 1024));
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

      ByteBuffer buffer = ByteBuffer.wrap(byteOut.toByteArray());
      wrap = new ByteBufferWrapper(buffer);

      wrap.rewind();

      newStr = UTF8Util.readUTF(wrap);

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

      ChannelBufferWrapper buffer = new ChannelBufferWrapper(0xffff + 4);

      try
      {
         UTF8Util.saveUTF(buffer, str);
         fail("String is too big, supposed to throw an exception");
      }
      catch (Exception ignored)
      {
      }

      assertEquals(0, buffer.position());

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

      assertEquals(0, buffer.position());

      // Testing a string right on the limit
      chars = new char[0xffff];

      for (int i = 0; i < chars.length; i++)
      {
         chars[i] = (char)(i % 100 + 1);
      }

      str = new String(chars);

      UTF8Util.saveUTF(buffer, str);

      assertEquals(0xffff + DataConstants.SIZE_SHORT, buffer.position());

      buffer.rewind();

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
