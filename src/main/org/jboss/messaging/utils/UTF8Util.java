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

package org.jboss.messaging.utils;

import java.io.IOException;
import java.lang.ref.SoftReference;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * 
 * A UTF8Util
 * 
 * This class will write UTFs directly to the ByteOutput (through the MessageBuffer interface)
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Feb 20, 2009 1:37:18 PM
 *
 *
 */
public class UTF8Util
{
   static boolean optimizeStrings = true;

   private static final Logger log = Logger.getLogger(UTF8Util.class);

   private static final boolean isDebug = log.isDebugEnabled();

   private static ThreadLocal<SoftReference<StringUtilBuffer>> currenBuffer = new ThreadLocal<SoftReference<StringUtilBuffer>>();

   public static void saveUTF(final MessagingBuffer out, final String str) throws IOException
   {
      StringUtilBuffer buffer = getThreadLocalBuffer();

      if (str.length() > 0xffff)
      {
         throw new IllegalArgumentException("the specified string is too long (" + str.length() + ")");
      }

      final int len = calculateUTFSize(str, buffer);

      if (len > 0xffff)
      {
         throw new IllegalArgumentException("the encoded string is too long (" + len + ")");
      }

      out.putShort((short)len);

      if (len > buffer.byteBuffer.length)
      {
         buffer.resizeByteBuffer(len);
      }

      if (len == (long)str.length())
      {
         for (int byteLocation = 0; byteLocation < len; byteLocation++)
         {
            buffer.byteBuffer[byteLocation] = (byte)buffer.charBuffer[byteLocation];
         }
         out.putBytes(buffer.byteBuffer, 0, len);
      }
      else
      {
         if (isDebug)
         {
            log.debug("Saving string with utfSize=" + len + " stringSize=" + str.length());
         }

         int stringLength = str.length();

         int charCount = 0;

         for (int i = 0; i < stringLength; i++)
         {
            char charAtPos = buffer.charBuffer[i];
            if (charAtPos >= 1 && charAtPos < 0x7f)
            {
               buffer.byteBuffer[charCount++] = (byte)charAtPos;
            }
            else if (charAtPos >= 0x800)
            {
               buffer.byteBuffer[charCount++] = (byte)(0xE0 | charAtPos >> 12 & 0x0F);
               buffer.byteBuffer[charCount++] = (byte)(0x80 | charAtPos >> 6 & 0x3F);
               buffer.byteBuffer[charCount++] = (byte)(0x80 | charAtPos >> 0 & 0x3F);
            }
            else
            {
               buffer.byteBuffer[charCount++] = (byte)(0xC0 | charAtPos >> 6 & 0x1F);
               buffer.byteBuffer[charCount++] = (byte)(0x80 | charAtPos >> 0 & 0x3F);

            }
         }
         out.putBytes(buffer.byteBuffer, 0, len);
      }
   }

   public static String readUTF(final MessagingBuffer input) throws IOException
   {
      StringUtilBuffer buffer = getThreadLocalBuffer();

      final int size = input.getUnsignedShort();

      if (size > buffer.byteBuffer.length)
      {
         buffer.resizeByteBuffer(size);
      }

      if (size > buffer.charBuffer.length)
      {
         buffer.resizeCharBuffer(size);
      }

      if (isDebug)
      {
         log.debug("Reading string with utfSize=" + size);
      }

      int count = 0;
      int byte1, byte2, byte3;
      int charCount = 0;

      input.getBytes(buffer.byteBuffer, 0, size);

      while (count < size)
      {
         byte1 = buffer.byteBuffer[count++];

         if (byte1 > 0 && byte1 <= 0x7F)
         {
            buffer.charBuffer[charCount++] = (char)byte1;
         }
         else
         {
            int c = byte1 & 0xff;
            switch (c >> 4)
            {
               case 0xc:
               case 0xd:
                  byte2 = buffer.byteBuffer[count++];
                  buffer.charBuffer[charCount++] = (char)((c & 0x1F) << 6 | byte2 & 0x3F);
                  break;
               case 0xe:
                  byte2 = buffer.byteBuffer[count++];
                  byte3 = buffer.byteBuffer[count++];
                  buffer.charBuffer[charCount++] = (char)((c & 0x0F) << 12 | (byte2 & 0x3F) << 6 | (byte3 & 0x3F) << 0);
                  break;
            }
         }
      }

      return new String(buffer.charBuffer, 0, charCount);

   }

   private static StringUtilBuffer getThreadLocalBuffer()
   {
      SoftReference<StringUtilBuffer> softReference = currenBuffer.get();
      StringUtilBuffer value;
      if (softReference == null)
      {
         value = new StringUtilBuffer();
         softReference = new SoftReference<StringUtilBuffer>(value);
         currenBuffer.set(softReference);
      }
      else
      {
         value = softReference.get();
      }

      if (value == null)
      {
         value = new StringUtilBuffer();
         softReference = new SoftReference<StringUtilBuffer>(value);
         currenBuffer.set(softReference);
      }

      return value;
   }

   public static void clearBuffer()
   {
      SoftReference<StringUtilBuffer> ref = currenBuffer.get();
      if (ref.get() != null)
      {
         ref.clear();
      }
   }

   public static int calculateUTFSize(final String str, final StringUtilBuffer stringBuffer)
   {
      int calculatedLen = 0;
      
      int stringLength = str.length();

      if (stringLength > stringBuffer.charBuffer.length)
      {
         stringBuffer.resizeCharBuffer(stringLength);
      }
      
      str.getChars(0, stringLength, stringBuffer.charBuffer, 0);

      for (int i = 0; i < stringLength; i++)
      {
         char c = stringBuffer.charBuffer[i];

         if (c >= 1 && c < 0x7f)
         {
            calculatedLen++;
         }
         else if (c >= 0x800)
         {
            calculatedLen += 3;
         }
         else
         {
            calculatedLen += 2;
         }
      }
      return calculatedLen;
   }

   private static class StringUtilBuffer
   {

      public char charBuffer[];

      public byte byteBuffer[];

      public void resizeCharBuffer(final int newSize)
      {
         if (newSize > charBuffer.length)
         {
            charBuffer = new char[newSize];
         }
      }

      public void resizeByteBuffer(final int newSize)
      {
         if (newSize > byteBuffer.length)
         {
            this.byteBuffer = new byte[newSize];
         }
      }

      public StringUtilBuffer()
      {
         this(1024, 1024);
      }

      public StringUtilBuffer(final int sizeChar, final int sizeByte)
      {
         charBuffer = new char[sizeChar];
         byteBuffer = new byte[sizeByte];
      }

   }

}
