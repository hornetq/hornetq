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

package org.jboss.messaging.util;

import java.io.IOException;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * 
 * A UTFUtil
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Feb 20, 2009 1:37:18 PM
 *
 *
 */
public class UTFUtil
{
   static boolean optimizeStrings = true;

   private static final Logger log = Logger.getLogger(UTFUtil.class);

   private static final boolean isDebug = log.isDebugEnabled();

   private static ThreadLocal<StringUtilBuffer> currenBuffer = new ThreadLocal<StringUtilBuffer>();

   private static void flushByteBuffer(final MessagingBuffer out, final byte[] byteBuffer, final Position pos) throws IOException
   {
      out.putBytes(byteBuffer, 0, pos.pos);
      pos.pos = 0;
   }

   public static void saveUTF(final MessagingBuffer out, final String str) throws IOException
   {
      StringUtilBuffer buffer = getThreadLocalBuffer();

      int len = calculateUTFSize(str, buffer);
      if (len > 0xffff)
      {
         out.putBoolean(true);
         out.putInt(len);
      }
      else
      {
         out.putBoolean(false);
         out.putShort((short)len);
      }

      if (len == (long)str.length())
      {
         if (len > buffer.byteBuffer.length)
         {
            buffer.resizeByteBuffer(len);
         }

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

         Position pos = buffer.position.reset();

         int stringLength = str.length();
         for (int bufferPosition = 0; bufferPosition < stringLength;)
         {
            int countArray = Math.min(stringLength - bufferPosition, buffer.charBuffer.length);
            str.getChars(bufferPosition, bufferPosition + countArray, buffer.charBuffer, 0);

            for (int i = 0; i < countArray; i++)
            {
               char charAtPos = buffer.charBuffer[i];
               if (charAtPos >= 1 && charAtPos < 0x7f)
               {
                  if (pos.pos >= buffer.byteBuffer.length)
                  {
                     flushByteBuffer(out, buffer.byteBuffer, pos);
                  }
                  buffer.byteBuffer[pos.pos++] = (byte)charAtPos;
               }
               else if (charAtPos >= 0x800)
               {
                  if (pos.pos + 3 >= buffer.byteBuffer.length)
                  {
                     flushByteBuffer(out, buffer.byteBuffer, pos);
                  }

                  buffer.byteBuffer[pos.pos++] = (byte)(0xE0 | charAtPos >> 12 & 0x0F);
                  buffer.byteBuffer[pos.pos++] = (byte)(0x80 | charAtPos >> 6 & 0x3F);
                  buffer.byteBuffer[pos.pos++] = (byte)(0x80 | charAtPos >> 0 & 0x3F);
               }
               else
               {
                  if (pos.pos + 2 >= buffer.byteBuffer.length)
                  {
                     flushByteBuffer(out, buffer.byteBuffer, pos);
                  }

                  buffer.byteBuffer[pos.pos++] = (byte)(0xC0 | charAtPos >> 6 & 0x1F);
                  buffer.byteBuffer[pos.pos++] = (byte)(0x80 | charAtPos >> 0 & 0x3F);

               }
            }

            bufferPosition += countArray;
         }
         flushByteBuffer(out, buffer.byteBuffer, pos);
      }
   }

   public static String readUTF(final MessagingBuffer input) throws IOException
   {
      StringUtilBuffer buffer = getThreadLocalBuffer();

      long size = 0;

      boolean isLong = input.getBoolean();

      if (isLong)
      {
         size = input.getInt();
      }
      else
      {
         size = input.getShort();
      }

      if (isDebug)
      {
         log.debug("Reading string with utfSize=" + size + " isLong=" + isLong);
      }

      long count = 0;
      int byte1, byte2, byte3;
      int charCount = 0;
      Position pos = buffer.position.reset();;
      StringBuffer strbuffer = null;

      while (count < size)
      {
         if (pos.pos >= pos.size)
         {
            if (isDebug)
            {
               log.debug("readString::pulling data to Buffer at pos " + pos.pos + " size= " + pos.size);
            }
            pullDataFromBuffer(input, pos, buffer.byteBuffer, count, size);
         }
         byte1 = buffer.byteBuffer[pos.pos++];
         count++;

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
                  if (pos.pos >= pos.size)
                  {
                     if (isDebug)
                     {
                        log.debug("readString::pulling data to Buffer at pos test1 " + pos.pos + " size= " + pos.size);
                     }
                     pullDataFromBuffer(input, pos, buffer.byteBuffer, count, size);
                  }
                  byte2 = buffer.byteBuffer[pos.pos++];
                  buffer.charBuffer[charCount++] = (char)((c & 0x1F) << 6 | byte2 & 0x3F);
                  count++;
                  break;
               case 0xe:
                  if (pos.pos >= pos.size)
                  {
                     if (isDebug)
                     {
                        log.debug("readString::pulling data to Buffer at pos test2 " + pos.pos + " size= " + pos.size);
                     }
                     pullDataFromBuffer(input, pos, buffer.byteBuffer, count, size);
                  }
                  byte2 = buffer.byteBuffer[pos.pos++];
                  count++;
                  if (pos.pos >= pos.size)
                  {
                     pullDataFromBuffer(input, pos, buffer.byteBuffer, count, size);
                  }
                  byte3 = buffer.byteBuffer[pos.pos++];
                  buffer.charBuffer[charCount++] = (char)((c & 0x0F) << 12 | (byte2 & 0x3F) << 6 | (byte3 & 0x3F) << 0);
                  count++;

                  break;
            }
         }

         if (charCount == buffer.charBuffer.length)
         {
            if (strbuffer == null)
            {
               strbuffer = new StringBuffer((int)size);
            }
            strbuffer.append(buffer.charBuffer);
            charCount = 0;
         }
      }

      if (strbuffer != null)
      {
         strbuffer.append(buffer.charBuffer, 0, charCount);
         return strbuffer.toString();
      }
      else
      {
         return new String(buffer.charBuffer, 0, charCount);
      }

   }

   private static StringUtilBuffer getThreadLocalBuffer()
   {
      StringUtilBuffer retValue = currenBuffer.get();
      if (retValue == null)
      {
         retValue = new StringUtilBuffer();
         currenBuffer.set(retValue);
      }

      return retValue;
   }

   private static void pullDataFromBuffer(final MessagingBuffer input,
                                          final Position pos,
                                          final byte[] byteBuffer,
                                          final long currentPosition,
                                          final long size) throws IOException
   {
      pos.pos = 0;

      pos.size = (int)Math.min(size - currentPosition, byteBuffer.length);

      input.getBytes(byteBuffer, 0, (int)pos.size);
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

   /* A way to pass an integer as a parameter to a method */
   public static class Position
   {
      int pos;

      long size;

      public Position reset()
      {
         pos = 0;
         size = 0;
         return this;
      }
   }

   private static class StringUtilBuffer
   {

      Position position = new Position();

      public char charBuffer[];

      public byte byteBuffer[];

      public void resizeCharBuffer(final int newSize)
      {
         if (newSize <= charBuffer.length)
         {
            throw new RuntimeException("New buffer can't be smaller");
         }
         char[] newCharBuffer = new char[newSize];
         for (int i = 0; i < charBuffer.length; i++)
         {
            newCharBuffer[i] = charBuffer[i];
         }
         charBuffer = newCharBuffer;
      }

      public void resizeByteBuffer(final int newSize)
      {
         System.out.println("Buffer size = " + newSize);
         if (newSize <= byteBuffer.length)
         {
            throw new RuntimeException("New buffer can't be smaller");
         }
         byte[] newByteBuffer = new byte[newSize];
         for (int i = 0; i < byteBuffer.length; i++)
         {
            newByteBuffer[i] = byteBuffer[i];
         }
         byteBuffer = newByteBuffer;
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
