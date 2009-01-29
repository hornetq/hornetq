/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

import static org.jboss.messaging.util.DataConstants.SIZE_INT;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A SimpleString
 * 
 * A simple String class that can store all characters, and stores as simple byte[],
 * this minimises expensive copying between String objects
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SimpleString implements CharSequence, Serializable, Comparable<SimpleString>
{
   private static final long serialVersionUID = 4204223851422244307L;

   private static final Logger log = Logger.getLogger(SimpleString.class);

   // Attributes
   // ------------------------------------------------------------------------
   private final byte[] data;

   private transient int hash;

   // Cache the string
   private transient String str;

   // Static
   // ----------------------------------------------------------------------

   /**
    * Returns a SimpleString constructed from the <code>string</code> parameter.
    * If <code>string</code> is <code>null</code>, the return value will be <code>null</code> too.
    */
   public static SimpleString toSimpleString(final String string)
   {
      if (string == null)
      {
         return null;
      }
      return new SimpleString(string);
   }

   // Constructors
   // ----------------------------------------------------------------------

   public SimpleString(final String string)
   {
      int len = string.length();

      data = new byte[len << 1];

      int j = 0;

      for (int i = 0; i < len; i++)
      {
         char c = string.charAt(i);

         byte low = (byte)(c & 0xFF); // low byte

         data[j++] = low;

         byte high = (byte)(c >> 8 & 0xFF); // high byte

         data[j++] = high;
      }

      str = string;
   }

   public SimpleString(final byte[] data)
   {
      this.data = data;
   }

   // CharSequence implementation
   // ---------------------------------------------------------------------------

   public int length()
   {
      return data.length >> 1;
   }

   public char charAt(int pos)
   {
      if (pos < 0 || pos >= data.length >> 1)
      {
         throw new IndexOutOfBoundsException();
      }
      pos <<= 1;

      return (char)(data[pos] | data[pos + 1] << 8);
   }

   public CharSequence subSequence(final int start, final int end)
   {
      int len = data.length >> 1;

      if (end < start || start < 0 || end > len)
      {
         throw new IndexOutOfBoundsException();
      }
      else
      {
         int newlen = (end - start) << 1;
         byte[] bytes = new byte[newlen];

         System.arraycopy(data, start << 1, bytes, 0, newlen);

         return new SimpleString(bytes);
      }
   }

   // Comparable implementation -------------------------------------

   public int compareTo(SimpleString o)
   {
      return toString().compareTo(o.toString());
   }

   // Public
   // ---------------------------------------------------------------------------

   public byte[] getData()
   {
      return data;
   }

   public boolean startsWith(final SimpleString other)
   {
      byte[] otherdata = other.data;

      if (otherdata.length > this.data.length)
      {
         return false;
      }

      for (int i = 0; i < otherdata.length; i++)
      {
         if (this.data[i] != otherdata[i])
         {
            return false;
         }
      }

      return true;
   }

   public String toString()
   {
      if (str == null)
      {
         int len = data.length >> 1;

         char[] chars = new char[len];

         int j = 0;

         for (int i = 0; i < len; i++)
         {
            int low = data[j++] & 0xFF;

            int high = (data[j++] << 8) & 0xFF00;

            chars[i] = (char)(low | high);
         }

         str = new String(chars);
      }

      return str;
   }

   public boolean equals(Object other)
   {
      if (other instanceof SimpleString)
      {
         SimpleString s = (SimpleString)other;

         if (data.length != s.data.length)
         {
            return false;
         }

         for (int i = 0; i < data.length; i++)
         {
            if (data[i] != s.data[i])
            {
               return false;
            }
         }

         return true;
      }
      else
      {
         return false;
      }
   }

   public int hashCode()
   {
      if (hash == 0)
      {
         for (int i = 0; i < data.length; i++)
         {
            hash = 31 * hash + data[i];
         }
      }

      return hash;
   }

   public SimpleString[] split(char delim)
   {
      if (!contains(delim))
      {
         return new SimpleString[] { this };
      }
      else
      {
         List<SimpleString> all = new ArrayList<SimpleString>();
         int lasPos = 0;
         for (int i = 0; i < data.length; i += 2)
         {
            byte low = (byte)(delim & 0xFF); // low byte
            byte high = (byte)(delim >> 8 & 0xFF); // high byte
            if (data[i] == low && data[i + 1] == high)
            {
               byte[] bytes = new byte[i - lasPos];
               System.arraycopy(data, lasPos, bytes, 0, bytes.length);
               lasPos = i + 2;
               all.add(new SimpleString(bytes));
            }
         }
         byte[] bytes = new byte[data.length - lasPos];
         System.arraycopy(data, lasPos, bytes, 0, bytes.length);
         all.add(new SimpleString(bytes));
         SimpleString[] parts = new SimpleString[all.size()];
         return all.toArray(parts);
      }
   }

   public boolean contains(char c)
   {
      for (int i = 0; i < data.length; i += 2)
      {
         byte low = (byte)(c & 0xFF); // low byte
         byte high = (byte)(c >> 8 & 0xFF); // high byte
         if (data[i] == low && data[i + 1] == high)
         {
            return true;
         }
      }
      return false;
   }

   public SimpleString concat(final SimpleString toAdd)
   {
      byte[] bytes = new byte[data.length + toAdd.getData().length];
      System.arraycopy(data, 0, bytes, 0, data.length);
      System.arraycopy(toAdd.getData(), 0, bytes, data.length, toAdd.getData().length);
      return new SimpleString(bytes);
   }

   public SimpleString concat(final char c)
   {
      byte[] bytes = new byte[data.length + 2];
      System.arraycopy(data, 0, bytes, 0, data.length);
      bytes[data.length] = (byte)(c & 0xFF);
      bytes[data.length + 1] = (byte)(c >> 8 & 0xFF);
      return new SimpleString(bytes);
   }

   public static int sizeofString(final SimpleString str)
   {
      return SIZE_INT + str.data.length;
   }

   public static int sizeofNullableString(final SimpleString str)
   {
      if (str == null)
      {
         return 1;
      }
      else
      {
         return 1 + sizeofString(str);
      }
   }

   public void getChars(int srcBegin, int srcEnd, char dst[], int dstBegin)
   {
      if (srcBegin < 0)
      {
         throw new StringIndexOutOfBoundsException(srcBegin);
      }
      if (srcEnd > length())
      {
         throw new StringIndexOutOfBoundsException(srcEnd);
      }
      if (srcBegin > srcEnd)
      {
         throw new StringIndexOutOfBoundsException(srcEnd - srcBegin);
      }

      int j = 0;

      for (int i = srcBegin; i < srcEnd - srcBegin; i++)
      {
         int low = data[j++] & 0xFF;

         int high = (data[j++] << 8) & 0xFF00;

         dst[i] = (char)(low | high);
      }
   }

}