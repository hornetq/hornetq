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


using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace JBoss.JBM.Client.util
{
    /**
     * 
     * A SimpleString
     * 
     * A simple String class that can store all characters, and stores as simple byte[],
     * this minimises expensive copying between String objects
     * 
     * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
     * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
     *
     */
    public class SimpleString : IComparable
    {
        // Attributes
        // ------------------------------------------------------------------------
        private byte[] data;

        private int hash;

        // Cache the string
        private string str;

        // Static
        // ----------------------------------------------------------------------

        /**
         * Returns a SimpleString constructed from the <code>string</code> parameter.
         * If <code>string</code> is <code>null</code>, the return value will be <code>null</code> too.
         */
        public static SimpleString ToSimpleString(string value)
        {
            if (value == null)
            {
                return null;
            }
            return new SimpleString(value);
        }

        // Constructors
        // ----------------------------------------------------------------------

        public SimpleString(string value)
        {

            int j = 0;

            char[] characters = value.ToCharArray();

            int len = characters.Length;

            data = new byte[len << 1];

            for (int i = 0; i < len; i++)
            {
                char c = characters[i];

                byte low = (byte)(c & 0xFF); // low byte

                data[j++] = low;

                byte high = (byte)(c >> 8 & 0xFF); // high byte

                data[j++] = high;
            }

            str = value;
        }

        public SimpleString(byte[] data)
        {
            this.data = data;
        }

        // CharSequence implementation
        // ---------------------------------------------------------------------------

        public int Length
        {
            get
            {
                return data.Length >> 1;
            }
        }

        public char CharAt(int pos)
        {
            if (pos < 0 || pos >= data.Length >> 1)
            {
                throw new IndexOutOfRangeException();
            }
            pos <<= 1;

            return (char)(data[pos] | data[pos + 1] << 8);
        }

        public SimpleString SubSequence(int start, int end)
        {
            int len = data.Length >> 1;

            if (end < start || start < 0 || end > len)
            {
                throw new IndexOutOfRangeException();
            }
            else
            {
                int newlen = (end - start) << 1;
                byte[] bytes = new byte[newlen];

                Array.Copy(data, start << 1, bytes, 0, newlen);

                return new SimpleString(bytes);
            }
        }

        // Comparable implementation -------------------------------------

        public int CompareTo(Object o)
        {
            return this.ToString().CompareTo(o.ToString());
        }

        // Public
        // ---------------------------------------------------------------------------

		public byte[] Data
		{
			get
			{
				return this.data;
			}
			private set
			{
				this.data = value;
			}
		}

        public bool StartsWith(SimpleString other)
        {
            byte[] otherdata = other.data;

            if (otherdata.Length > this.data.Length)
            {
                return false;
            }

            for (int i = 0; i < otherdata.Length; i++)
            {
                if (this.data[i] != otherdata[i])
                {
                    return false;
                }
            }

            return true;
        }


        public override string ToString()
        {
            if (str == null)
            {
                int len = data.Length >> 1;

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

        public override bool Equals(Object other)
        {
            if (other is SimpleString)
            {
                SimpleString s = (SimpleString)other;

                if (data.Length != s.data.Length)
                {
                    return false;
                }

                for (int i = 0; i < data.Length; i++)
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

        public override int GetHashCode()
        {
            if (hash == 0)
            {
                for (int i = 0; i < data.Length; i++)
                {
                    hash = (31 * hash + data[i]) % (int.MaxValue>>5);
                }
            }

            return hash;
        }

        public SimpleString[] Split(char delim)
        {
            if (!Contains(delim))
            {
                return new SimpleString[] { this };
            }
            else
            {
                IList<SimpleString> all = new List<SimpleString>();
                int lasPos = 0;
                for (int i = 0; i < data.Length; i += 2)
                {
                    byte low = (byte)(delim & 0xFF); // low byte
                    byte high = (byte)(delim >> 8 & 0xFF); // high byte
                    if (data[i] == low && data[i + 1] == high)
                    {
                        byte[] bytes = new byte[i - lasPos];
                        Array.Copy(data, lasPos, bytes, 0, bytes.Length);
                        lasPos = i + 2;
                        all.Add(new SimpleString(bytes));
                    }
                }
                byte[] bytes2 = new byte[data.Length - lasPos];
                Array.Copy(data, lasPos, bytes2, 0, bytes2.Length);
                all.Add(new SimpleString(bytes2));
                return all.ToArray();
            }
        }

        public bool Contains(char c)
        {
            for (int i = 0; i < data.Length; i += 2)
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

        public SimpleString Concat(SimpleString toAdd)
        {

            byte[] bytes = new byte[data.Length + toAdd.Data.Length];
            Array.Copy(data, 0, bytes, 0, data.Length);
            Array.Copy(toAdd.Data, 0, bytes, data.Length, toAdd.Data.Length);
            return new SimpleString(bytes);
        }

        public SimpleString Concat(char c)
        {
            byte[] bytes = new byte[data.Length + 2];
            Array.Copy(data, 0, bytes, 0, data.Length);
            bytes[data.Length] = (byte)(c & 0xFF);
            bytes[data.Length + 1] = (byte)(c >> 8 & 0xFF);
            return new SimpleString(bytes);
        }

        public static int SizeofString(SimpleString str)
        {

            return DataConstants.SIZE_INT + str.data.Length;
        }

        public static int SizeofNullableString(SimpleString str)
        {
            if (str == null)
            {
                return 1;
            }
            else
            {
                return 1 + SizeofString(str);
            }
        }

        public void GetChars(int srcBegin, int srcEnd, char[] dst, int dstBegin)
        {
            if (srcBegin < 0)
            {
                throw new IndexOutOfRangeException("Invalid initialIndex: " + srcBegin);
            }
            if (srcEnd > this.Length)
            {
                throw new IndexOutOfRangeException("Invalid endIndex: " + srcEnd);
            }
            if (srcBegin > srcEnd)
            {
                throw new IndexOutOfRangeException("Invalid Length: " + (srcEnd - srcBegin));
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
}
