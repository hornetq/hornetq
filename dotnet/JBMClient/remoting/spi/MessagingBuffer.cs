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

namespace JBoss.JBM.Client.remoting.spi
{
    using JBoss.JBM.Client.util;

    /**
     *
     * A MessagingBuffer
     *
     * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
     *
     */
    public interface MessagingBuffer
    {
        void PutByte(byte val);

        void PutBytes(byte[] bytes);

        void PutBytes(byte[] bytes, int offset, int length);

        void PutInt(int val);

        void PutInt(int pos, int val);

        void PutLong(long val);

        void PutShort(short val);

        void PutDouble(double val);

        void PutFloat(float val);

        void PutBoolean(bool val);

        void PutChar(char val);

        void PutNullableString(string val);

        void PutString(string val);

        void PutSimpleString(SimpleString val);

        void PutNullableSimpleString(SimpleString val);

        void PutUTF(string utf);

        byte GetByte();

        short GetUnsignedByte();

        void GetBytes(byte[] bytes);

        void GetBytes(byte[] bytes, int offset, int length);

        int GetInt();

        long GetLong();

        short GetShort();

        int GetUnsignedShort();

        double GetDouble();

        float GetFloat();

        bool GetBoolean();

        char GetChar();

        string GetString();

        string GetNullableString();

        SimpleString GetSimpleString();

        SimpleString getNullableSimpleString();

        string GetUTF();

        byte[] ToArray();

        int Remaining();

        int Capacity();

        int Limit();

        void limit(int limit);

        void Flip();

        void Position(int position);

        int Position();

        void Rewind();

        MessagingBuffer Slice();

        MessagingBuffer CreateNewBuffer(int len);

        object GetUnderlyingBuffer();
    }
}