/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.message;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

/**
 * 
 * Thin delegator for a JBossBytesMessage
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * 
 */
public class BytesMessageDelegate extends MessageDelegate implements BytesMessage
{
   public BytesMessageDelegate(JBossBytesMessage message)
   {
      super(message);
   }

   public long getBodyLength() throws JMSException
   {
      return ((BytesMessage)message).getBodyLength();
   }

   public boolean readBoolean() throws JMSException
   {
      return ((BytesMessage)message).readBoolean();
   }

   public byte readByte() throws JMSException
   {
      return ((BytesMessage)message).readByte();
   }

   public int readUnsignedByte() throws JMSException
   {
      return ((BytesMessage)message).readUnsignedByte();
   }

   public short readShort() throws JMSException
   {
      return ((BytesMessage)message).readShort();
   }

   public int readUnsignedShort() throws JMSException
   {
      return ((BytesMessage)message).readUnsignedShort();
   }

   public char readChar() throws JMSException
   {
      return ((BytesMessage)message).readChar();
   }

   public int readInt() throws JMSException
   {
      return ((BytesMessage)message).readInt();
   }

   public long readLong() throws JMSException
   {
      return ((BytesMessage)message).readLong();
   }

   public float readFloat() throws JMSException
   {
      return ((BytesMessage)message).readFloat();
   }

   public double readDouble() throws JMSException
   {
      return ((BytesMessage)message).readDouble();
   }

   public String readUTF() throws JMSException
   {
      return ((BytesMessage)message).readUTF();
   }

   public int readBytes(byte[] value) throws JMSException
   {
      return ((BytesMessage)message).readBytes(value);
   }

   public int readBytes(byte[] value, int length) throws JMSException
   {
      return ((BytesMessage)message).readBytes(value, length);
   }

   public void writeBoolean(boolean value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeBoolean(value);
   }

   public void writeByte(byte value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeByte(value);
   }

   public void writeShort(short value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeShort(value);
   }

   public void writeChar(char value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeChar(value);
   }

   public void writeInt(int value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeInt(value);
   }

   public void writeLong(long value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeLong(value);
   }

   public void writeFloat(float value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeFloat(value);
   }

   public void writeDouble(double value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeDouble(value);
   }

   public void writeUTF(String value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeUTF(value);
   }

   public void writeBytes(byte[] value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeBytes(value);
   }

   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeBytes(value, offset, length);
   }

   public void writeObject(Object value) throws JMSException
   {
      bodyChange();
      ((BytesMessage)message).writeObject(value);
   }

   public void reset() throws JMSException
   {
      ((BytesMessage)message).reset();
   }

}
