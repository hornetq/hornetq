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

import javax.jms.JMSException;
import javax.jms.StreamMessage;

/**
 * 
 * Thin delegator for a JBossStreamMessage
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * 
 */
public class StreamMessageDelegate extends MessageDelegate implements StreamMessage
{
   public StreamMessageDelegate(JBossStreamMessage message)
   {
      super(message);
   }

   public boolean readBoolean() throws JMSException
   {
      return ((StreamMessage)message).readBoolean();
   }

   public byte readByte() throws JMSException
   {
      return ((StreamMessage)message).readByte();
   }
   
   public int readBytes(byte[] value) throws JMSException
   {
      return ((StreamMessage)message).readBytes(value);
   }
   
   public char readChar() throws JMSException
   {
      return ((StreamMessage)message).readChar();
   }

   public double readDouble() throws JMSException
   {
      return ((StreamMessage)message).readDouble();
   }

   public float readFloat() throws JMSException
   {
      return ((StreamMessage)message).readFloat();
   }
   
   public int readInt() throws JMSException
   {
      return ((StreamMessage)message).readInt();
   }

   public long readLong() throws JMSException
   {
      return ((StreamMessage)message).readLong();
   }
   
   public Object readObject() throws JMSException
   {
      return ((StreamMessage)message).readObject();
   }
   
   public short readShort() throws JMSException
   {
      return ((StreamMessage)message).readShort();
   }
   
   public String readString() throws JMSException
   {
      return ((StreamMessage)message).readString();
   }

   public void reset() throws JMSException
   {
      ((StreamMessage)message).reset();
   }

   public void writeBoolean(boolean value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeBoolean(value);
   }

   public void writeByte(byte value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeByte(value);
   }
   
   public void writeBytes(byte[] value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeBytes(value);
   }
   
   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeBytes(value, offset, length);
   }
   
   public void writeChar(char value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeChar(value);
   }
   
   public void writeDouble(double value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeDouble(value);
   }
   
   public void writeFloat(float value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeFloat(value);
   }
   
   public void writeInt(int value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeInt(value);
   }
   
   public void writeLong(long value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeLong(value);
   }
   
   public void writeObject(Object value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeObject(value);
   }
   
   public void writeShort(short value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeShort(value);
   }
   
   public void writeString(String value) throws JMSException
   {
      bodyChange();
      ((StreamMessage)message).writeString(value);
   }

}

