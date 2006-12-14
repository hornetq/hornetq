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
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.StreamMessage;

/**
 * 
 * Thin proxy for a JBossStreamMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * StreamMessageProxy.java,v 1.1 2006/03/08 08:00:34 timfox Exp
 */
public class StreamMessageProxy extends MessageProxy implements StreamMessage
{
   private static final long serialVersionUID = 856367553964704474L;

   public StreamMessageProxy(long deliveryId, JBossStreamMessage message, int deliveryCount)
   {
      super(deliveryId, message, deliveryCount);
   }
   
   public StreamMessageProxy(JBossStreamMessage message)
   {
      super(message);
   }

   public boolean readBoolean() throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readBoolean();
   }

   public byte readByte() throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readByte();
   }
   
   public int readBytes(byte[] value) throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readBytes(value);
   }
   
   public char readChar() throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readChar();
   }

   public double readDouble() throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readDouble();
   }

   public float readFloat() throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readFloat();
   }
   
   public int readInt() throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readInt();
   }

   public long readLong() throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readLong();
   }
   
   public Object readObject() throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readObject();
   }
   
   public short readShort() throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readShort();
   }
   
   public String readString() throws JMSException
   {
      if (!bodyReadOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      return ((StreamMessage)message).readString();
   }

   public void reset() throws JMSException
   {
      bodyReadOnly = true;
      ((StreamMessage)message).reset();
   }

   public void writeBoolean(boolean value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeBoolean(value);
   }

   public void writeByte(byte value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeByte(value);
   }
   
   public void writeBytes(byte[] value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeBytes(value);
   }
   
   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeBytes(value, offset, length);
   }
   
   public void writeChar(char value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeChar(value);
   }
   
   public void writeDouble(double value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeDouble(value);
   }
   
   public void writeFloat(float value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeFloat(value);
   }
   
   public void writeInt(int value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeInt(value);
   }
   
   public void writeLong(long value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeLong(value);
   }
   
   public void writeObject(Object value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeObject(value);
   }
   
   public void writeShort(short value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeShort(value);
   }
   
   public void writeString(String value) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      bodyChange();
      ((StreamMessage)message).writeString(value);
   }
}

