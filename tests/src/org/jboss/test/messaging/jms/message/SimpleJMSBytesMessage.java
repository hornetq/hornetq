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
package org.jboss.test.messaging.jms.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class SimpleJMSBytesMessage extends SimpleJMSMessage implements BytesMessage
{
   // Static -------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected byte[] internalArray;
   protected boolean bodyWriteOnly = true;
   private transient ByteArrayOutputStream ostream;
   private transient DataOutputStream p;
   private transient ByteArrayInputStream istream;
   private transient DataInputStream m;

   // Constructor ---------------------------------------------------

   public SimpleJMSBytesMessage()
   {
      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
   }

   // BytesMessage implementation -----------------------------------

   public boolean readBoolean() throws JMSException
   {
      checkRead();
      try
      {
         return m.readBoolean();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public byte readByte() throws JMSException
   {
      checkRead();
      try
      {
         return m.readByte();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public int readUnsignedByte() throws JMSException
   {
      checkRead();
      try
      {
         return m.readUnsignedByte();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public short readShort() throws JMSException
   {
      checkRead();
      try
      {
         return m.readShort();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public int readUnsignedShort() throws JMSException
   {
      checkRead();
      try
      {
         return m.readUnsignedShort();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public char readChar() throws JMSException
   {
      checkRead();
      try
      {
         return m.readChar();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public int readInt() throws JMSException
   {
      checkRead();
      try
      {
         return m.readInt();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public long readLong() throws JMSException
   {
      checkRead();
      try
      {
         return m.readLong();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public float readFloat() throws JMSException
   {
      checkRead();
      try
      {
         return m.readFloat();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public double readDouble() throws JMSException
   {
      checkRead();
      try
      {
         return m.readDouble();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public String readUTF() throws JMSException
   {
      checkRead();
      try
      {
         return m.readUTF();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public int readBytes(byte[] value) throws JMSException
   {
      checkRead();
      try
      {
         return m.read(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public int readBytes(byte[] value, int length) throws JMSException
   {
      checkRead();
      try
      {
         return m.read(value, 0, length);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeBoolean(boolean value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeBoolean(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeByte(byte value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeByte(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeShort(short value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeShort(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeChar(char value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeChar(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeInt(int value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeInt(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeLong(long value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeLong(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeFloat(float value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeFloat(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeDouble(double value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeDouble(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeUTF(String value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeUTF(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeBytes(byte[] value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.write(value, 0, value.length);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.write(value, offset, length);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeObject(Object value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         if (value == null)
         {
            throw new NullPointerException("Attempt to write a new value");
         }
         if (value instanceof String)
         {
            p.writeUTF((String) value);
         }
         else if (value instanceof Boolean)
         {
            p.writeBoolean(((Boolean) value).booleanValue());
         }
         else if (value instanceof Byte)
         {
            p.writeByte(((Byte) value).byteValue());
         }
         else if (value instanceof Short)
         {
            p.writeShort(((Short) value).shortValue());
         }
         else if (value instanceof Integer)
         {
            p.writeInt(((Integer) value).intValue());
         }
         else if (value instanceof Long)
         {
            p.writeLong(((Long) value).longValue());
         }
         else if (value instanceof Float)
         {
            p.writeFloat(((Float) value).floatValue());
         }
         else if (value instanceof Double)
         {
            p.writeDouble(((Double) value).doubleValue());
         }
         else if (value instanceof byte[])
         {
            p.write((byte[]) value, 0, ((byte[]) value).length);
         }
         else
         {
            throw new MessageFormatException("Invalid object for properties");
         }
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }

   }

   public void reset() throws JMSException
   {
      try
      {
         if (bodyWriteOnly)
         {
            p.flush();
            internalArray = ostream.toByteArray();
            ostream.close();
         }
         ostream = null;
         istream = null;
         m = null;
         p = null;
         bodyWriteOnly = false;
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }


   public long getBodyLength() throws JMSException
   {
      checkRead();
      return internalArray.length;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * Check the message is readable
    *
    * @throws javax.jms.JMSException when not readable
    */
   private void checkRead() throws JMSException
   {
      if (bodyWriteOnly)
      {
         throw new MessageNotReadableException("readByte while the buffer is writeonly");
      }

      // We have just received/reset() the message, and the client is trying to
      // read it
      if (istream == null || m == null)
      {
         istream = new ByteArrayInputStream(internalArray);
         m = new DataInputStream(istream);
      }
   }

   // Inner classes -------------------------------------------------
}
