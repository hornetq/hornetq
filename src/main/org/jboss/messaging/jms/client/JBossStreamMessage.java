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

package org.jboss.messaging.jms.client;

import java.nio.BufferUnderflowException;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.mina.IoBufferWrapper;
import org.jboss.messaging.util.DataConstants;

/**
 * This class implements javax.jms.StreamMessage.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Some parts based on JBM 1.x class by:
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * 
 * @version $Revision: 3412 $
 *
 * $Id: JBossStreamMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class JBossStreamMessage extends JBossMessage implements StreamMessage
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossStreamMessage.class);
   
   
   public static final byte TYPE = 6;

   // Attributes ----------------------------------------------------
 
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossStreamMessage()
   {   
      super(JBossStreamMessage.TYPE);
   }

   public JBossStreamMessage(final ClientSession session)
   {   
      super(JBossStreamMessage.TYPE, session);
   }
   
   public JBossStreamMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }
   
   public JBossStreamMessage(final StreamMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, JBossStreamMessage.TYPE, session);
      
      foreign.reset();
      
      try
      {
         while (true)
         {
            Object obj = foreign.readObject();
            this.writeObject(obj);
         }
      }
      catch (MessageEOFException e)
      {
         //Ignore
      }
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossStreamMessage.TYPE;
   }
   
   // StreamMessage implementation ----------------------------------

   public boolean readBoolean() throws JMSException
   {
      checkRead();
      try
      {
         byte type = body.getByte();
         
         switch (type)
         {
            case DataConstants.BOOLEAN:
               return body.getBoolean();
            case DataConstants.STRING:
               String s = body.getNullableString();
               return Boolean.valueOf(s);
            default:
               throw new MessageFormatException("Invalid conversion");           
         }
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   public byte readByte() throws JMSException
   {
      checkRead();
      try
      {
         byte type = body.getByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return body.getByte();
            case DataConstants.STRING:
               String s = body.getNullableString();
               return Byte.parseByte(s);
            default:
               throw new MessageFormatException("Invalid conversion");           
         }
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   public short readShort() throws JMSException
   {
      checkRead();
      try
      {
         byte type = body.getByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return body.getByte();
            case DataConstants.SHORT:
               return body.getShort();
            case DataConstants.STRING:
               String s = body.getNullableString();
               return Short.parseShort(s);
            default:
               throw new MessageFormatException("Invalid conversion");           
         }
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   public char readChar() throws JMSException
   {
      checkRead();
      try
      {
         byte type = body.getByte();
         switch (type)
         {
            case DataConstants.CHAR:
               return body.getChar();
            default:
               throw new MessageFormatException("Invalid conversion");           
         }
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readInt() throws JMSException
   {
      checkRead();
      try
      {
         byte type = body.getByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return body.getByte();
            case DataConstants.SHORT:
               return body.getShort();
            case DataConstants.INT:
               return body.getInt();
            case DataConstants.STRING:
               String s = body.getNullableString();
               return Integer.parseInt(s);
            default:
               throw new MessageFormatException("Invalid conversion");           
         }
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   public long readLong() throws JMSException
   {
      checkRead();
      try
      {
         byte type = body.getByte();
         switch (type)
         {
            case DataConstants.BYTE:
               return body.getByte();
            case DataConstants.SHORT:
               return body.getShort();
            case DataConstants.INT:
               return body.getInt();
            case DataConstants.LONG:
               return body.getLong();
            case DataConstants.STRING:
               String s = body.getNullableString();
               return Long.parseLong(s);
            default:
               throw new MessageFormatException("Invalid conversion");           
         }
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   public float readFloat() throws JMSException
   {
      checkRead();
      try
      {
         byte type = body.getByte();
         switch (type)
         {
            case DataConstants.FLOAT:
               return body.getFloat();
            case DataConstants.STRING:
               String s = body.getNullableString();
               return Float.parseFloat(s);
            default:
               throw new MessageFormatException("Invalid conversion");           
         }
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   public double readDouble() throws JMSException
   {
      checkRead();
      try
      {
         byte type = body.getByte();
         switch (type)
         {
            case DataConstants.FLOAT:
               return body.getFloat();
            case DataConstants.DOUBLE:
               return body.getDouble();
            case DataConstants.STRING:
               String s = body.getNullableString();
               return Double.parseDouble(s);
            default:
               throw new MessageFormatException("Invalid conversion: " + type);           
         }
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }
   
   public String readString() throws JMSException
   {
      checkRead();
      try
      {
         byte type = body.getByte();
         switch (type)
         {
            case DataConstants.BOOLEAN:
               return String.valueOf(body.getBoolean());
            case DataConstants.BYTE:
               return String.valueOf(body.getByte());
            case DataConstants.SHORT:
               return String.valueOf(body.getShort());
            case DataConstants.CHAR:
               return String.valueOf(body.getChar());
            case DataConstants.INT:
               return String.valueOf(body.getInt());
            case DataConstants.LONG:
               return String.valueOf(body.getLong());
            case DataConstants.FLOAT:
               return String.valueOf(body.getFloat());
            case DataConstants.DOUBLE:
               return String.valueOf(body.getDouble());
            case DataConstants.STRING:
               return body.getNullableString();
            default:
               throw new MessageFormatException("Invalid conversion");           
         }
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   private int len;
   
   public int readBytes(final byte[] value) throws JMSException
   {
      checkRead();
      try
      {
         if (len == -1)
         {
            len = 0;
            return -1;
         }
         else if (len == 0)
         {
            byte type = body.getByte();
            if (type != DataConstants.BYTES)
            {
               throw new MessageFormatException("Invalid conversion"); 
            }
            len = body.getInt();       
         }     
         int read = Math.min(value.length, len);
         body.getBytes(value, 0, read);
         len -= read;
         if (len == 0)
         {
            len = -1;
         }
         return read;      
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }
   
   public Object readObject() throws JMSException
   {
      checkRead();
      byte type = body.getByte();
      switch (type)
      {
         case DataConstants.BOOLEAN:
            return body.getBoolean();
         case DataConstants.BYTE:
            return body.getByte();
         case DataConstants.SHORT:
            return body.getShort();
         case DataConstants.CHAR:
            return body.getChar();
         case DataConstants.INT:
            return body.getInt();
         case DataConstants.LONG:
            return body.getLong();
         case DataConstants.FLOAT:
            return body.getFloat();
         case DataConstants.DOUBLE:
            return body.getDouble();
         case DataConstants.STRING:
            return body.getNullableString();         
         case DataConstants.BYTES:
            int len = body.getInt();
            byte[] bytes = new byte[len];
            body.getBytes(bytes);
            return bytes;
         default:
            throw new MessageFormatException("Invalid conversion");           
      }
   }

   public void writeBoolean(final boolean value) throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.BOOLEAN);
      body.putBoolean(value);
   }

   public void writeByte(final byte value) throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.BYTE);
      body.putByte(value);
   }

   public void writeShort(final short value) throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.SHORT);
      body.putShort(value);
   }

   public void writeChar(final char value) throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.CHAR);
      body.putChar(value);
   }

   public void writeInt(final int value) throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.INT);
      body.putInt(value);
   }

   public void writeLong(final long value) throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.LONG);
      body.putLong(value);
   }

   public void writeFloat(final float value) throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.FLOAT);
      body.putFloat(value);
   }

   public void writeDouble(final double value) throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.DOUBLE);
      body.putDouble(value);
   }
   
   public void writeString(final String value) throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.STRING);
      body.putNullableString(value);
   }

   public void writeBytes(final byte[] value) throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.BYTES);
      body.putInt(value.length);
      body.putBytes(value);
   }

   public void writeBytes(final byte[] value, final int offset, final int length)
         throws JMSException
   {
      checkWrite();
      body.putByte(DataConstants.BYTES);
      body.putInt(length);
      body.putBytes(value, offset, length);
   }

   public void writeObject(final Object value) throws JMSException
   {
      if (value == null) 
      {
         throw new NullPointerException("Attempt to write a null value");
      }
      if (value instanceof String)
      {
         writeString((String)value);
      }
      else if (value instanceof Boolean)
      {
         writeBoolean((Boolean)value);
      }
      else if (value instanceof Byte)
      {
         writeByte((Byte)value);
      }
      else if (value instanceof Short)
      {
         writeShort((Short)value);
      }
      else if (value instanceof Integer)
      {
         writeInt((Integer)value);
      }
      else if (value instanceof Long)
      {
         writeLong((Long)value);
      }
      else if (value instanceof Float)
      {
         writeFloat((Float)value);
      }
      else if (value instanceof Double)
      {
         writeDouble((Double)value);
      }
      else if (value instanceof byte[])
      {
         writeBytes((byte[])value);
      }
      else if (value instanceof Character)
      {
         this.writeChar((Character)value);
      }
      else
      {
         throw new MessageFormatException("Invalid object type: " + value.getClass());
      }
   }

   public void reset() throws JMSException
   {
      if (!readOnly)
      {
         readOnly = true;
         
         body.flip();
      }
      else
      {
         body.rewind();
      }
   }

   // JBossMessage overrides ----------------------------------------
  
   public void clearBody() throws JMSException
   {
      super.clearBody();
      
      body = new IoBufferWrapper(1024);
   }
   
   public void doBeforeSend() throws Exception
   {
      reset();
      
      message.setBody(body);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
