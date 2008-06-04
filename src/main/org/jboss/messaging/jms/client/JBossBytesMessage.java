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
package org.jboss.messaging.jms.client;

import java.nio.BufferUnderflowException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.mina.IoBufferWrapper;

/**
 * This class implements javax.jms.BytesMessage.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * 
 * @version $Revision: 3412 $
 * 
 * $Id: JBossBytesMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class JBossBytesMessage extends JBossMessage implements BytesMessage
{
   // Static -------------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossBytesMessage.class);

   public static final byte TYPE = 4;

   // Attributes ----------------------------------------------------

   // Constructor ---------------------------------------------------

   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossBytesMessage()
   {
      super(JBossBytesMessage.TYPE);
   }

   /*
    * Constructor on receipt at client side
    */
   public JBossBytesMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }

   /*
    * Foreign message constructor
    */
   public JBossBytesMessage(final BytesMessage foreign) throws JMSException
   {
      super(foreign, JBossBytesMessage.TYPE);

      foreign.reset();

      byte[] buffer = new byte[1024];
      int n = foreign.readBytes(buffer);
      while (n != -1)
      {
         writeBytes(buffer, 0, n);
         n = foreign.readBytes(buffer);
      }
   }

   // BytesMessage implementation -----------------------------------

   public boolean readBoolean() throws JMSException
   {
      checkRead();
      try
      {
         return body.getBoolean();
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
         return body.getByte();
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readUnsignedByte() throws JMSException
   {
      checkRead();
      try
      {
         return body.getUnsignedByte();
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
         return body.getShort();
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readUnsignedShort() throws JMSException
   {
      checkRead();
      try
      {
         return body.getUnsignedShort();
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
         return body.getChar();
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
         return body.getInt();
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
         return body.getLong();
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
         return body.getFloat();
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
         return body.getDouble();
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
   }

   public String readUTF() throws JMSException
   {
      checkRead();
      try
      {
         return body.getUTF();
      }
      catch (BufferUnderflowException e)
      {
         throw new MessageEOFException("");
      }
      catch (Exception e)
      {
         JMSException je = new JMSException("Failed to get UTF");
         je.setLinkedException(e);
         throw je;
      }
   }

   public int readBytes(final byte[] value) throws JMSException
   {
      return readBytes(value, value.length);
   }

   public int readBytes(final byte[] value, final int length)
         throws JMSException
   {
      checkRead();

      if (body.remaining() == 0) { return -1; }

      int read = Math.min(length, body.remaining());

      if (read != 0)
      {
         body.getBytes(value, 0, read);
      }

      return read;
   }

   public void writeBoolean(final boolean value) throws JMSException
   {
      checkWrite();
      body.putBoolean(value);
   }

   public void writeByte(final byte value) throws JMSException
   {
      checkWrite();
      body.putByte(value);
   }

   public void writeShort(final short value) throws JMSException
   {
      checkWrite();
      body.putShort(value);
   }

   public void writeChar(final char value) throws JMSException
   {
      checkWrite();
      body.putChar(value);
   }

   public void writeInt(final int value) throws JMSException
   {
      checkWrite();
      body.putInt(value);
   }

   public void writeLong(final long value) throws JMSException
   {
      checkWrite();
      body.putLong(value);
   }

   public void writeFloat(final float value) throws JMSException
   {
      checkWrite();
      body.putFloat(value);
   }

   public void writeDouble(final double value) throws JMSException
   {
      checkWrite();
      body.putDouble(value);
   }

   public void writeUTF(final String value) throws JMSException
   {
      checkWrite();
      try
      {
         body.putUTF(value);
      }
      catch (Exception e)
      {
         JMSException je = new JMSException("Failed to write UTF");
         je.setLinkedException(e);
         throw je;
      }
   }

   public void writeBytes(final byte[] value) throws JMSException
   {
      checkWrite();
      body.putBytes(value);
   }

   public void writeBytes(final byte[] value, final int offset, final int length)
         throws JMSException
   {
      checkWrite();
      body.putBytes(value, offset, length);
   }

   public void writeObject(final Object value) throws JMSException
   {
      if (value == null) { throw new NullPointerException(
            "Attempt to write a null value"); }
      if (value instanceof String)
      {
         writeUTF((String) value);
      }
      else if (value instanceof Boolean)
      {
         writeBoolean((Boolean) value);
      }
      else if (value instanceof Byte)
      {
         writeByte((Byte) value);
      }
      else if (value instanceof Short)
      {
         writeShort((Short) value);
      }
      else if (value instanceof Integer)
      {
         writeInt((Integer) value);
      }
      else if (value instanceof Long)
      {
         writeLong((Long) value);
      }
      else if (value instanceof Float)
      {
         writeFloat((Float) value);
      }
      else if (value instanceof Double)
      {
         writeDouble((Double) value);
      }
      else if (value instanceof byte[])
      {
         writeBytes((byte[]) value);
      }
      else
      {
         throw new MessageFormatException("Invalid object for properties");
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

   public long getBodyLength() throws JMSException
   {
      checkRead();

      return body.limit();
   }

   public void doBeforeSend() throws Exception
   {
      reset();

      message.setBody(body);
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossBytesMessage.TYPE;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
