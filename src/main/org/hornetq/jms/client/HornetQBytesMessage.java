/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.hornetq.jms.client;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.HornetQBuffer;

/**
 * This class implements javax.jms.BytesMessage.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * 
 * @version $Revision: 3412 $
 * 
 * $Id: HornetQRABytesMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class HornetQBytesMessage extends HornetQMessage implements BytesMessage
{
   // Static -------------------------------------------------------

   private static final Logger log = Logger.getLogger(HornetQBytesMessage.class);

   public static final byte TYPE = 4;

   // Attributes ----------------------------------------------------

   // Constructor ---------------------------------------------------
   public HornetQBytesMessage()
   {
      super(HornetQBytesMessage.TYPE);
   }

   /*
    * This constructor is used to construct messages prior to sending
    */
   public HornetQBytesMessage(final ClientSession session)
   {
      super(HornetQBytesMessage.TYPE, session);
   }

   /*
    * Constructor on receipt at client side
    */
   public HornetQBytesMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }

   /*
    * Foreign message constructor
    */
   public HornetQBytesMessage(final BytesMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, HornetQBytesMessage.TYPE, session);

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
         return getBody().readBoolean();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public byte readByte() throws JMSException
   {
      checkRead();
      try
      {
         return getBody().readByte();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readUnsignedByte() throws JMSException
   {
      checkRead();
      try
      {
         return getBody().readUnsignedByte();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public short readShort() throws JMSException
   {
      checkRead();
      try
      {
         return getBody().readShort();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readUnsignedShort() throws JMSException
   {
      checkRead();
      try
      {
         return getBody().readUnsignedShort();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public char readChar() throws JMSException
   {
      checkRead();
      try
      {
         return getBody().readChar();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readInt() throws JMSException
   {
      checkRead();
      try
      {
         return getBody().readInt();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public long readLong() throws JMSException
   {
      checkRead();
      try
      {
         return getBody().readLong();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public float readFloat() throws JMSException
   {
      checkRead();
      try
      {
         return getBody().readFloat();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public double readDouble() throws JMSException
   {
      checkRead();
      try
      {
         return getBody().readDouble();
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public String readUTF() throws JMSException
   {
      checkRead();
      try
      {
         return getBody().readUTF();
      }
      catch (IndexOutOfBoundsException e)
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

      if (!getBody().readable()) { return -1; }

      int read = Math.min(length, getBody().readableBytes());

      if (read != 0)
      {
         getBody().readBytes(value, 0, read);
      }

      return read;
   }

   public void writeBoolean(final boolean value) throws JMSException
   {
      checkWrite();
      getBody().writeBoolean(value);
   }

   public void writeByte(final byte value) throws JMSException
   {
      checkWrite();
      getBody().writeByte(value);
   }

   public void writeShort(final short value) throws JMSException
   {
      checkWrite();
      getBody().writeShort(value);
   }

   public void writeChar(final char value) throws JMSException
   {
      checkWrite();
      getBody().writeChar(value);
   }

   public void writeInt(final int value) throws JMSException
   {
      checkWrite();
      getBody().writeInt(value);
   }

   public void writeLong(final long value) throws JMSException
   {
      checkWrite();
      getBody().writeLong(value);
   }

   public void writeFloat(final float value) throws JMSException
   {
      checkWrite();
      getBody().writeFloat(value);
   }

   public void writeDouble(final double value) throws JMSException
   {
      checkWrite();
      getBody().writeDouble(value);
   }

   public void writeUTF(final String value) throws JMSException
   {
      checkWrite();
      try
      {
         getBody().writeUTF(value);
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
      getBody().writeBytes(value);
   }

   public void writeBytes(final byte[] value, final int offset, final int length)
         throws JMSException
   {
      checkWrite();
      getBody().writeBytes(value, offset, length);
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
      else if (value instanceof Character)
      {
         writeChar((Character) value);
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

         getBody().resetReaderIndex();
      }
      else
      {
         getBody().resetReaderIndex();
      }
   }

   // HornetQRAMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      super.clearBody();
      HornetQBuffer currentBody = message.getBody();
      currentBody.clear();
   }

   public long getBodyLength() throws JMSException
   {
      checkRead();
      
      return message.getLargeBodySize();
   }

   public void doBeforeSend() throws Exception
   {
      reset();
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return HornetQBytesMessage.TYPE;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
