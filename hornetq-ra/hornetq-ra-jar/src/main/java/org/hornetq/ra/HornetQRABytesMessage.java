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

package org.hornetq.ra;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import org.hornetq.core.logging.Logger;

/**
 * A wrapper for a message
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class HornetQRABytesMessage extends HornetQRAMessage implements BytesMessage
{
   /** The logger */
   private static final Logger log = Logger.getLogger(HornetQRABytesMessage.class);

   /** Whether trace is enabled */
   private static boolean trace = HornetQRABytesMessage.log.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param message the message
    * @param session the session
    */
   public HornetQRABytesMessage(final BytesMessage message, final HornetQRASession session)
   {
      super(message, session);

      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("constructor(" + message + ", " + session + ")");
      }
   }

   /**
    * Get body length
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public long getBodyLength() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("getBodyLength()");
      }

      return ((BytesMessage)message).getBodyLength();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public boolean readBoolean() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readBoolean()");
      }

      return ((BytesMessage)message).readBoolean();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public byte readByte() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readByte()");
      }

      return ((BytesMessage)message).readByte();
   }

   /**
    * Read
    * @param value The value
    * @param length The length
    * @return The result
    * @exception JMSException Thrown if an error occurs
    */
   public int readBytes(final byte[] value, final int length) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readBytes(" + value + ", " + length + ")");
      }

      return ((BytesMessage)message).readBytes(value, length);
   }

   /**
    * Read
    * @param value The value
    * @return The result
    * @exception JMSException Thrown if an error occurs
    */
   public int readBytes(final byte[] value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readBytes(" + value + ")");
      }

      return ((BytesMessage)message).readBytes(value);
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public char readChar() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readChar()");
      }

      return ((BytesMessage)message).readChar();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public double readDouble() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readDouble()");
      }

      return ((BytesMessage)message).readDouble();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public float readFloat() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readFloat()");
      }

      return ((BytesMessage)message).readFloat();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int readInt() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readInt()");
      }

      return ((BytesMessage)message).readInt();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public long readLong() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readLong()");
      }

      return ((BytesMessage)message).readLong();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public short readShort() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readShort()");
      }

      return ((BytesMessage)message).readShort();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int readUnsignedByte() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readUnsignedByte()");
      }

      return ((BytesMessage)message).readUnsignedByte();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int readUnsignedShort() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readUnsignedShort()");
      }

      return ((BytesMessage)message).readUnsignedShort();
   }

   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public String readUTF() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("readUTF()");
      }

      return ((BytesMessage)message).readUTF();
   }

   /**
    * Reset
    * @exception JMSException Thrown if an error occurs
    */
   public void reset() throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("reset()");
      }

      ((BytesMessage)message).reset();
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeBoolean(final boolean value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeBoolean(" + value + ")");
      }

      ((BytesMessage)message).writeBoolean(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeByte(final byte value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeByte(" + value + ")");
      }

      ((BytesMessage)message).writeByte(value);
   }

   /**
    * Write
    * @param value The value 
    * @param offset The offset
    * @param length The length
    * @exception JMSException Thrown if an error occurs
    */
   public void writeBytes(final byte[] value, final int offset, final int length) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeBytes(" + value + ", " + offset + ", " + length + ")");
      }

      ((BytesMessage)message).writeBytes(value, offset, length);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeBytes(final byte[] value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeBytes(" + value + ")");
      }

      ((BytesMessage)message).writeBytes(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeChar(final char value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeChar(" + value + ")");
      }

      ((BytesMessage)message).writeChar(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeDouble(final double value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeDouble(" + value + ")");
      }

      ((BytesMessage)message).writeDouble(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeFloat(final float value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeFloat(" + value + ")");
      }

      ((BytesMessage)message).writeFloat(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeInt(final int value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeInt(" + value + ")");
      }

      ((BytesMessage)message).writeInt(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeLong(final long value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeLong(" + value + ")");
      }

      ((BytesMessage)message).writeLong(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeObject(final Object value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeObject(" + value + ")");
      }

      ((BytesMessage)message).writeObject(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeShort(final short value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeShort(" + value + ")");
      }

      ((BytesMessage)message).writeShort(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeUTF(final String value) throws JMSException
   {
      if (HornetQRABytesMessage.trace)
      {
         HornetQRABytesMessage.log.trace("writeUTF(" + value + ")");
      }

      ((BytesMessage)message).writeUTF(value);
   }
}
