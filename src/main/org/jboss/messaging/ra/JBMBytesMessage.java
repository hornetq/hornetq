/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import org.jboss.messaging.core.logging.Logger;

/**
 * A wrapper for a message
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMBytesMessage extends JBMMessage implements BytesMessage
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMBytesMessage.class);
   
   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param message the message
    * @param session the session
    */
   public JBMBytesMessage(BytesMessage message, JBMSession session)
   {
      super(message, session);

      if (trace)
         log.trace("constructor(" + message + ", " + session + ")");
   }
   
   /**
    * Get body length
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public long getBodyLength() throws JMSException
   {
      if (trace)
         log.trace("getBodyLength()");

      return ((BytesMessage) message).getBodyLength();
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public boolean readBoolean() throws JMSException
   {
      if (trace)
         log.trace("readBoolean()");

      return ((BytesMessage) message).readBoolean();
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public byte readByte() throws JMSException
   {
      if (trace)
         log.trace("readByte()");

      return ((BytesMessage) message).readByte();
   }
   
   /**
    * Read
    * @param value The value
    * @param length The length
    * @return The result
    * @exception JMSException Thrown if an error occurs
    */
   public int readBytes(byte[] value, int length) throws JMSException
   {
      if (trace)
         log.trace("readBytes(" + value + ", " + length + ")");

      return ((BytesMessage) message).readBytes(value, length);
   }
   
   /**
    * Read
    * @param value The value
    * @return The result
    * @exception JMSException Thrown if an error occurs
    */
   public int readBytes(byte[] value) throws JMSException
   {
      if (trace)
         log.trace("readBytes(" + value + ")");

      return ((BytesMessage) message).readBytes(value);
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public char readChar() throws JMSException
   {
      if (trace)
         log.trace("readChar()");

      return ((BytesMessage) message).readChar();
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public double readDouble() throws JMSException
   {
      if (trace)
         log.trace("readDouble()");

      return ((BytesMessage) message).readDouble();
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public float readFloat() throws JMSException
   {
      if (trace)
         log.trace("readFloat()");

      return ((BytesMessage) message).readFloat();
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int readInt() throws JMSException
   {
      if (trace)
         log.trace("readInt()");

      return ((BytesMessage) message).readInt();
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public long readLong() throws JMSException
   {
      if (trace)
         log.trace("readLong()");

      return ((BytesMessage) message).readLong();
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public short readShort() throws JMSException
   {
      if (trace)
         log.trace("readShort()");

      return ((BytesMessage) message).readShort();
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int readUnsignedByte() throws JMSException
   {
      if (trace)
         log.trace("readUnsignedByte()");

      return ((BytesMessage) message).readUnsignedByte();
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int readUnsignedShort() throws JMSException
   {
      if (trace)
         log.trace("readUnsignedShort()");

      return ((BytesMessage) message).readUnsignedShort();
   }
   
   /**
    * Read
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public String readUTF() throws JMSException
   {
      if (trace)
         log.trace("readUTF()");

      return ((BytesMessage) message).readUTF();
   }
   
   /**
    * Reset
    * @exception JMSException Thrown if an error occurs
    */
   public void reset() throws JMSException
   {
      if (trace)
         log.trace("reset()");

      ((BytesMessage) message).reset();
   }
   
   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeBoolean(boolean value) throws JMSException
   {
      if (trace)
         log.trace("writeBoolean(" + value + ")");

      ((BytesMessage) message).writeBoolean(value);
   }
   
   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeByte(byte value) throws JMSException
   {
      if (trace)
         log.trace("writeByte(" + value + ")");

      ((BytesMessage) message).writeByte(value);
   }
   
   /**
    * Write
    * @param value The value 
    * @param offset The offset
    * @param length The length
    * @exception JMSException Thrown if an error occurs
    */
   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      if (trace)
         log.trace("writeBytes(" + value + ", " + offset + ", " + length + ")");

      ((BytesMessage) message).writeBytes(value, offset, length);
   }
   
   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeBytes(byte[] value) throws JMSException
   {
      if (trace)
         log.trace("writeBytes(" + value + ")");

      ((BytesMessage) message).writeBytes(value);
   }
   
   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeChar(char value) throws JMSException
   {
      if (trace)
         log.trace("writeChar(" + value + ")");

      ((BytesMessage) message).writeChar(value);
   }
   
   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeDouble(double value) throws JMSException
   {
      if (trace)
         log.trace("writeDouble(" + value + ")");

      ((BytesMessage) message).writeDouble(value);
   }
   
   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeFloat(float value) throws JMSException
   {
      if (trace)
         log.trace("writeFloat(" + value + ")");

      ((BytesMessage) message).writeFloat(value);
   }
   
   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeInt(int value) throws JMSException
   {
      if (trace)
         log.trace("writeInt(" + value + ")");

      ((BytesMessage) message).writeInt(value);
   }
   
   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeLong(long value) throws JMSException
   {
      if (trace)
         log.trace("writeLong(" + value + ")");

      ((BytesMessage) message).writeLong(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeObject(Object value) throws JMSException
   {
      if (trace)
         log.trace("writeObject(" + value + ")");

      ((BytesMessage) message).writeObject(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeShort(short value) throws JMSException
   {
      if (trace)
         log.trace("writeShort(" + value + ")");

      ((BytesMessage) message).writeShort(value);
   }

   /**
    * Write
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void writeUTF(String value) throws JMSException
   {
      if (trace)
         log.trace("writeUTF(" + value + ")");

      ((BytesMessage) message).writeUTF(value);
   }
}
