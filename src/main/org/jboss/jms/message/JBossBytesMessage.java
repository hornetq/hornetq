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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.util.MessagingJMSException;
import org.jboss.logging.Logger;

/**
 * This class implements javax.jms.BytesMessage.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * 
 * @version $Revision$
 *
 * $Id$
 */
public class JBossBytesMessage extends JBossMessage implements BytesMessage
{
   // Static -------------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossBytesMessage.class);

   public static final byte TYPE = 1;

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   private transient ByteArrayOutputStream baos;
   private transient DataOutputStream dos;

   private transient ByteArrayInputStream bais;
   private transient DataInputStream dis;
   
   // Constructor ---------------------------------------------------

   /**
    * Only deserialization should use this constructor directly
    */
   public JBossBytesMessage()
   {   
   }
   
   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossBytesMessage(long messageID)
   {
      super(messageID);
      baos = new ByteArrayOutputStream();
      dos = new DataOutputStream(baos);
   }

   /*
    * This constructor is used to construct messages when retrieved from persistence storage
    */
   public JBossBytesMessage(long messageID,
                            boolean reliable,
                            long expiration,
                            long timestamp,
                            byte priority,
                            Map coreHeaders,
                            byte[] payloadAsByteArray,
                            String jmsType,
                            String correlationID,
                            byte[] correlationIDBytes,
                            JBossDestination destination,
                            JBossDestination replyTo,
                            HashMap jmsProperties)
   {
      super(messageID, reliable, expiration, timestamp, priority, coreHeaders, payloadAsByteArray,
            jmsType, correlationID, correlationIDBytes, destination, replyTo,
            jmsProperties);
      
      baos = new ByteArrayOutputStream();
      dos = new DataOutputStream(baos);
   }

   /**
    * 
    * Make a shallow copy of another JBossBytesMessage
    * 
    * @param other
    */
   public JBossBytesMessage(JBossBytesMessage other)
   {
      super(other);
      
      if (trace) { log.trace("Creating new JBossBytesMessage from other JBossBytesMessage"); }
      
   }

   public JBossBytesMessage(BytesMessage foreign, long id) throws JMSException
   {
      super(foreign, id);
      
      foreign.reset();
      
      baos = new ByteArrayOutputStream();
      dos = new DataOutputStream(baos);
                     
      byte[] buffer = new byte[1024];
      int n = foreign.readBytes(buffer);
      while (n != -1)
      {
         writeBytes(buffer, 0, n);
         n = foreign.readBytes(buffer);
      }
   }

   // Streamable override ---------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);

      // transfer the value read into payloadAsBytes by superclass to payload, since this is how
      // BytesMessage instances keep it
      copyPayloadAsByteArrayToPayload();
      clearPayloadAsByteArray();
   }

   // BytesMessage implementation -----------------------------------

   public boolean readBoolean() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readBoolean();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public byte readByte() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readByte();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public int readUnsignedByte() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readUnsignedByte();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public short readShort() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readShort();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public int readUnsignedShort() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readUnsignedShort();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public char readChar() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readChar();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public int readInt() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readInt();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public long readLong() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readLong();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public float readFloat() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readFloat();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public double readDouble() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readDouble();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public String readUTF() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readUTF();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public int readBytes(byte[] value) throws JMSException
   {
      checkRead();
      try
      {
         return dis.read(value);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public int readBytes(byte[] value, int length) throws JMSException
   {
      checkRead();
      try
      {
         return dis.read(value, 0, length);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeBoolean(boolean value) throws JMSException
   {
      try
      {
         dos.writeBoolean(value);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeByte(byte value) throws JMSException
   {
      try
      {
         dos.writeByte(value);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeShort(short value) throws JMSException
   {
      try
      {
         dos.writeShort(value);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeChar(char value) throws JMSException
   {
      try
      {
         dos.writeChar(value);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeInt(int value) throws JMSException
   {
      try
      {
         dos.writeInt(value);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeLong(long value) throws JMSException
   {
      try
      {
         dos.writeLong(value);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeFloat(float value) throws JMSException
   {
      try
      {
         dos.writeFloat(value);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeDouble(double value) throws JMSException
   {
      try
      {
         dos.writeDouble(value);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeUTF(String value) throws JMSException
   {
      try
      {
         dos.writeUTF((String)value);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeBytes(byte[] value) throws JMSException
   {
      try
      {
         dos.write(value, 0, value.length);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      try
      {
         dos.write(value, offset, length);
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void writeObject(Object value) throws JMSException
   {
      try
      {
         if (value == null)
         {
            throw new NullPointerException("Attempt to write a new value");
         }
         if (value instanceof String)
         {
            dos.writeUTF((String)value);
         }
         else if (value instanceof Boolean)
         {
            dos.writeBoolean(((Boolean) value).booleanValue());
         }
         else if (value instanceof Byte)
         {
            dos.writeByte(((Byte) value).byteValue());
         }
         else if (value instanceof Short)
         {
            dos.writeShort(((Short) value).shortValue());
         }
         else if (value instanceof Integer)
         {
            dos.writeInt(((Integer) value).intValue());
         }
         else if (value instanceof Long)
         {
            dos.writeLong(((Long) value).longValue());
         }
         else if (value instanceof Float)
         {
            dos.writeFloat(((Float) value).floatValue());
         }
         else if (value instanceof Double)
         {
            dos.writeDouble(((Double) value).doubleValue());
         }
         else if (value instanceof byte[])
         {
            dos.write((byte[]) value, 0, ((byte[]) value).length);
         }
         else
         {
            throw new MessageFormatException("Invalid object for properties");
         }
      }
      catch (IOException e)
      {
         throw new MessagingJMSException("IOException", e);
      }
   }

   public void reset() throws JMSException
   {
      if (trace) log.trace("reset()");
      try
      {
         if (baos != null)
         {
            if (trace)  { log.trace("Flushing ostream to array"); }

            dos.flush();
            this.setPayload(baos.toByteArray());
            this.clearPayloadAsByteArray();

            if (trace) { log.trace("Array is now: " + this.getPayload()); }

            baos.close();
         }
         baos = null;
         bais = null;
         dis = null;
         dos = null;      
      }
      catch (Exception e)
      {
         JMSException e2 = new JMSException(e.getMessage());
         e2.setStackTrace(e.getStackTrace());
         throw e2;
      }
   }

   // MessageSupport overrides --------------------------------------

   /**
    * A JBossBytesMessage avoid double serialization by holding on its original payload, which is
    * a byte[] to start with.
    */
   public byte[] getPayloadAsByteArray()
   {
      return (byte[])getPayload();
   }

   // JBossMessage overrides ----------------------------------------

   public void doAfterSend() throws JMSException
   {
      reset();
   }

   public void clearBody() throws JMSException
   {
      try
      {
         if (baos != null)
         {
            baos.close();
         }
         else
         {
            // REVIEW: istream is only initialised on a read.
            // It looks like it is possible to acknowledge
            // a message without reading it? Guard against
            // an NPE in this case.
            if (bais != null)
            {
               bais.close();
            }
         }
      }
      catch (IOException e)
      {
         //don't throw an exception
      }

      baos = new ByteArrayOutputStream();
      dos = new DataOutputStream(baos);
      this.setPayload(null);
      bais = null;
      dis = null;

      super.clearBody();
   }

   public long getBodyLength() throws JMSException
   {
      checkRead();
      return ((byte[])this.getPayload()).length;
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossBytesMessage.TYPE;
   }

   public JBossMessage doShallowCopy() throws JMSException
   {
      reset();
      
      return new JBossBytesMessage(this);      
   }
      
   public void copyPayload(Object payload) throws JMSException
   {
      byte[] otherBytes = (byte[])payload;
      if (otherBytes == null)
      {
         this.setPayload(null);
      }
      else
      {
         this.setPayload(new byte[otherBytes.length]);
         System.arraycopy(otherBytes, 0, this.getPayload(), 0, otherBytes.length);
      }     
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * Check the message is readable
    *
    * @throws JMSException when not readable
    */
   void checkRead() throws JMSException
   {   
      // We have just received/reset() the message, and the client is trying to
      // read it
      if (bais == null || dis == null)
      {
         if (trace) {  log.trace("internalArray:" + this.getPayload()); }
         bais = new ByteArrayInputStream((byte[])this.getPayload());
         dis = new DataInputStream(bais);
      }
   }

   // Inner classes -------------------------------------------------
}
