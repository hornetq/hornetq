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
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import org.jboss.jms.util.JBossJMSException;
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
public class JBossBytesMessage extends JBossMessage implements BytesMessage, Externalizable
{
   // Static -------------------------------------------------------

   private static final long serialVersionUID = 4636242783244742795L;

   private static final Logger log = Logger.getLogger(JBossBytesMessage.class);

   public static final byte TYPE = 1;

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   private transient ByteArrayOutputStream ostream;

   private transient DataOutputStream p;

   private transient ByteArrayInputStream istream;

   private transient DataInputStream m;
   
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
   public JBossBytesMessage(String messageID)
   {
      super(messageID);
      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
   }
   

   /*
    * This constructor is used to construct messages when retrieved from persistence storage
    */
   public JBossBytesMessage(String messageID,
         boolean reliable,
         long expiration,
         long timestamp,
         byte priority,
         Map coreHeaders,
         byte[] payloadAsByteArray,
         String jmsType,
         String correlationID,
         byte[] correlationIDBytes,
         boolean destinationIsQueue,
         String destination,
         boolean replyToIsQueue,
         String replyTo,
         HashMap jmsProperties)
   {
      super(messageID, reliable, expiration, timestamp, priority, coreHeaders, payloadAsByteArray,
            jmsType, correlationID, correlationIDBytes, destinationIsQueue, destination, replyToIsQueue, replyTo, 
            jmsProperties);
      
      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
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

   public JBossBytesMessage(BytesMessage foreign) throws JMSException
   {
      super(foreign);
      
      foreign.reset();
      
      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
                     
      byte[] buffer = new byte[1024];
      int n = foreign.readBytes(buffer);
      while (n != -1)
      {
         writeBytes(buffer, 0, n);
         n = foreign.readBytes(buffer);
      }
                             
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
         this.clearPayloadAsByteArray();
      }
      else
      {
         this.setPayload(new byte[otherBytes.length]);
         this.clearPayloadAsByteArray();
         System.arraycopy(otherBytes, 0, this.getPayload(), 0, otherBytes.length);
      }     
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
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
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeBoolean(boolean value) throws JMSException
   {
      try
      {
         p.writeBoolean(value);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeByte(byte value) throws JMSException
   {
      try
      {
         p.writeByte(value);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeShort(short value) throws JMSException
   {
      try
      {
         p.writeShort(value);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeChar(char value) throws JMSException
   {
      try
      {
         p.writeChar(value);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeInt(int value) throws JMSException
   {
      try
      {
         p.writeInt(value);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeLong(long value) throws JMSException
   {
      try
      {
         p.writeLong(value);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeFloat(float value) throws JMSException
   {
      try
      {
         p.writeFloat(value);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeDouble(double value) throws JMSException
   {
      try
      {
         p.writeDouble(value);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeUTF(String value) throws JMSException
   {
      try
      {
         p.writeUTF(value);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeBytes(byte[] value) throws JMSException
   {
      try
      {
         p.write(value, 0, value.length);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
   }

   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      try
      {
         p.write(value, offset, length);
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
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
            //p.writeChars((String) value);
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
         throw new JBossJMSException("IOException", e);
      }

   }

   public void reset() throws JMSException
   {
      if (trace) log.trace("reset()");
      try
      {
         if (ostream != null)
         {
            if (trace)  { log.trace("Flushing ostream to array"); }

            p.flush();
            this.setPayload(ostream.toByteArray());
            this.clearPayloadAsByteArray();

            if (trace) { log.trace("Array is now: " + this.getPayload()); }

            ostream.close();
         }
         ostream = null;
         istream = null;
         m = null;
         p = null;
      }
      catch (IOException e)
      {
         throw new JBossJMSException("IOException", e);
      }
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
         if (ostream != null)
         {
            ostream.close();
         }
         else
         {
            // REVIEW: istream is only initialised on a read.
            // It looks like it is possible to acknowledge
            // a message without reading it? Guard against
            // an NPE in this case.
            if (istream != null)
            {
               istream.close();
            }
         }
      }
      catch (IOException e)
      {
         //don't throw an exception
      }

      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
      this.setPayload(null);
      this.clearPayloadAsByteArray();
      istream = null;
      m = null;
      
      super.clearBody();
   }
   
   public long getBodyLength() throws JMSException
   {
      checkRead();
      return ((byte[])this.getPayload()).length;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void writePayloadExternal(ObjectOutput out, Serializable thePayload) throws IOException
   {
      out.write((byte[])thePayload);
   }

   protected Serializable readPayloadExternal(ObjectInput in, int length)
      throws IOException, ClassNotFoundException
   {
      byte[] payload = new byte[length];
      in.readFully(payload);      
      return payload;
   }

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
      if (istream == null || m == null)
      {
         if (trace) {  log.trace("internalArray:" + this.getPayload()); }
         istream = new ByteArrayInputStream((byte[])this.getPayload());
         m = new DataInputStream(istream);
      }
   }

   // Inner classes -------------------------------------------------
}
