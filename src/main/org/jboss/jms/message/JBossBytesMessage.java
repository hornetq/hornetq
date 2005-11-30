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
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import org.jboss.logging.Logger;
import org.jboss.jms.util.JBossJMSException;



/**
 * This class implements javax.jms.BytesMessage.
 * 
 * It is largely ported from SpyBytesMessage in JBossMQ.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
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

   public static final int TYPE = 1;

   // Attributes ----------------------------------------------------

   private transient ByteArrayOutputStream ostream;

   private transient DataOutputStream p;

   private transient ByteArrayInputStream istream;

   private transient DataInputStream m;

   // Constructor ---------------------------------------------------

   public JBossBytesMessage()
   {
      if (log.isTraceEnabled()) { log.trace("Creating new JBossBytesMessage"); }
      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
   }

   public JBossBytesMessage(String messageID,
         boolean reliable,
         long expiration,
         long timestamp,
         int priority,
         int deliveryCount,
         Map coreHeaders,
         Serializable payload,
         String jmsType,
         Object correlationID,
         boolean destinationIsQueue,
         String destination,
         boolean replyToIsQueue,
         String replyTo,
         String connectionID,
         Map jmsProperties)
   {
      super(messageID, reliable, expiration, timestamp, priority, deliveryCount, coreHeaders, payload,
            jmsType, correlationID, destinationIsQueue, destination, replyToIsQueue, replyTo, connectionID,
            jmsProperties);
   
      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
   }

   protected JBossBytesMessage(JBossBytesMessage other) throws JMSException
   {
      super(other);
      
      if (log.isTraceEnabled()) { log.trace("Creating new JBossBytesMessage from other JBossBytesMessage"); }
      
      if (other.payload != null)
      {
         if (log.isTraceEnabled()) { log.trace("There's an internal array"); }
         this.payload = new byte[((byte[])other.payload).length];
         System.arraycopy((byte[])other.payload, 0, (byte[])this.payload, 0,
                          ((byte[])other.payload).length);
      }

      // if the message is not reset, is essential to clone ostream too
      if (other.ostream != null)
      {
         if (log.isTraceEnabled()) { log.trace("ostream isn't null"); }
         this.ostream = new ByteArrayOutputStream(other.ostream.size());
         try
         {
            this.ostream.write(other.ostream.toByteArray());
         }
         catch(Exception e)
         {
            throw new JBossJMSException("Failed to clone BytesMessage's ostream", e);
         }         
      }      
      else
      {
         ostream = new ByteArrayOutputStream();
      }
      p = new DataOutputStream(this.ostream);
   }

   /**
    * A copy constructor for non-JBoss Messaging JMS byte messages.
    * TODO - This should probably not actually be a copy constructor since
    * it changes the state of the message being copied (it calls reset)- which is intrusive
    * Possibly refactor into another method
    */
   protected JBossBytesMessage(BytesMessage foreign) throws JMSException
   {
      super(foreign);
      
      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
      
      foreign.reset();            
                  
      byte[] buffer = new byte[1024];
      int n = foreign.readBytes(buffer);
      while (n != -1)
      {
         writeBytes(buffer, 0, n);
         n = foreign.readBytes(buffer);
      }
                             
   }

   // Public --------------------------------------------------------

   public int getType()
   {
      return JBossBytesMessage.TYPE;
   }


   public JBossMessage doClone() throws JMSException
   {
      return new JBossBytesMessage(this);
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
      if (bodyReadOnly)
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
         throw new JMSException("IOException");
      }

   }

   public void reset() throws JMSException
   {
      if (log.isTraceEnabled()) log.trace("reset()");
      try
      {
         if (!bodyReadOnly)
         {
            if (log.isTraceEnabled())  { log.trace("Flushing ostream to array"); }

            p.flush();
            payload = ostream.toByteArray();

            if (log.isTraceEnabled()) { log.trace("Array is now: " + payload); }

            ostream.close();
         }
         ostream = null;
         istream = null;
         m = null;
         p = null;
         bodyReadOnly = true;
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   // JBossMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      try
      {
         if (!bodyReadOnly)
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
               istream.close();
         }
      }
      catch (IOException e)
      {
         //don't throw an exception
      }

      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
      payload = null;
      istream = null;
      m = null;
      
      super.clearBody();
   }
   
   public long getBodyLength() throws JMSException
   {
      checkRead();
      return ((byte[])payload).length;
   }

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
      byte[] arrayToSend = null;
      
      if (!bodyReadOnly)
      {
         p.flush();
         arrayToSend = ostream.toByteArray();
      }
      else
      {
         arrayToSend = (byte[])payload;
      }
      super.writeExternal(out);
      if (arrayToSend == null)
      {
         out.writeInt(0); //pretend to be empty array
      }
      else
      {
         out.writeInt(arrayToSend.length);
         out.write(arrayToSend);
      }
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      super.readExternal(in);
      int length = in.readInt();
      if (length < 0)
      {
         payload = null;
      }
      else
      {
         payload = new byte[length];
         in.readFully((byte[])payload);
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
   private void checkRead() throws JMSException
   {
      if (!bodyReadOnly)
      {
         throw new MessageNotReadableException("readByte while the buffer is writeonly");
      }

      // We have just received/reset() the message, and the client is trying to
      // read it
      if (istream == null || m == null)
      {
         if (log.isTraceEnabled()) {  log.trace("internalArray:" + payload); }
         istream = new ByteArrayInputStream((byte[])payload);
         m = new DataInputStream(istream);
      }
   }

   // Inner classes -------------------------------------------------
}
