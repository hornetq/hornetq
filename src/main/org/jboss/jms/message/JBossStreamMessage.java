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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.jboss.logging.Logger;
import org.jboss.util.Primitives;

/**
 * This class implements javax.jms.StreamMessage.
 * 
 * It is largely ported from SpyStreamMessage in JBossMQ.
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
public class JBossStreamMessage extends JBossMessage implements StreamMessage
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 5504501713994881078L;

   private static final Logger log = Logger.getLogger(JBossStreamMessage.class);

   public static final int TYPE = 4;

   // Attributes ----------------------------------------------------

   protected int position;

   protected int offset;

   protected int size;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Only deserialization should use this constructor directory
    */
   public JBossStreamMessage()
   {     
   }
   
   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossStreamMessage(String messageID)
   {
      super(messageID);      
      payload = new ArrayList();
      position = 0;
      size = 0;
      offset = 0;
   }
   
   /*
    * This constructor is used to construct messages when retrieved from persistence storage
    */
   public JBossStreamMessage(String messageID,
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
      
   }

   /**
    * 
    * Make a shallow copy of another JBossStreamMessage
    * 
    * @param other
    */
   public JBossStreamMessage(JBossStreamMessage other)
   {
      super(other);      
   }

   public JBossStreamMessage(StreamMessage foreign) throws JMSException
   {
      super(foreign);
      
      foreign.reset();
      
      payload = new ArrayList();
      position = 0;
      size = 0;
      offset = 0;
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

   public int getType()
   {
      return JBossStreamMessage.TYPE;
   }
   
   public void doAfterSend() throws JMSException
   {      
      reset();
   }
   
   public void copyPayload(Object other) throws JMSException
   {
      reset();
      payload = new ArrayList((List)other);
   }

   // StreamMessage implementation ----------------------------------

   public boolean readBoolean() throws JMSException
   {

      try
      {
         Object value = ((List)payload).get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Boolean)
         {
            position++;
            return ((Boolean) value).booleanValue();
         }
         else if (value instanceof String)
         {
            boolean result = Boolean.valueOf((String) value).booleanValue();
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }

   }

   public byte readByte() throws JMSException
   {
      try
      {
         Object value = ((List)payload).get(position);
         offset = 0;
         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Byte)
         {
            position++;
            return ((Byte) value).byteValue();
         }
         else if (value instanceof String)
         {
            byte result = Byte.parseByte((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public short readShort() throws JMSException
   {
      try
      {
         Object value = ((List)payload).get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Byte)
         {
            position++;
            return ((Byte) value).shortValue();
         }
         else if (value instanceof Short)
         {
            position++;
            return ((Short) value).shortValue();
         }
         else if (value instanceof String)
         {
            short result = Short.parseShort((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public char readChar() throws JMSException
   {
      try
      {
         Object value = ((List)payload).get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Character)
         {
            position++;
            return ((Character) value).charValue();
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readInt() throws JMSException
   {
      try
      {
         Object value = ((List)payload).get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Byte)
         {
            position++;
            return ((Byte) value).intValue();
         }
         else if (value instanceof Short)
         {
            position++;
            return ((Short) value).intValue();
         }
         else if (value instanceof Integer)
         {
            position++;
            return ((Integer) value).intValue();
         }
         else if (value instanceof String)
         {
            int result = Integer.parseInt((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public long readLong() throws JMSException
   {
      try
      {
         Object value = ((List)payload).get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Byte)
         {
            position++;
            return ((Byte) value).longValue();
         }
         else if (value instanceof Short)
         {
            position++;
            return ((Short) value).longValue();
         }
         else if (value instanceof Integer)
         {
            position++;
            return ((Integer) value).longValue();
         }
         else if (value instanceof Long)
         {
            position++;
            return ((Long) value).longValue();
         }
         else if (value instanceof String)
         {
            long result = Long.parseLong((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public float readFloat() throws JMSException
   {
      try
      {
         Object value = ((List)payload).get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Float)
         {
            position++;
            return ((Float) value).floatValue();
         }
         else if (value instanceof String)
         {
            float result = Float.parseFloat((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public double readDouble() throws JMSException
   {
      try
      {
         Object value = ((List)payload).get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Float)
         {
            position++;
            return ((Float) value).doubleValue();
         }
         else if (value instanceof Double)
         {
            position++;
            return ((Double) value).doubleValue();
         }
         else if (value instanceof String)
         {
            double result = Double.parseDouble((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public String readString() throws JMSException
   {
      try
      {
         Object value = ((List)payload).get(position);
         offset = 0;

         if (value == null)
         {
            position++;
            return null;
         }
         else if (value instanceof Boolean)
         {
            position++;
            return ((Boolean) value).toString();
         }
         else if (value instanceof Byte)
         {
            position++;
            return ((Byte) value).toString();
         }
         else if (value instanceof Short)
         {
            position++;
            return ((Short) value).toString();
         }
         else if (value instanceof Character)
         {
            position++;
            return ((Character) value).toString();
         }
         else if (value instanceof Integer)
         {
            position++;
            return ((Integer) value).toString();
         }
         else if (value instanceof Long)
         {
            position++;
            return ((Long) value).toString();
         }
         else if (value instanceof Float)
         {
            position++;
            return ((Float) value).toString();
         }
         else if (value instanceof Double)
         {
            position++;
            return ((Double) value).toString();
         }
         else if (value instanceof String)
         {
            position++;
            return (String) value;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readBytes(byte[] value) throws JMSException
   {
      try
      {
         Object myObj = ((List)payload).get(position);
         if (myObj == null)
            throw new NullPointerException("Value is null");
         else if (!(myObj instanceof byte[]))
            throw new MessageFormatException("Invalid conversion");
         byte[] obj = (byte[]) myObj;

         if (obj.length == 0)
         {
            position++;
            offset = 0;
            return 0;
         }

         if (offset >= obj.length)
         {
            position++;
            offset = 0;
            return -1;
         }

         if (obj.length - offset < value.length)
         {
            for (int i = 0; i < obj.length; i++)
               value[i] = obj[i + offset];

            position++;
            offset = 0;

            return obj.length - offset;
         }
         else
         {
            for (int i = 0; i < value.length; i++)
               value[i] = obj[i + offset];
            offset += value.length;

            return value.length;
         }

      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public Object readObject() throws JMSException
   {
      try
      {
         Object value = ((List)payload).get(position);
         position++;
         offset = 0;

         return value;
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public void writeBoolean(boolean value) throws JMSException
   {

      ((List)payload).add(Primitives.valueOf(value));
   }

   public void writeByte(byte value) throws JMSException
   {
      ((List)payload).add(new Byte(value));
   }

   public void writeShort(short value) throws JMSException
   {      
      ((List)payload).add(new Short(value));
   }

   public void writeChar(char value) throws JMSException
   {
      ((List)payload).add(new Character(value));
   }

   public void writeInt(int value) throws JMSException
   {
      ((List)payload).add(new Integer(value));
   }

   public void writeLong(long value) throws JMSException
   {
      ((List)payload).add(new Long(value));
   }

   public void writeFloat(float value) throws JMSException
   {
      ((List)payload).add(new Float(value));
   }

   public void writeDouble(double value) throws JMSException
   {
      ((List)payload).add(new Double(value));
   }

   public void writeString(String value) throws JMSException
   {
      if (value == null)
         ((List)payload).add(null);
      else
         ((List)payload).add(value);
   }

   public void writeBytes(byte[] value) throws JMSException
   {
      ((List)payload).add(value.clone());
   }

   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      if (offset + length > value.length)
         throw new JMSException("Array is too small");
      byte[] temp = new byte[length];
      for (int i = 0; i < length; i++)
         temp[i] = value[i + offset];

      ((List)payload).add(temp);
   }

   public void writeObject(Object value) throws JMSException
   {
      if (value == null)
         ((List)payload).add(null);
      else if (value instanceof Boolean)
         ((List)payload).add(value);
      else if (value instanceof Byte)
         ((List)payload).add(value);
      else if (value instanceof Short)
         ((List)payload).add(value);
      else if (value instanceof Character)
         ((List)payload).add(value);
      else if (value instanceof Integer)
         ((List)payload).add(value);
      else if (value instanceof Long)
         ((List)payload).add(value);
      else if (value instanceof Float)
         ((List)payload).add(value);
      else if (value instanceof Double)
         ((List)payload).add(value);
      else if (value instanceof String)
         ((List)payload).add(value);
      else if (value instanceof byte[])
         ((List)payload).add(((byte[]) value).clone());
      else
         throw new MessageFormatException("Invalid object type");
   }

   public void reset() throws JMSException
   {      
      position = 0;
      size = ((List)payload).size();
      offset = 0;
   }

   // JBossMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      super.clearBody();
      
      payload = new ArrayList();
      position = 0;
      offset = 0;
      size = 0;

   }
   
   public JBossMessage doShallowCopy()
   {
      return new JBossStreamMessage(this);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void writePayloadExternal(ObjectOutput out) throws IOException
   {
      out.writeObject(((List)payload));
      out.writeInt(position);
      out.writeInt(offset);
      out.writeInt(size);
   }

   protected Serializable readPayloadExternal(ObjectInput in)
      throws IOException, ClassNotFoundException
   {
      Serializable p = null;
      try
      {
         p = (ArrayList) in.readObject();
         position = in.readInt();
         offset = in.readInt();
         size = in.readInt();
      }
      catch (ClassNotFoundException e)
      {
         log.error("Failed to find class", e);
         throw new IOException(e.getMessage());
      }

      return p;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
