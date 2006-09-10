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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.util.StreamUtils;
import org.jboss.util.Primitives;

/**
 * This class implements javax.jms.StreamMessage.
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
public class JBossStreamMessage extends JBossMessage implements StreamMessage
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 5504501713994881078L;

   public static final byte TYPE = 4;

   // Attributes ----------------------------------------------------

   protected transient int position;

   protected transient int offset;

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
   public JBossStreamMessage(long messageID)
   {
      super(messageID);      
      setPayload(new ArrayList());
      clearPayloadAsByteArray();
      position = 0;
      size = 0;
      offset = 0;
   }
   
   /*
    * This constructor is used to construct messages when retrieved from persistence storage
    */
   public JBossStreamMessage(long messageID,
         boolean reliable,
         long expiration,
         long timestamp,
         byte priority,
         Map coreHeaders,
         byte[] payloadAsByteArray,         
         int persistentChannelCount,
         String jmsType,
         String correlationID,
         byte[] correlationIDBytes,
         JBossDestination destination,
         JBossDestination replyTo,
         HashMap jmsProperties)
   {
      super(messageID, reliable, expiration, timestamp, priority, coreHeaders, payloadAsByteArray,
            persistentChannelCount,
            jmsType, correlationID, correlationIDBytes, destination, replyTo, 
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

   public JBossStreamMessage(StreamMessage foreign, long id) throws JMSException
   {
      super(foreign, id);
      
      foreign.reset();
      
      setPayload(new ArrayList());
      clearPayloadAsByteArray();
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

   public byte getType()
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
      setPayload(new ArrayList((List)other));
      clearPayloadAsByteArray();
   }

   // StreamMessage implementation ----------------------------------

   public boolean readBoolean() throws JMSException
   {

      try
      {
         Object value = ((List)getPayload()).get(position);
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
         Object value = ((List)getPayload()).get(position);
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
         Object value = ((List)getPayload()).get(position);
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
         Object value = ((List)getPayload()).get(position);
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
         Object value = ((List)getPayload()).get(position);
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
         Object value = ((List)getPayload()).get(position);
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
         Object value = ((List)getPayload()).get(position);
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
         Object value = ((List)getPayload()).get(position);
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
         Object value = ((List)getPayload()).get(position);
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
         Object myObj = ((List)getPayload()).get(position);
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
         Object value = ((List)getPayload()).get(position);
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

      ((List)getPayload()).add(Primitives.valueOf(value));
   }

   public void writeByte(byte value) throws JMSException
   {
      ((List)getPayload()).add(new Byte(value));
   }

   public void writeShort(short value) throws JMSException
   {      
      ((List)getPayload()).add(new Short(value));
   }

   public void writeChar(char value) throws JMSException
   {
      ((List)getPayload()).add(new Character(value));
   }

   public void writeInt(int value) throws JMSException
   {
      ((List)getPayload()).add(new Integer(value));
   }

   public void writeLong(long value) throws JMSException
   {
      ((List)getPayload()).add(new Long(value));
   }

   public void writeFloat(float value) throws JMSException
   {
      ((List)getPayload()).add(new Float(value));
   }

   public void writeDouble(double value) throws JMSException
   {
      ((List)getPayload()).add(new Double(value));
   }

   public void writeString(String value) throws JMSException
   {
      if (value == null)
      {
         ((List)getPayload()).add(null);
      }
      else
      {
         ((List)getPayload()).add(value);
      }
   }

   public void writeBytes(byte[] value) throws JMSException
   {
      ((List)getPayload()).add(value.clone());
   }

   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      if (offset + length > value.length)
         throw new JMSException("Array is too small");
      byte[] temp = new byte[length];
      for (int i = 0; i < length; i++)
         temp[i] = value[i + offset];

      ((List)getPayload()).add(temp);
   }

   public void writeObject(Object value) throws JMSException
   {
      if (value == null)
         ((List)getPayload()).add(null);
      else if (value instanceof Boolean)
         ((List)getPayload()).add(value);
      else if (value instanceof Byte)
         ((List)getPayload()).add(value);
      else if (value instanceof Short)
         ((List)getPayload()).add(value);
      else if (value instanceof Character)
         ((List)getPayload()).add(value);
      else if (value instanceof Integer)
         ((List)getPayload()).add(value);
      else if (value instanceof Long)
         ((List)getPayload()).add(value);
      else if (value instanceof Float)
         ((List)getPayload()).add(value);
      else if (value instanceof Double)
         ((List)getPayload()).add(value);
      else if (value instanceof String)
         ((List)getPayload()).add(value);
      else if (value instanceof byte[])
         ((List)getPayload()).add(((byte[]) value).clone());
      else
         throw new MessageFormatException("Invalid object type");
   }

   public void reset() throws JMSException
   {      
      position = 0;
      size = ((List)getPayload()).size();
      offset = 0;
   }

   // JBossMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      super.clearBody();
      
      setPayload(new ArrayList());
      clearPayloadAsByteArray();
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

   protected void writePayloadExternal(ObjectOutput out, Serializable thePayload) throws IOException
   {
      StreamUtils.writeList(out, (List)thePayload);
   }

   protected Serializable readPayloadExternal(ObjectInput in, int length)
      throws IOException, ClassNotFoundException
   {
      ArrayList l = StreamUtils.readList(in);
      return l;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
