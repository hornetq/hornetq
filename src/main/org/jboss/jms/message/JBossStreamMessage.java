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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.StreamMessage;

import org.jboss.messaging.util.StreamUtils;

/**
 * This class implements javax.jms.StreamMessage.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * 
 * @version $Revision: 3412 $
 *
 * $Id: JBossStreamMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class JBossStreamMessage extends JBossMessage implements StreamMessage
{
   // Constants -----------------------------------------------------

   public static final byte TYPE = 6;

   // Attributes ----------------------------------------------------

   private int position;

   private int offset;

   //private int size;
   
   private List<Object> list = new ArrayList<Object>();
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossStreamMessage()
   {   
      super(JBossStreamMessage.TYPE);
   }
   
   public JBossStreamMessage(org.jboss.messaging.newcore.Message message, long deliveryID, int deliveryCount)
   {
      super(message, deliveryID, deliveryCount);
   }
   
   public JBossStreamMessage(StreamMessage foreign) throws JMSException
   {
      super(foreign, JBossStreamMessage.TYPE);
      
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
         Object value = list.get(position);
         
         offset = 0;
         
         boolean result;

         if (value == null)
         {
            throw new NullPointerException("Value is null");
         }
         else if (value instanceof Boolean)
         {            
            result = ((Boolean)value).booleanValue();
         }
         else if (value instanceof String)
         {
            result = Boolean.valueOf((String) value).booleanValue();
         }
         else
         {
            throw new MessageFormatException("Invalid conversion");
         }
         
         position++;
         
         return result;
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
         Object value = list.get(position);
         
         offset = 0;
         
         byte result;
         
         if (value == null)
         {
            throw new NullPointerException("Value is null");
         }
         else if (value instanceof Byte)
         {
            result =  ((Byte) value).byteValue();
         }
         else if (value instanceof String)
         {
            result = Byte.parseByte((String) value);
         }
         else
         {
            throw new MessageFormatException("Invalid conversion");
         }
         
         position++;
         
         return result;
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
         Object value = list.get(position);
         
         short result;
         
         offset = 0;

         if (value == null)
         {
            throw new NullPointerException("Value is null");
         }
         else if (value instanceof Byte)
         {
            result = ((Byte) value).shortValue();
         }
         else if (value instanceof Short)
         {
            result = ((Short) value).shortValue();
         }
         else if (value instanceof String)
         {
            result = Short.parseShort((String) value);
         }
         else
         {
            throw new MessageFormatException("Invalid conversion");
         }
         
         position++;
         
         return result;         
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
         Object value = list.get(position);
         
         char result;
         
         offset = 0;

         if (value == null)
         {
            throw new NullPointerException("Value is null");
         }
         else if (value instanceof Character)
         {
            result = ((Character) value).charValue();
         }
         else
         {
            throw new MessageFormatException("Invalid conversion");
         }
         
         position++;
         
         return result;
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
         Object value = list.get(position);
         
         int result;
         
         offset = 0;

         if (value == null)
         {
            throw new NullPointerException("Value is null");
         }
         else if (value instanceof Byte)
         {
            result = ((Byte) value).intValue();
         }
         else if (value instanceof Short)
         {
            result = ((Short) value).intValue();
         }
         else if (value instanceof Integer)
         {
            result = ((Integer) value).intValue();
         }
         else if (value instanceof String)
         {
            result = Integer.parseInt((String) value);
         }
         else
         {
            throw new MessageFormatException("Invalid conversion");
         }
         
         position++;
         
         return result;
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
         Object value = list.get(position);
         
         long result;
         
         offset = 0;

         if (value == null)
         {
            throw new NullPointerException("Value is null");
         }
         else if (value instanceof Byte)
         {
            result = ((Byte) value).longValue();
         }
         else if (value instanceof Short)
         {
            result = ((Short) value).longValue();
         }
         else if (value instanceof Integer)
         {
            result = ((Integer) value).longValue();
         }
         else if (value instanceof Long)
         {
            result = ((Long) value).longValue();
         }
         else if (value instanceof String)
         {
            result = Long.parseLong((String) value);
         }
         else
         {
            throw new MessageFormatException("Invalid conversion");
         }
         
         position++;
         
         return result;
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
         Object value = list.get(position);
         
         float result;
         
         offset = 0;

         if (value == null)
         {
            throw new NullPointerException("Value is null");
         }
         else if (value instanceof Float)
         {
            result = ((Float) value).floatValue();
         }
         else if (value instanceof String)
         {
            result = Float.parseFloat((String) value);
         }
         else
         {
            throw new MessageFormatException("Invalid conversion");
         }
         
         position++;
         
         return result;
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
         Object value = list.get(position);
         
         offset = 0;
         
         double result;

         if (value == null)
         {
            throw new NullPointerException("Value is null");
         }
         else if (value instanceof Float)
         {
            result = ((Float) value).doubleValue();
         }
         else if (value instanceof Double)
         {
            result = ((Double) value).doubleValue();
         }
         else if (value instanceof String)
         {
            result = Double.parseDouble((String) value);
         }
         else
         {
            throw new MessageFormatException("Invalid conversion");
         }
         
         position++;
         
         return result;
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public String readString() throws JMSException
   {
      checkRead();
      try
      {
         Object value = list.get(position);
         
         String result;
         
         offset = 0;

         if (value == null)
         {
            result = null;
         }
         else if (value instanceof Boolean)
         {
            result = ((Boolean) value).toString();
         }
         else if (value instanceof Byte)
         {
            result = ((Byte) value).toString();
         }
         else if (value instanceof Short)
         {
            result = ((Short) value).toString();
         }
         else if (value instanceof Character)
         {
            result = ((Character) value).toString();
         }
         else if (value instanceof Integer)
         {
            result = ((Integer) value).toString();
         }
         else if (value instanceof Long)
         {
            result = ((Long) value).toString();
         }
         else if (value instanceof Float)
         {
            result = ((Float) value).toString();
         }
         else if (value instanceof Double)
         {
            result = ((Double) value).toString();
         }
         else if (value instanceof String)
         {
            result =  (String) value;
         }
         else
         {
            throw new MessageFormatException("Invalid conversion");
         }
         
         position++;
         
         return result;
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readBytes(byte[] value) throws JMSException
   {
      checkRead();
      try
      {
         Object myObj = list.get(position);
         
         if (myObj == null)
         {
            throw new NullPointerException("Value is null");
         }
         
         if (!(myObj instanceof byte[]))
         {
            throw new MessageFormatException("Invalid conversion");
         }
         
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
      checkRead();
      try
      {
         Object value = list.get(position);
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
      checkWrite();
      list.add(Boolean.valueOf(value));
   }

   public void writeByte(byte value) throws JMSException
   {
      checkWrite();
      list.add(Byte.valueOf(value));
   }

   public void writeShort(short value) throws JMSException
   {      
      checkWrite();
      list.add(Short.valueOf(value));
   }

   public void writeChar(char value) throws JMSException
   {
      checkWrite();
      list.add(Character.valueOf(value));
   }

   public void writeInt(int value) throws JMSException
   {
      checkWrite();
      list.add(Integer.valueOf(value));
   }

   public void writeLong(long value) throws JMSException
   {
      checkWrite();
      list.add(Long.valueOf(value));
   }

   public void writeFloat(float value) throws JMSException
   {
      checkWrite();
      list.add(Float.valueOf(value));
   }

   public void writeDouble(double value) throws JMSException
   {
      checkWrite();
      list.add(Double.valueOf(value));
   }

   public void writeString(String value) throws JMSException
   {
      checkWrite();
      if (value == null)
      {
         list.add(null);
      }
      else
      {
         list.add(value);
      }
   }

   public void writeBytes(byte[] value) throws JMSException
   {
      checkWrite();
      list.add(value.clone());
   }

   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      checkWrite();
      if (offset + length > value.length)
      {
         throw new JMSException("Invalid offset/length");
      }
      
      byte[] newBytes = new byte[length];
      
      System.arraycopy(value, offset, newBytes, 0, length);

      list.add(newBytes);
   }

   public void writeObject(Object value) throws JMSException
   {
      checkWrite();
      if (value == null)
         list.add(null);
      else if (value instanceof Boolean)
         list.add(value);
      else if (value instanceof Byte)
         list.add(value);
      else if (value instanceof Short)
         list.add(value);
      else if (value instanceof Character)
         list.add(value);
      else if (value instanceof Integer)
         list.add(value);
      else if (value instanceof Long)
         list.add(value);
      else if (value instanceof Float)
         list.add(value);
      else if (value instanceof Double)
         list.add(value);
      else if (value instanceof String)
         list.add(value);
      else if (value instanceof byte[])
         list.add(((byte[]) value).clone());
      else
         throw new MessageFormatException("Invalid object type");
   }

   public void reset() throws JMSException
   {      
      position = 0;
      offset = 0;
      readOnly = true;
   }

   // JBossMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      super.clearBody();
      
      list.clear();
      position = 0;
      offset = 0;
   }
   
   public void doBeforeSend() throws Exception
   {
      reset();

      beforeSend();
   }

   public void doBeforeReceive() throws Exception
   {
      beforeReceive();
   }
     
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void writePayload(DataOutputStream daos) throws Exception
   {
      StreamUtils.writeList(daos, list);
   }

   protected void readPayload(DataInputStream dais) throws Exception
   {
      list = StreamUtils.readList(dais);
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
