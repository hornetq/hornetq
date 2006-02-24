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
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;

import org.jboss.util.Primitives;

/**
 * This class implements javax.jms.MapMessage
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
public class JBossMapMessage extends JBossMessage implements MapMessage
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -8018832209056373908L;

   public static final byte TYPE = 2;


   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Only deserialization should use this constructor directory
    */
   public JBossMapMessage()
   {     
   }
   
   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossMapMessage(String messageID)
   {
      super(messageID);
      payload = new HashMap();
   }

   /*
    * This constructor is used to construct messages when retrieved from persistence storage
    */
   public JBossMapMessage(String messageID,
         boolean reliable,
         long expiration,
         long timestamp,
         byte priority,
         Map coreHeaders,
         Serializable payload,
         String jmsType,
         String correlationID,
         byte[] correlationIDBytes,
         boolean destinationIsQueue,
         String destination,
         boolean replyToIsQueue,
         String replyTo,
         HashMap jmsProperties)
   {
      super(messageID, reliable, expiration, timestamp, priority, coreHeaders, payload,
            jmsType, correlationID, correlationIDBytes, destinationIsQueue, destination, replyToIsQueue, replyTo, 
            jmsProperties);
   }

   public JBossMapMessage(JBossMapMessage other)
   {
      super(other);
   }

   /**
    * 
    * Make a shallow copy of another JBossMapMessage
    * @param foreign
    * @throws JMSException
    */
   public JBossMapMessage(MapMessage foreign) throws JMSException
   {
      super(foreign);     
      payload = new HashMap();
      Enumeration names = foreign.getMapNames();
      while (names.hasMoreElements())
      {
         String name = (String)names.nextElement();
         Object obj = foreign.getObject(name);
         this.setObject(name, obj);
      } 
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossMapMessage.TYPE;
   }
   
   
   public void copyPayload(Object payload) throws JMSException
   {      
      this.payload = new HashMap((Map)payload);
   }


   // MapMessage implementation -------------------------------------

   public void setBoolean(String name, boolean value) throws JMSException
   {
      checkName(name);
      ((Map)payload).put(name, Primitives.valueOf(value));
   }

   public void setByte(String name, byte value) throws JMSException
   {
      checkName(name);
      ((Map)payload).put(name, new Byte(value));
   }

   public void setShort(String name, short value) throws JMSException
   {
      checkName(name);
      ((Map)payload).put(name, new Short(value));
   }

   public void setChar(String name, char value) throws JMSException
   {
      checkName(name);
      ((Map)payload).put(name, new Character(value));
   }

   public void setInt(String name, int value) throws JMSException
   {
      checkName(name);
      ((Map)payload).put(name, new Integer(value));
   }

   public void setLong(String name, long value) throws JMSException
   {
      checkName(name);
      ((Map)payload).put(name, new Long(value));
   }

   public void setFloat(String name, float value) throws JMSException
   {
      checkName(name);
      ((Map)payload).put(name, new Float(value));
   }

   public void setDouble(String name, double value) throws JMSException
   {
      checkName(name);
      ((Map)payload).put(name, new Double(value));
   }

   public void setString(String name, String value) throws JMSException
   {
      checkName(name);
      ((Map)payload).put(name, value);
   }

   public void setBytes(String name, byte[] value) throws JMSException
   {
      checkName(name);
      ((Map)payload).put(name, value.clone());
   }

   public void setBytes(String name, byte[] value, int offset, int length) throws JMSException
   {
      checkName(name);
      if (offset + length > value.length)
      {
         throw new JMSException("Array is too small");
      }
      byte[] temp = new byte[length];
      for (int i = 0; i < length; i++)
      {
         temp[i] = value[i + offset];
      }
      ((Map)payload).put(name, temp);
   }

   public void setObject(String name, Object value) throws JMSException
   {
      checkName(name);
      if (value instanceof Boolean)
         ((Map)payload).put(name, value);
      else if (value instanceof Byte)
         ((Map)payload).put(name, value);
      else if (value instanceof Short)
         ((Map)payload).put(name, value);
      else if (value instanceof Character)
         ((Map)payload).put(name, value);
      else if (value instanceof Integer)
         ((Map)payload).put(name, value);
      else if (value instanceof Long)
         ((Map)payload).put(name, value);
      else if (value instanceof Float)
         ((Map)payload).put(name, value);
      else if (value instanceof Double)
         ((Map)payload).put(name, value);
      else if (value instanceof String)
         ((Map)payload).put(name, value);
      else if (value instanceof byte[])
         ((Map)payload).put(name, ((byte[]) value).clone());
      else
         throw new MessageFormatException("Invalid object type.");

   }

   public boolean getBoolean(String name) throws JMSException
   {
      Object value;

      value = ((Map)payload).get(name);

      if (value == null)
         return Boolean.valueOf(null).booleanValue();

      if (value instanceof Boolean)
         return ((Boolean) value).booleanValue();
      else if (value instanceof String)
         return Boolean.valueOf((String) value).booleanValue();
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public byte getByte(String name) throws JMSException
   {
      Object value;

      value = ((Map)payload).get(name);

      if (value == null)
         return Byte.parseByte(null);

      if (value instanceof Byte)
         return ((Byte) value).byteValue();
      else if (value instanceof String)
         return Byte.parseByte((String) value);
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public short getShort(String name) throws JMSException
   {
      Object value;

      value = ((Map)payload).get(name);

      if (value == null)
         return Short.parseShort(null);

      if (value instanceof Byte)
         return ((Byte) value).shortValue();
      else if (value instanceof Short)
         return ((Short) value).shortValue();
      else if (value instanceof String)
         return Short.parseShort((String) value);
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public char getChar(String name) throws JMSException
   {
      Object value;

      value = ((Map)payload).get(name);

      if (value == null)
         throw new NullPointerException("Invalid conversion");

      if (value instanceof Character)
         return ((Character) value).charValue();
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public int getInt(String name) throws JMSException
   {
      Object value;

      value = ((Map)payload).get(name);

      if (value == null)
         return Integer.parseInt(null);

      if (value instanceof Byte)
         return ((Byte) value).intValue();
      else if (value instanceof Short)
         return ((Short) value).intValue();
      else if (value instanceof Integer)
         return ((Integer) value).intValue();
      else if (value instanceof String)
         return Integer.parseInt((String) value);
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public long getLong(String name) throws JMSException
   {
      Object value;

      value = ((Map)payload).get(name);

      if (value == null)
         return Long.parseLong(null);

      if (value instanceof Byte)
         return ((Byte) value).longValue();
      else if (value instanceof Short)
         return ((Short) value).longValue();
      else if (value instanceof Integer)
         return ((Integer) value).longValue();
      else if (value instanceof Long)
         return ((Long) value).longValue();
      else if (value instanceof String)
         return Long.parseLong((String) value);
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public float getFloat(String name) throws JMSException
   {
      Object value;

      value = ((Map)payload).get(name);

      if (value == null)
         return Float.parseFloat(null);

      if (value instanceof Float)
         return ((Float) value).floatValue();
      else if (value instanceof String)
         return Float.parseFloat((String) value);
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public double getDouble(String name) throws JMSException
   {
      Object value;

      value = ((Map)payload).get(name);

      if (value == null)
         return Double.parseDouble(null);

      if (value instanceof Float)
         return ((Float) value).doubleValue();
      else if (value instanceof Double)
         return ((Double) value).doubleValue();
      else if (value instanceof String)
         return Double.parseDouble((String) value);
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public String getString(String name) throws JMSException
   {
      Object value;

      value = ((Map)payload).get(name);

      if (value == null)
         return null;

      if (value instanceof Boolean)
      {
         return  value.toString();
      }
      else if (value instanceof Byte)
      {
         return value.toString();
      }
      else if (value instanceof Short)
      {
         return value.toString();
      }
      else if (value instanceof Character)
      {
         return value.toString();
      }
      else if (value instanceof Integer)
      {
         return value.toString();
      }
      else if (value instanceof Long)
      {
         return value.toString();
      }
      else if (value instanceof Float)
      {
         return value.toString();
      }
      else if (value instanceof Double)
      {
         return value.toString();
      }
      else if (value instanceof String)
      {
         return (String) value;
      }
      else
      {
         throw new MessageFormatException("Invalid conversion");
      }
   }

   public byte[] getBytes(String name) throws JMSException
   {
      Object value;

      value = ((Map)payload).get(name);

      if (value == null)
         return null;
      if (value instanceof byte[])
         return (byte[]) value;
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public Object getObject(String name) throws JMSException
   {

      return ((Map)payload).get(name);

   }

   public Enumeration getMapNames() throws JMSException
   {

      return Collections.enumeration(new HashMap(((Map)payload)).keySet());

   }

   public boolean itemExists(String name) throws JMSException
   {

      return ((Map)payload).containsKey(name);

   }

   // JBossMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      super.clearBody();
      payload = new HashMap();
   }
   
   public JBossMessage doShallowCopy()
   {
      return new JBossMapMessage(this);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void writePayloadExternal(ObjectOutput out) throws IOException
   {
      writeMap(out, ((Map)payload));
   }

   protected Serializable readPayloadExternal(ObjectInput in)
      throws IOException, ClassNotFoundException
   {
      return (Serializable)readMap(in);
   }

   // Private -------------------------------------------------------

   /**
    * Check the name
    * 
    * @param name the name
    */
   private void checkName(String name)
   {
      if (name == null)
         throw new IllegalArgumentException("Name must not be null.");

      if (name.equals(""))
         throw new IllegalArgumentException("Name must not be an empty String.");
   }

   // Inner classes -------------------------------------------------

}

