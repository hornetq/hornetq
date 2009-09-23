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

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientSession;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/**
 * This class implements javax.jms.MapMessage
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * 
 * @version $Revision: 3412 $
 *
 * $Id: HornetQRAMapMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class HornetQMapMessage extends HornetQMessage implements MapMessage
{
   // Constants -----------------------------------------------------

   public static final byte TYPE = 5;

   // Attributes ----------------------------------------------------
   
   private TypedProperties map = new TypedProperties();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   public HornetQMapMessage()
   {
      super(HornetQMapMessage.TYPE);

      map = new TypedProperties();
   }
   /*
    * This constructor is used to construct messages prior to sending
    */
   public HornetQMapMessage(final ClientSession session)
   {
      super(HornetQMapMessage.TYPE, session);
      
      map = new TypedProperties();
   }
   
   public HornetQMapMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }

   /**
    * 
    * Constructor for a foreign MapMessage
    * @param foreign
    * @throws JMSException
    */
   public HornetQMapMessage(final MapMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, HornetQMapMessage.TYPE, session);     
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
      return HornetQMapMessage.TYPE;
   }
      
   // MapMessage implementation -------------------------------------

   public void setBoolean(final String name, final boolean value) throws JMSException
   {
      checkName(name);
      map.putBooleanProperty(new SimpleString(name), value);
   }

   public void setByte(final String name, final byte value) throws JMSException
   {
      checkName(name);
      map.putByteProperty(new SimpleString(name), value);
   }

   public void setShort(final String name, final short value) throws JMSException
   {
      checkName(name);
      map.putShortProperty(new SimpleString(name), value);
   }

   public void setChar(final String name, final char value) throws JMSException
   {
      checkName(name);
      map.putCharProperty(new SimpleString(name), value);
   }

   public void setInt(final String name, final int value) throws JMSException
   {
      checkName(name);
      map.putIntProperty(new SimpleString(name), value);
   }

   public void setLong(final String name, final long value) throws JMSException
   {
      checkName(name);
      map.putLongProperty(new SimpleString(name), value);
   }

   public void setFloat(final String name, final float value) throws JMSException
   {
      checkName(name);
      map.putFloatProperty(new SimpleString(name), value);
   }

   public void setDouble(final String name, final double value) throws JMSException
   {
      checkName(name);
      map.putDoubleProperty(new SimpleString(name), value);
   }

   public void setString(final String name, final String value) throws JMSException
   {
      checkName(name);
      map.putStringProperty(new SimpleString(name), value == null ? null : new SimpleString(value));
   }

   public void setBytes(final String name, final byte[] value) throws JMSException
   {
      checkName(name);
      map.putBytesProperty(new SimpleString(name), value);
   }

   public void setBytes(final String name, final byte[] value, final int offset, final int length) throws JMSException
   {
      checkName(name);
      if (offset + length > value.length)
      {
         throw new JMSException("Invalid offset/length");
      }
      byte[] newBytes = new byte[length];
      System.arraycopy(value, offset, newBytes, 0, length);
      map.putBytesProperty(new SimpleString(name), newBytes);
   }

   public void setObject(final String name, final Object value) throws JMSException
   {
      checkName(name);
      SimpleString key = new SimpleString(name);
      if (value instanceof Boolean)
         map.putBooleanProperty(key, (Boolean)value);
      else if (value instanceof Byte)
         map.putByteProperty(key, (Byte)value);
      else if (value instanceof Short)
         map.putShortProperty(key, (Short)value);
      else if (value instanceof Character)
         map.putCharProperty(key, (Character)value);
      else if (value instanceof Integer)
         map.putIntProperty(key, (Integer)value);
      else if (value instanceof Long)
         map.putLongProperty(key, (Long)value);
      else if (value instanceof Float)
         map.putFloatProperty(key, (Float)value);
      else if (value instanceof Double)
         map.putDoubleProperty(key, (Double)value);
      else if (value instanceof String)
         map.putStringProperty(key, new SimpleString((String)value));
      else if (value instanceof byte[])
         map.putBytesProperty(key, (byte[]) value);
      else
         throw new MessageFormatException("Invalid object type.");
   }

   public boolean getBoolean(final String name) throws JMSException
   {
      Object value = map.getProperty(new SimpleString(name));

      if (value == null)
         return Boolean.valueOf(null).booleanValue();

      if (value instanceof Boolean)
         return ((Boolean) value).booleanValue();
      else if (value instanceof SimpleString)
         return Boolean.valueOf(((SimpleString) value).toString()).booleanValue();
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public byte getByte(final String name) throws JMSException
   {
      Object value = map.getProperty(new SimpleString(name));

      if (value == null)
         return Byte.parseByte(null);

      if (value instanceof Byte)
         return ((Byte) value).byteValue();
      else if (value instanceof SimpleString)
         return Byte.parseByte(((SimpleString) value).toString());
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public short getShort(final String name) throws JMSException
   {
      Object value = map.getProperty(new SimpleString(name));

      if (value == null)
         return Short.parseShort(null);

      if (value instanceof Byte)
         return ((Byte) value).shortValue();
      else if (value instanceof Short)
         return ((Short) value).shortValue();
      else if (value instanceof SimpleString)
         return Short.parseShort(((SimpleString) value).toString());
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public char getChar(final String name) throws JMSException
   {
      Object value = map.getProperty(new SimpleString(name));

      if (value == null)
         throw new NullPointerException("Invalid conversion");

      if (value instanceof Character)
         return ((Character) value).charValue();
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public int getInt(final String name) throws JMSException
   {
      Object value = map.getProperty(new SimpleString(name));

      if (value == null)
         return Integer.parseInt(null);

      if (value instanceof Byte)
         return ((Byte) value).intValue();
      else if (value instanceof Short)
         return ((Short) value).intValue();
      else if (value instanceof Integer)
         return ((Integer) value).intValue();
      else if (value instanceof SimpleString)
         return Integer.parseInt(((SimpleString) value).toString());
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public long getLong(final String name) throws JMSException
   {
      Object value = map.getProperty(new SimpleString(name));

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
      else if (value instanceof SimpleString)
         return Long.parseLong(((SimpleString) value).toString());
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public float getFloat(final String name) throws JMSException
   {
      Object value = map.getProperty(new SimpleString(name));

      if (value == null)
         return Float.parseFloat(null);

      if (value instanceof Float)
         return ((Float) value).floatValue();
      else if (value instanceof SimpleString)
         return Float.parseFloat(((SimpleString) value).toString());
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public double getDouble(final String name) throws JMSException
   {
      Object value = map.getProperty(new SimpleString(name));

      if (value == null)
         return Double.parseDouble(null);

      if (value instanceof Float)
         return ((Float) value).doubleValue();
      else if (value instanceof Double)
         return ((Double) value).doubleValue();
      else if (value instanceof SimpleString)
         return Double.parseDouble(((SimpleString) value).toString());
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public String getString(final String name) throws JMSException
   {
      Object value = map.getProperty(new SimpleString(name));

      if (value == null)
         return null;

      if (value instanceof SimpleString)
      {
         return ((SimpleString) value).toString();
      }      
      else if (value instanceof Boolean)
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
      else
      {
         throw new MessageFormatException("Invalid conversion");
      }
   }

   public byte[] getBytes(final String name) throws JMSException
   {
      Object value = map.getProperty(new SimpleString(name));

      if (value == null)
         return null;
      if (value instanceof byte[])
         return (byte[]) value;
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public Object getObject(final String name) throws JMSException
   {
      Object val = map.getProperty(new SimpleString(name));
      
      if (val instanceof SimpleString)
      {
         val = ((SimpleString)val).toString();
      }
      
      return val;
   }

   public Enumeration getMapNames() throws JMSException
   {
      Set propNames = new HashSet<String>();
      
      for (SimpleString str: map.getPropertyNames())
      {
         propNames.add(str.toString());
      }
      
      return Collections.enumeration(propNames);
   }

   public boolean itemExists(final String name) throws JMSException
   {
      return map.containsProperty(new SimpleString(name));
   }

   // HornetQRAMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      super.clearBody();
      
      map.clear();
   }
   
   public void doBeforeSend() throws Exception
   {
      message.getBody().clear();
      map.encode(message.getBody());
      
      super.doBeforeSend();
   }
   
   public void doBeforeReceive() throws Exception
   {        
      super.doBeforeReceive();
      
      map.decode(message.getBody());
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
     
   // Private -------------------------------------------------------
   
   /**
    * Check the name
    * 
    * @param name the name
    */
   private void checkName(String name) throws JMSException
   {
      checkWrite();            
      
      if (name == null)
      {
         throw new IllegalArgumentException("Name must not be null.");
      }

      if (name.equals(""))
      {
         throw new IllegalArgumentException("Name must not be an empty String.");
      }
   }

   // Inner classes -------------------------------------------------

}

