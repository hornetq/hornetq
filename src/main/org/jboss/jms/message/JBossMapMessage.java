/*
 * JBossMQ, the OpenSource JMS implementation
 * 
 * Distributable under LGPL license. See terms of license at gnu.org.
 */
package org.jboss.jms.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.jboss.logging.Logger;
import org.jboss.util.Primitives;

/**
 * This class implements javax.jms.MapMessage
 * 
 * It is largely ported from SpyMapMessage in JBossMQ.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * 
 * @version $Revision$
 */
public class JBossMapMessage extends JBossMessage implements MapMessage
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -8018832209056373908L;

   private static final Logger log = Logger.getLogger(JBossMapMessage.class);

   // Attributes ----------------------------------------------------

   protected Map content;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JBossMapMessage()
   {
      content = new HashMap();
   }

   // Public --------------------------------------------------------

   // MapMessage implementation -------------------------------------

   public void setBoolean(String name, boolean value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      content.put(name, Primitives.valueOf(value));

   }

   public void setByte(String name, byte value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      content.put(name, new Byte(value));

   }

   public void setShort(String name, short value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      content.put(name, new Short(value));

   }

   public void setChar(String name, char value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      content.put(name, new Character(value));

   }

   public void setInt(String name, int value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      content.put(name, new Integer(value));

   }

   public void setLong(String name, long value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      content.put(name, new Long(value));

   }

   public void setFloat(String name, float value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      content.put(name, new Float(value));

   }

   public void setDouble(String name, double value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      content.put(name, new Double(value));

   }

   public void setString(String name, String value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      content.put(name, value);

   }

   public void setBytes(String name, byte[] value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      content.put(name, value.clone());

   }

   public void setBytes(String name, byte[] value, int offset, int length) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      if (offset + length > value.length)
         throw new JMSException("Array is too small");
      byte[] temp = new byte[length];
      for (int i = 0; i < length; i++)
         temp[i] = value[i + offset];

      content.put(name, temp);

   }

   public void setObject(String name, Object value) throws JMSException
   {
      checkName(name);
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Message is ReadOnly !");

      if (value instanceof Boolean)
         content.put(name, value);
      else if (value instanceof Byte)
         content.put(name, value);
      else if (value instanceof Short)
         content.put(name, value);
      else if (value instanceof Character)
         content.put(name, value);
      else if (value instanceof Integer)
         content.put(name, value);
      else if (value instanceof Long)
         content.put(name, value);
      else if (value instanceof Float)
         content.put(name, value);
      else if (value instanceof Double)
         content.put(name, value);
      else if (value instanceof String)
         content.put(name, value);
      else if (value instanceof byte[])
         content.put(name, ((byte[]) value).clone());
      else
         throw new MessageFormatException("Invalid object type.");

   }

   public boolean getBoolean(String name) throws JMSException
   {
      Object value;

      value = content.get(name);

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

      value = content.get(name);

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

      value = content.get(name);

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

      value = content.get(name);

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

      value = content.get(name);

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

      value = content.get(name);

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

      value = content.get(name);

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

      value = content.get(name);

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

      value = content.get(name);

      if (value == null)
         return null;

      if (value instanceof Boolean)
         return ((Boolean) value).toString();
      else if (value instanceof Byte)
         return ((Byte) value).toString();
      else if (value instanceof Short)
         return ((Short) value).toString();
      else if (value instanceof Character)
         return ((Character) value).toString();
      else if (value instanceof Integer)
         return ((Integer) value).toString();
      else if (value instanceof Long)
         return ((Long) value).toString();
      else if (value instanceof Float)
         return ((Float) value).toString();
      else if (value instanceof Double)
         return ((Double) value).toString();
      else if (value instanceof String)
         return (String) value;
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public byte[] getBytes(String name) throws JMSException
   {
      Object value;

      value = content.get(name);

      if (value == null)
         return null;
      if (value instanceof byte[])
         return (byte[]) value;
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public Object getObject(String name) throws JMSException
   {

      return content.get(name);

   }

   public Enumeration getMapNames() throws JMSException
   {

      return Collections.enumeration(new HashMap(content).keySet());

   }

   public boolean itemExists(String name) throws JMSException
   {

      return content.containsKey(name);

   }

   // JBossMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      content = new HashMap();
      super.clearBody();
   }

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
      super.writeExternal(out);
      writeMap(out, content);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      super.readExternal(in);

      content = readMap(in);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

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

