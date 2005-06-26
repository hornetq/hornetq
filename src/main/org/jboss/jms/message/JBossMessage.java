/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.messaging.core.message.RoutableSupport;
import org.jboss.util.Primitives;
import org.jboss.util.Strings;

/**
 * 
 * Implementation of a JMS Message
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * 
 * Partially ported from JBossMQ implementation originally written by:
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author Hiram Chirino (Cojonudo14@hotmail.com)
 * @author David Maplesden (David.Maplesden@orion.co.nz)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 *
 * $Id$
 */
public class JBossMessage extends RoutableSupport implements javax.jms.Message
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 8341387096828690976L;

   // Static --------------------------------------------------------

   private static final HashSet reservedIdentifiers = new HashSet();
   static
   {
      reservedIdentifiers.add("NULL");
      reservedIdentifiers.add("TRUE");
      reservedIdentifiers.add("FALSE");
      reservedIdentifiers.add("NOT");
      reservedIdentifiers.add("AND");
      reservedIdentifiers.add("OR");
      reservedIdentifiers.add("BETWEEN");
      reservedIdentifiers.add("LIKE");
      reservedIdentifiers.add("IN");
      reservedIdentifiers.add("IS");
      reservedIdentifiers.add("ESCAPE");
   }

   // Attributes ----------------------------------------------------

   protected Destination destination;
   
   protected Destination replyToDestination;

   protected String type;

   // the delegate is set only on incoming messages, but we make it transient nonetheless
   protected transient SessionDelegate delegate;

   protected boolean messageReadWrite = true;

   protected Map properties; 
   
   protected boolean isCorrelationIDBytes;
   
   protected String correlationID;
   
   protected byte[] correlationIDBytes;
   
   protected String connectionID;

   protected int priority;

   // Constructors --------------------------------------------------

   public JBossMessage()
   {
      this((String) null);
   }

   public JBossMessage(String messageID)
   {
      super(messageID);
      reliable = false;
      redelivered = false;
      type = null;
      properties = new HashMap();
   }

   protected JBossMessage(JBossMessage other)
   {
      super(other);
      this.destination = other.destination;
      this.replyToDestination = other.replyToDestination;
      this.type = other.type;
      this.delegate = other.delegate;
      this.messageReadWrite = other.messageReadWrite;
      this.properties = new HashMap(other.properties);
      this.isCorrelationIDBytes = other.isCorrelationIDBytes;
      this.correlationID = other.correlationID;
      if (this.isCorrelationIDBytes)
      {
         this.correlationIDBytes = new byte[other.correlationIDBytes.length];
         System.arraycopy(other.correlationIDBytes, 0, this.correlationIDBytes, 0, other.correlationIDBytes.length);
      }
      this.connectionID = other.connectionID;
      this.priority = other.priority;
   }

   // javax.jmx.Message implementation ------------------------------

   public String getJMSMessageID() throws JMSException
   {
      return (String) messageID;
   }

   public void setJMSMessageID(String messageID) throws JMSException
   {
      this.messageID = messageID;
   }

   public long getJMSTimestamp() throws JMSException
   {
      return timestamp;
   }

   public void setJMSTimestamp(long timestamp) throws JMSException
   {
      this.timestamp = timestamp;
   }

   public byte[] getJMSCorrelationIDAsBytes() throws JMSException
   {
      if (!isCorrelationIDBytes)
      {
         throw new JMSException("CorrelationID is a String for this message");
      }
      return correlationIDBytes;
   }

   public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
   {
      if (correlationID == null || correlationID.length == 0)
      {
         throw new JMSException("Please specify a non-zero length byte[]");
      }
      correlationIDBytes = correlationID;
      isCorrelationIDBytes = true;
   }

   public void setJMSCorrelationID(String correlationID) throws JMSException
   {
      this.correlationID = correlationID;
      isCorrelationIDBytes = false;
   }

   public String getJMSCorrelationID() throws JMSException
   {
      if (isCorrelationIDBytes)
      {
         throw new JMSException("CorrelationID is a byte[] for this message");
      }
      return correlationID;
   }

   public Destination getJMSReplyTo() throws JMSException
   {
      return replyToDestination;
   }

   public void setJMSReplyTo(Destination replyTo) throws JMSException
   {
      this.replyToDestination = replyTo;
   }

   public Destination getJMSDestination() throws JMSException
   {
      return destination;
   }

   public void setJMSDestination(Destination destination) throws JMSException
   {
      this.destination = destination;
   }

   public int getJMSDeliveryMode() throws JMSException
   {
      return reliable ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
   }

   public void setJMSDeliveryMode(int deliveryMode) throws JMSException
   {
      if (deliveryMode == DeliveryMode.PERSISTENT)
      {
         reliable = true;
      }
      else if (deliveryMode == DeliveryMode.NON_PERSISTENT)
      {
         reliable = false;
      }
      else
      {
         throw new JBossJMSException("Delivery mode must be either DeliveryMode.PERSISTENT "
               + "or DeliveryMode.NON_PERSISTENT");
      }
   }

   public boolean getJMSRedelivered() throws JMSException
   {
      return isRedelivered();
   }

   public void setJMSRedelivered(boolean redelivered) throws JMSException
   {
      setRedelivered(redelivered);
   }

   /**
    * 
    * @return java.lang.String type
    * @throws JMSException
    */
   public String getJMSType() throws JMSException
   {
      return type;
   }

   /**
    * 
    * @param type
    * @throws JMSException
    */
   public void setJMSType(String type) throws JMSException
   {
      this.type = type;
   }

   public long getJMSExpiration() throws JMSException
   {
      return expiration;
   }

   public void setJMSExpiration(long expiration) throws JMSException
   {
      this.expiration = expiration;
   }

   public int getJMSPriority() throws JMSException
   {
      return priority;
   }

   public void setJMSPriority(int priority) throws JMSException
   {
      this.priority = priority;
   }

   public void clearProperties() throws JMSException
   {
      properties.clear();
   }

   public void acknowledge() throws JMSException
   {
      if (delegate != null)
      {
         delegate.acknowledgeSession();
      }
   }

   public void clearBody() throws JMSException
   {
      this.messageReadWrite = true;
   }

   public boolean propertyExists(String name) throws JMSException
   {
      return properties.containsKey(name);
   }

   public boolean getBooleanProperty(String name) throws JMSException
   {
      Object value = properties.get(name);
      if (value == null)
         return Boolean.valueOf(null).booleanValue();

      if (value instanceof Boolean)
         return ((Boolean) value).booleanValue();
      else if (value instanceof String)
         return Boolean.valueOf((String) value).booleanValue();
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public byte getByteProperty(String name) throws JMSException
   {
      Object value = properties.get(name);
      if (value == null)
         throw new NumberFormatException("Message property '" + name + "' not set.");

      if (value instanceof Byte)
         return ((Byte) value).byteValue();
      else if (value instanceof String)
         return Byte.parseByte((String) value);
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public short getShortProperty(String name) throws JMSException
   {
      Object value = properties.get(name);
      if (value == null)
         throw new NumberFormatException("Message property '" + name + "' not set.");

      if (value instanceof Byte)
         return ((Byte) value).shortValue();
      else if (value instanceof Short)
         return ((Short) value).shortValue();
      else if (value instanceof String)
         return Short.parseShort((String) value);
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public int getIntProperty(String name) throws JMSException
   {
      Object value = properties.get(name);
      if (value == null)
         throw new NumberFormatException("Message property '" + name + "' not set.");

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

   public long getLongProperty(String name) throws JMSException
   {
      Object value = properties.get(name);
      if (value == null)
         throw new NumberFormatException("Message property '" + name + "' not set.");

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

   public float getFloatProperty(String name) throws JMSException
   {
      Object value = properties.get(name);
      if (value == null)
         return Float.valueOf(null).floatValue();

      if (value instanceof Float)
         return ((Float) value).floatValue();
      else if (value instanceof String)
         return Float.parseFloat((String) value);
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public double getDoubleProperty(String name) throws JMSException
   {
      Object value = properties.get(name);
      if (value == null)
         return Double.valueOf(null).doubleValue();

      if (value instanceof Float)
         return ((Float) value).doubleValue();
      else if (value instanceof Double)
         return ((Double) value).doubleValue();
      else if (value instanceof String)
         return Double.parseDouble((String) value);
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public String getStringProperty(String name) throws JMSException
   {
      Object value = properties.get(name);
      if (value == null)
         return null;

      if (value instanceof Boolean)
         return ((Boolean) value).toString();
      else if (value instanceof Byte)
         return ((Byte) value).toString();
      else if (value instanceof Short)
         return ((Short) value).toString();
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

   public Object getObjectProperty(String name) throws JMSException
   {
      Object value = properties.get(name);
      return value;
   }

   public Enumeration getPropertyNames() throws JMSException
   {
      Enumeration names = Collections.enumeration(properties.keySet());
      return names;
   }

   public void setBooleanProperty(String name, boolean value) throws JMSException
   {
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Properties are read-only");
      Boolean b = Primitives.valueOf(value);
      checkProperty(name, b);
      properties.put(name, b);
   }

   public void setByteProperty(String name, byte value) throws JMSException
   {
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Properties are read-only");
      Byte b = new Byte(value);
      checkProperty(name, b);
      properties.put(name, b);
   }

   public void setShortProperty(String name, short value) throws JMSException
   {
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Properties are read-only");
      Short s = new Short(value);
      checkProperty(name, s);
      properties.put(name, s);
   }

   public void setIntProperty(String name, int value) throws JMSException
   {
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Properties are read-only");
      Integer i = new Integer(value);
      checkProperty(name, i);
      properties.put(name, i);
   }

   public void setLongProperty(String name, long value) throws JMSException
   {
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Properties are read-only");
      Long l = new Long(value);
      checkProperty(name, l);
      properties.put(name, l);
   }

   public void setFloatProperty(String name, float value) throws JMSException
   {
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Properties are read-only");
      Float f = new Float(value);
      checkProperty(name, f);
      properties.put(name, f);
   }

   public void setDoubleProperty(String name, double value) throws JMSException
   {
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Properties are read-only");
      Double d = new Double(value);
      checkProperty(name, d);
      properties.put(name, d);
   }

   public void setStringProperty(String name, String value) throws JMSException
   {
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Properties are read-only");
      checkProperty(name, value);
      this.properties.put(name, value);
   }

   public void setObjectProperty(String name, Object value) throws JMSException
   {
      if (!messageReadWrite)
         throw new MessageNotWriteableException("Properties are read-only");
      checkProperty(name, value);
      if (value instanceof Boolean)
         properties.put(name, value);
      else if (value instanceof Byte)
         properties.put(name, value);
      else if (value instanceof Short)
         properties.put(name, value);
      else if (value instanceof Integer)
         properties.put(name, value);
      else if (value instanceof Long)
         properties.put(name, value);
      else if (value instanceof Float)
         properties.put(name, value);
      else if (value instanceof Double)
         properties.put(name, value);
      else if (value instanceof String)
         properties.put(name, value);
      else if (value == null)
         properties.put(name, null);
      else
         throw new MessageFormatException("Invalid object type");
   }

   // Public --------------------------------------------------------

   public void setSessionDelegate(SessionDelegate sd)
   {
      this.delegate = sd;
   }

   public Map getJMSProperties()
   {
      return properties;
   }

   public void setPropertiesReadWrite(boolean readWrite)
   {
      this.messageReadWrite = readWrite;
   }

   
   public JBossMessage doClone() throws JMSException
   {
      return new JBossMessage(this);
   }
   
   
   public String getConnectionID()
   {
      return connectionID;
   }
   
   public void setConnectionID(String connectionID)
   {
      this.connectionID = connectionID;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("JBossMessage[");
      sb.append("ID=");
      sb.append(messageID);
      sb.append("]");
      return sb.toString();
   }

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
      super.writeExternal(out);

      out.writeObject(destination);
      out.writeObject(replyToDestination);
      writeString(out, type);
      out.writeBoolean(messageReadWrite); //do we really need to write this??

      writeMap(out, properties);
      
      out.writeBoolean(isCorrelationIDBytes);
      if (isCorrelationIDBytes)
      {
         if (correlationIDBytes == null)
         {
            out.writeInt(-1);
         }
         else
         {
            out.writeInt(correlationIDBytes.length);
            out.write(correlationIDBytes);
         }
      }
      else
      {
         writeString(out, correlationID);
      }
      writeString(out, connectionID);
      out.writeInt(priority);
   }



   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      super.readExternal(in);

      destination = (Destination) in.readObject();
      replyToDestination = (Destination) in.readObject();
      type = readString(in);
      messageReadWrite = in.readBoolean();
      properties = readMap(in);
      
      isCorrelationIDBytes = in.readBoolean();
      if (isCorrelationIDBytes)
      {
         int length = in.readInt();
         if (length == -1)
         {
            correlationIDBytes = null;
         }
         else
         {
            correlationIDBytes = new byte[length];
            in.readFully(correlationIDBytes);
         }
      }
      else
      {
         correlationID = readString(in);
      }
      connectionID = readString(in);
      priority = in.readInt();
   }

   // Package protected ---------------------------------------------

   /**
    * Check a property is valid
    * 
    * @param name the name
    * @param value the value
    * @throws JMSException for any error
    */
   void checkProperty(String name, Object value) throws JMSException
   {
      if (name == null)
         throw new IllegalArgumentException("The name of a property must not be null.");

      if (name.equals(""))
         throw new IllegalArgumentException("The name of a property must not be an empty String.");

      if (Strings.isValidJavaIdentifier(name) == false)
         throw new IllegalArgumentException("The property name '" + name + "' is not a valid java identifier.");

      if (reservedIdentifiers.contains(name))
         throw new IllegalArgumentException("The property name '" + name + "' is reserved due to selector syntax.");

      if (name.regionMatches(false, 0, "JMSX", 0, 4))
      {
         throw new JMSException("Illegal property name: " + name);
      }
     
      
   }

   // Protected -----------------------------------------------------

   // Inner classes -------------------------------------------------
}
