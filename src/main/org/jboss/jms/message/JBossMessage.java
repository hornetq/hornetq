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
import java.util.HashSet;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.util.Primitives;
import org.jboss.util.Strings;

/**
 * 
 * Implementation of a JMS Message
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Partially ported from JBossMQ implementation originally written by:
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author Hiram Chirino (Cojonudo14@hotmail.com)
 * @author David Maplesden (David.Maplesden@orion.co.nz)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class JBossMessage extends MessageSupport implements javax.jms.Message
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 8341387096828690976L;
   
   public static final int TYPE = 0;

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

   public static MessageDelegate createThinDelegate(JBossMessage m, int deliveryCount)
   {
      MessageDelegate del = null;
      
      if (m instanceof BytesMessage)
      {
         del = new BytesMessageDelegate((JBossBytesMessage)m, deliveryCount);
      }
      else if (m instanceof MapMessage)
      {
         del = new MapMessageDelegate((JBossMapMessage)m, deliveryCount);
      }
      else if (m instanceof ObjectMessage)
      {
         del = new ObjectMessageDelegate((JBossObjectMessage)m, deliveryCount);
      }
      else if (m instanceof StreamMessage)
      {
         del = new StreamMessageDelegate((JBossStreamMessage)m, deliveryCount);
      }
      else if (m instanceof TextMessage)
      {
         del = new TextMessageDelegate((JBossTextMessage)m, deliveryCount);
      }      
      else if (m instanceof JBossMessage)
      {
         del = new MessageDelegate(m, deliveryCount);
      }
     
      return del;
   }

   // Attributes ----------------------------------------------------

   protected Destination destination;
   
   protected Destination replyToDestination;

   protected String jmsType;

   protected Map properties;
   
   protected String correlationID;
   
   protected byte[] correlationIDBytes;
   
   protected String connectionID;
   
   // Constructors --------------------------------------------------
 
   /**
    * Only deserialization should use this constructor directory
    */
   public JBossMessage()
   {     
   }
   
   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossMessage(String messageID)
   {
      this(messageID, true, 0, System.currentTimeMillis(), 4, 0,
           null, null, null, null, true, null, true, null, null, null);
   }

   /*
    * This constructor is used to construct messages when retrieved from persistence storage
    */
   public JBossMessage(String messageID,
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
      super(messageID, reliable, expiration, timestamp, priority, deliveryCount, 0, coreHeaders, payload);

      this.jmsType = jmsType;      

      if (correlationID instanceof byte[])
      {         
         correlationIDBytes = (byte[])correlationID;
         this.correlationID = null;
      }
      else
      {
         this.correlationID = (String)correlationID;
         this.correlationIDBytes = null;
      }

      if (destination != null)
      {
         if (destinationIsQueue)
         {
            this.destination = new JBossQueue(destination);
         }
         else
         {
            this.destination = new JBossTopic(destination);
         }
      }

      if (replyTo != null)
      {
         if (replyToIsQueue)
         {
            this.replyToDestination = new JBossQueue(replyTo);
         }
         else
         {
            this.replyToDestination = new JBossTopic(replyTo);
         }
      }

      if (jmsProperties == null)
      {
         properties = new HashMap();
      }
      else
      {
         properties = new HashMap(jmsProperties);
      }

      this.connectionID = connectionID;
   }

   /**
    * 
    * Create a new JBossMessage by making a shallow copy of another
    * 
    * @param other The message to make a shallow copy from
    */
   protected JBossMessage(JBossMessage other)
   {
      super(other);
      this.destination = other.destination;
      this.replyToDestination = other.replyToDestination;
      this.jmsType = other.jmsType;      
      this.properties = other.properties;     
      this.correlationID = other.correlationID;
      this.correlationIDBytes = other.correlationIDBytes;
      this.connectionID = other.connectionID;      
   }

   /**
    * A copy constructor for non-JBoss Messaging JMS messages.
    */   
   public JBossMessage(Message foreign) throws JMSException
   {
      super(foreign.getJMSMessageID());

      setJMSTimestamp(foreign.getJMSTimestamp());

      try
      {
         byte[] corrIDBytes = foreign.getJMSCorrelationIDAsBytes();
         setJMSCorrelationIDAsBytes(corrIDBytes);
      }
      catch(JMSException e)
      {
         // specified as String
         String corrIDString = foreign.getJMSCorrelationID();
         if (corrIDString != null)
         {
            setJMSCorrelationID(corrIDString);
         }
      }
      setJMSReplyTo(foreign.getJMSReplyTo());
      setJMSDestination(foreign.getJMSDestination());
      setJMSDeliveryMode(foreign.getJMSDeliveryMode());
      setJMSRedelivered(foreign.getJMSRedelivered());
      setJMSExpiration(foreign.getJMSExpiration());
      setJMSPriority(foreign.getJMSPriority());

      if (properties == null)
      {
         properties = new HashMap();
      }

      for(Enumeration props = foreign.getPropertyNames(); props.hasMoreElements(); )
      {
         String name = (String)props.nextElement();
         
         Object prop = foreign.getObjectProperty(name);
         if ("JMSXDeliveryCount".equals(name))
         {
            deliveryCount = foreign.getIntProperty("JMSXDeliveryCount");
         }
         else
         {
            this.setObjectProperty(name, prop);
         }                
      }
   }
   

   // Routable implementation ---------------------------------------

   public boolean isReference()
   {
      return false;
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
      if (this.correlationIDBytes == null)
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
      correlationID = null;
   }

   public void setJMSCorrelationID(String correlationID) throws JMSException
   {
      this.correlationID = correlationID;
      correlationIDBytes = null;
   }

   public String getJMSCorrelationID() throws JMSException
   {
      if (this.correlationIDBytes != null)
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
      return jmsType;
   }

   /**
    * 
    * @param type
    * @throws JMSException
    */
   public void setJMSType(String type) throws JMSException
   {
      this.jmsType = type;
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

   public void clearBody() throws JMSException
   {
      this.payload = null;
   }

   public boolean propertyExists(String name) throws JMSException
   {
      return properties.containsKey(name) || "JMSXDeliveryCount".equals(name);
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
      if ("JMSXDeliveryCount".equals(name))
      {
         return deliveryCount;
      }
      
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
      HashSet set = new HashSet();
      set.addAll(properties.keySet());
      set.add("JMSXDeliveryCount");
      Enumeration names = Collections.enumeration(set);
      return names;
   }

   public void setBooleanProperty(String name, boolean value) throws JMSException
   {
      Boolean b = Primitives.valueOf(value);
      checkProperty(name, b);
      properties.put(name, b);
   }

   public void setByteProperty(String name, byte value) throws JMSException
   {
      Byte b = new Byte(value);
      checkProperty(name, b);
      properties.put(name, b);
   }

   public void setShortProperty(String name, short value) throws JMSException
   {
      Short s = new Short(value);
      checkProperty(name, s);
      properties.put(name, s);
   }

   public void setIntProperty(String name, int value) throws JMSException
   {
      Integer i = new Integer(value);
      checkProperty(name, i);
      properties.put(name, i);
   }

   public void setLongProperty(String name, long value) throws JMSException
   {
      Long l = new Long(value);
      checkProperty(name, l);
      properties.put(name, l);
   }

   public void setFloatProperty(String name, float value) throws JMSException
   {
      Float f = new Float(value);
      checkProperty(name, f);
      properties.put(name, f);
   }

   public void setDoubleProperty(String name, double value) throws JMSException
   {
      Double d = new Double(value);
      checkProperty(name, d);
      properties.put(name, d);
   }

   public void setStringProperty(String name, String value) throws JMSException
   {
      checkProperty(name, value);
      this.properties.put(name, value);
   }

   public void setObjectProperty(String name, Object value) throws JMSException
   {
      checkProperty(name, value);

      if (value instanceof Boolean)
      {
         properties.put(name, value);
      }
      else if (value instanceof Byte)
      {
         properties.put(name, value);
      }
      else if (value instanceof Short)
      {
         properties.put(name, value);
      }
      else if (value instanceof Integer)
      {
         properties.put(name, value);
      }
      else if (value instanceof Long)
      {
         properties.put(name, value);
      }
      else if (value instanceof Float)
      {
         properties.put(name, value);
      }
      else if (value instanceof Double)
      {
         properties.put(name, value);
      }
      else if (value instanceof String)
      {
         properties.put(name, value);
      }
      else if (value == null)
      {
         properties.put(name, null);
      }
      else
      {
         throw new MessageFormatException("Invalid object type");
      }
   }

   // Public --------------------------------------------------------
   
   public void doAfterSend() throws JMSException
   {      
   }

   public int getType()
   {
      return JBossMessage.TYPE;
   }   

   /**
    * @return a reference of the internal JMS property map.
    */
   public Map getJMSProperties()
   {
      return properties;
   }
   
   public void setJMSProperties(Map props)
   {
      this.properties = props;
   }
   
//   public void copyProperties(JBossMessage other)
//   {
//      this.properties = new HashMap(other.properties);
//   }
   
   public void copyPayload(Object payload) throws JMSException
   {
      
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
      sb.append("");
      sb.append(messageID);
      sb.append("]:");
      sb.append(isReliable() ? "PERSISTENT" : "NON-PERSISTENT");
      return sb.toString();
   }
   
   public JBossMessage doShallowCopy() throws JMSException
   {
      return new JBossMessage(this);
   }
   
   public boolean isCorrelationIDBytes()
   {
      return this.correlationIDBytes != null;
   }
   
   public void acknowledge()
   {
      //do nothing - handled in thin delegate
   }

   // org.jboss.messaging.core.Message implementation ---------------

   public Serializable getPayload()
   {
      return payload;
   }

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
      super.writeExternal(out);

      out.writeObject(destination);
      out.writeObject(replyToDestination);
      writeString(out, jmsType);
      writeMap(out, properties);
            
      if (correlationIDBytes == null)
      {
         out.writeInt(-1);
      }
      else
      {
         out.writeInt(correlationIDBytes.length);
         out.write(correlationIDBytes);
      }
   
      writeString(out, correlationID);
      
      writeString(out, connectionID);
   }



   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      super.readExternal(in);

      destination = (Destination) in.readObject();
      replyToDestination = (Destination) in.readObject();
      jmsType = readString(in);     
      properties = readMap(in);
      
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
      
      correlationID = readString(in);
         
      connectionID = readString(in);
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

      if (name.regionMatches(false, 0, "JMSX", 0, 4) &&
            !name.equals("JMSXGroupID") && !name.equals("JMSXGroupSeq"))
      {
         throw new JMSException("Can only set JMSXGroupId, JMSXGroupSeq");
      }           
   }
   


   // Protected -----------------------------------------------------

   // Inner classes -------------------------------------------------
}
