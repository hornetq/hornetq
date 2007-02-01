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
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTemporaryQueue;
import org.jboss.jms.destination.JBossTemporaryTopic;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.util.MessagingJMSException;
import org.jboss.messaging.core.message.MessageSupport;
import org.jboss.messaging.util.StreamUtils;
import org.jboss.util.Primitives;
import org.jboss.util.Strings;

/**
 * 
 * Implementation of a JMS Message
 * 
 * Note that the only reason this class is Serializable is so that messages
 * can be returned from JMX operations.
 * 
 * Java serialization is not used to serialize messages between client and server
 * in normal JMS operations
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
public class JBossMessage extends MessageSupport implements javax.jms.Message, Serializable
{
   // Constants -----------------------------------------------------
   private static final long serialVersionUID = 2833181306818971346L;

   public static final byte TYPE = 0;
   
   private static final byte NULL = 0;
   
   private static final byte STRING = 1;
   
   private static final byte BYTES = 2;
   
   private static final String JMSX_DELIVERY_COUNT_PROP_NAME = "JMSXDeliveryCount";   
   
   public static final String JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME = "JMS_JBOSS_SCHEDULED_DELIVERY";
   
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

   public static MessageProxy createThinDelegate(long deliveryId, JBossMessage m, int deliveryCount)
   {
      MessageProxy del;
      
      if (m instanceof BytesMessage)
      {
         del = new BytesMessageProxy(deliveryId, (JBossBytesMessage)m, deliveryCount);
      }
      else if (m instanceof MapMessage)
      {
         del = new MapMessageProxy(deliveryId, (JBossMapMessage)m, deliveryCount);
      }
      else if (m instanceof ObjectMessage)
      {
         del = new ObjectMessageProxy(deliveryId, (JBossObjectMessage)m, deliveryCount);
      }
      else if (m instanceof StreamMessage)
      {
         del = new StreamMessageProxy(deliveryId, (JBossStreamMessage)m, deliveryCount);
      }
      else if (m instanceof TextMessage)
      {
         del = new TextMessageProxy(deliveryId, (JBossTextMessage)m, deliveryCount);
      }      
      else
      {
         del = new MessageProxy(deliveryId, m, deliveryCount);
      }

      return del;
   }

   public static String dump(JBossMessage m)
   {
      String type = null;
      if (m instanceof BytesMessage)
      {
         type = "Bytes";
      }
      else if (m instanceof MapMessage)
      {
         type = "Map";
      }
      else if (m instanceof ObjectMessage)
      {
         type = "Object";
      }
      else if (m instanceof StreamMessage)
      {
         type = "Stream";
      }
      else if (m instanceof TextMessage)
      {
         type = "Text";
      }
      else
      {
         type = "Generic";
      }

      StringBuffer sb = new StringBuffer();

      sb.append("\n");
      sb.append("         MESSAGE DUMP\n");
      sb.append("              Core ID:       ").append(m.messageID).append('\n');
      sb.append("              reliable:      ").append(m.reliable).append('\n');
      sb.append("              expiration:    ").append(m.expiration).append('\n');
      sb.append("              timestamp:     ").append(m.timestamp).append('\n');
      sb.append("              headers:       ");

      if (m.headers.size() == 0)
      {
         sb.append("NO HEADERS").append('\n');
      }
      else
      {
         sb.append('\n');
         for(Iterator i = m.headers.keySet().iterator(); i.hasNext(); )
         {
            String name = (String)i.next();
            sb.append("                             ");
            sb.append(name).append(" - ").append(m.headers.get(name)).append('\n');
         }
      }
      sb.append("              redelivered:   ").append(m.deliveryCount >= 1).append('\n');
      sb.append("              priority:      ").append(m.priority).append('\n');
      sb.append("              deliveryCount: ").append(m.deliveryCount).append('\n');

      sb.append("              JMS ID:        ").append(m.getJMSMessageID()).append('\n');
      sb.append("              type:          ").append(type).append('\n');
      sb.append("              destination:   ").append(m.destination).append('\n');
      sb.append("              replyTo:       ").append(m.replyToDestination).append('\n');
      sb.append("              jmsType:       ").append(m.jmsType).append('\n');
      sb.append("              properties:    ");

      if (m.properties.size() == 0)
      {
         sb.append("NO PROPERTIES").append('\n');
      }
      else
      {
         sb.append('\n');
         for(Iterator i = m.properties.keySet().iterator(); i.hasNext(); )
         {
            String name = (String)i.next();
            sb.append("                             ");
            sb.append(name).append(" - ").append(m.properties.get(name)).append('\n');
         }
      }
      sb.append("\n");

      return sb.toString();
   }

   // Attributes ----------------------------------------------------

   protected JBossDestination destination;
   protected JBossDestination replyToDestination;
   protected String jmsType;
   protected Map properties;
   protected String correlationID;
   protected byte[] correlationIDBytes;
   protected transient int connectionID;
   
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
   public JBossMessage(long messageID)
   {
      this(messageID, true, 0, System.currentTimeMillis(), (byte)4,
           null, null, null, null, null, null, null, null);
   }

   public JBossMessage(long messageID,
                       boolean reliable,
                       long expiration,
                       long timestamp,
                       byte priority,    
                       Map coreHeaders,
                       byte[] payloadAsByteArray,
                       String jmsType,
                       String correlationID,
                       byte[] correlationIDBytes,
                       JBossDestination destination,
                       JBossDestination replyTo,
                       HashMap jmsProperties)
   {
      super(messageID,
            reliable,
            expiration,
            timestamp,
            priority,
            0, 0,
            coreHeaders,
            payloadAsByteArray);

      this.jmsType = jmsType;      
      this.correlationID = correlationID;
      this.correlationIDBytes = correlationIDBytes;
      this.connectionID = Integer.MIN_VALUE;
      this.destination = destination;
      this.replyToDestination = replyTo;

      if (jmsProperties == null)
      {
         properties = new HashMap();
      }
      else
      {
         properties = jmsProperties;
      }
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
   public JBossMessage(Message foreign, long messageID) throws JMSException
   {
      super(messageID);

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
      if (foreign.getJMSReplyTo() instanceof JBossDestination)
      {
         setJMSReplyTo(foreign.getJMSReplyTo());
      }
      if (foreign.getJMSDestination() instanceof JBossDestination)
      {
         setJMSDestination(foreign.getJMSDestination());
      }
      setJMSDeliveryMode(foreign.getJMSDeliveryMode());
      setDeliveryCount(foreign.getJMSRedelivered() ? 1 : 0);
      setJMSExpiration(foreign.getJMSExpiration());
      setJMSPriority(foreign.getJMSPriority());
      setJMSType(foreign.getJMSType());

      if (properties == null)
      {
         properties = new HashMap();
      }

      for (Enumeration props = foreign.getPropertyNames(); props.hasMoreElements(); )
      {
         String name = (String)props.nextElement();
         
         Object prop = foreign.getObjectProperty(name);
         if (JMSX_DELIVERY_COUNT_PROP_NAME.equals(name))
         {
            deliveryCount = foreign.getIntProperty(JMSX_DELIVERY_COUNT_PROP_NAME);
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
   
   protected transient String jmsMessageID;

   public String getJMSMessageID()
   {
      if (jmsMessageID == null)
      {       
         jmsMessageID = "ID:JBM-" + messageID;
      }
      return jmsMessageID;
   }

   public void setJMSMessageID(String jmsMessageID) throws JMSException
   {
      if (jmsMessageID != null && !jmsMessageID.startsWith("ID:"))
      {
         throw new JMSException("JMSMessageID must start with ID:");
      }
      this.jmsMessageID = jmsMessageID;
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
      return correlationID;
   }

   public Destination getJMSReplyTo() throws JMSException
   {
      return replyToDestination;
   }

   public void setJMSReplyTo(Destination replyTo) throws JMSException
   {
      if (!(replyTo instanceof JBossDestination))
      {
         throw new InvalidDestinationException("Replyto cannot be foreign");
      }
      this.replyToDestination = (JBossDestination)replyTo;
   }

   public Destination getJMSDestination() throws JMSException
   {
      return destination;
   }

   public void setJMSDestination(Destination destination) throws JMSException
   {
      if (!(destination instanceof JBossDestination))
      {
         throw new InvalidDestinationException("Destination cannot be foreign");
      }
      this.destination = (JBossDestination)destination;
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
         throw new MessagingJMSException("Delivery mode must be either DeliveryMode.PERSISTENT "
               + "or DeliveryMode.NON_PERSISTENT");
      }
   }

   public boolean getJMSRedelivered() throws JMSException
   {
      return deliveryCount >= 2;
   }

   public void setJMSRedelivered(boolean redelivered) throws JMSException
   {
      if (deliveryCount == 1)
      {
         deliveryCount++;
      }
      else
      {
         //do nothing
      }
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
      this.priority = (byte)priority;
   }

   public void clearProperties() throws JMSException
   {
      properties.clear();
   }

   public void clearBody() throws JMSException
   {
      this.setPayload(null);
      clearPayloadAsByteArray();
   }

   public boolean propertyExists(String name) throws JMSException
   {
      return properties.containsKey(name) || JMSX_DELIVERY_COUNT_PROP_NAME.equals(name) ||
      (this.scheduledDeliveryTime != 0 && JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME.equals(name));
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
      if (JMSX_DELIVERY_COUNT_PROP_NAME.equals(name))
      {
         return deliveryCount;
      }
               
      Object value = properties.get(name);

      if (value == null)
      {
         throw new NumberFormatException("Message property '" + name + "' not set.");
      }

      if (value instanceof Byte)
      {
         return ((Byte) value).intValue();
      }
      else if (value instanceof Short)
      {
         return ((Short) value).intValue();
      }
      else if (value instanceof Integer)
      {
         return ((Integer) value).intValue();
      }
      else if (value instanceof String)
      {
         return Integer.parseInt((String) value);
      }
      else
      {
         throw new MessageFormatException("Invalid conversion");
      }
   }

   public long getLongProperty(String name) throws JMSException
   {
      if (JMSX_DELIVERY_COUNT_PROP_NAME.equals(name))
      {
         return (long)deliveryCount;
      }
      else if (JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME.equals(name) && scheduledDeliveryTime > 0)
      {
         return scheduledDeliveryTime;
      }

      Object value = properties.get(name);

      if (value == null)
      {
         throw new NumberFormatException("Message property '" + name + "' not set.");
      }

      if (value instanceof Byte)
      {
         return ((Byte) value).longValue();
      }
      else if (value instanceof Short)
      {
         return ((Short) value).longValue();
      }
      else if (value instanceof Integer)
      {
         return ((Integer) value).longValue();
      }
      else if (value instanceof Long)
      {
         return ((Long) value).longValue();
      }
      else if (value instanceof String)
      {
         return Long.parseLong((String) value);
      }
      else
      {
         throw new MessageFormatException("Invalid conversion");
      }
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
      if (JMSX_DELIVERY_COUNT_PROP_NAME.equals(name))
      {
         return String.valueOf(deliveryCount);
      }
      else if (JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME.equals(name) && scheduledDeliveryTime > 0)
      {
         return String.valueOf(scheduledDeliveryTime);
      }

      Object value = properties.get(name);
      if (value == null)
         return null;

      if (value instanceof Boolean)
      {
         return value.toString();
      }
      else if (value instanceof Byte)
      {
         return value.toString();
      }
      else if (value instanceof Short)
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

   public Object getObjectProperty(String name) throws JMSException                                                              
   {
      return properties.get(name);
   }

   public Enumeration getPropertyNames() throws JMSException
   {
      HashSet set = new HashSet();
      set.addAll(properties.keySet());
      set.add(JMSX_DELIVERY_COUNT_PROP_NAME);
      if (this.scheduledDeliveryTime > 0)
      {
         set.add(JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME);
      }
      return Collections.enumeration(set);
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
      if (JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME.equals(name))
      {
         this.scheduledDeliveryTime = value;
      }
      else
      {
         Long l = new Long(value);
         checkProperty(name, l);
         properties.put(name, l);
      }
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

   public byte getType()
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
   
   public void copyPayload(Object payload) throws JMSException
   {      
   }
   
   public int getConnectionID()
   {
      return connectionID;
   }
   
   public void setConnectionID(int connectionID)
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
   
   public void setMessageId(long messageID)
   {
      this.messageID = messageID;
   }

   // Streamable implementation ---------------------------------
   
   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
      
      JBossDestination.writeDestination(out, destination);
      
      JBossDestination.writeDestination(out, replyToDestination);
      
      if (jmsType == null)
      {
         out.writeByte(NULL);
      }
      else
      {
         out.writeByte(STRING);
         out.writeUTF(jmsType);
      }
            
      StreamUtils.writeMap(out, properties, true);
      
      if (correlationID == null && correlationIDBytes == null)
      {
         out.writeByte(NULL);
      }
      else if (correlationIDBytes == null)
      {
         //String correlation id
         out.writeByte(STRING);
         
         out.writeUTF(correlationID);
      }
      else
      {
         //Bytes correlation id
         out.writeByte(BYTES);
         
         out.writeInt(correlationIDBytes.length);  
         
         out.write(correlationIDBytes);
      }            
   }

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);

      destination = JBossDestination.readDestination(in);

      replyToDestination = JBossDestination.readDestination(in);
 
      byte b = in.readByte();
      if (b == NULL)
      {
         jmsType =  null;
      }
      else
      {
         jmsType = in.readUTF();
      }
      
      Map m = StreamUtils.readMap(in, true);
      if (!(m instanceof HashMap))
      {
         properties =  new HashMap(m);
      }
      else
      {
         properties = (HashMap)m;
      }

      // correlation id
      
      b = in.readByte();
      
      if (b == NULL)
      {
         // No correlation id
         correlationID = null;
         
         correlationIDBytes = null;
      }
      else if (b == STRING)
      {
         // String correlation id
         correlationID = in.readUTF();
         
         correlationIDBytes = null;
      }
      else
      {
         // Bytes correlation id
         correlationID = null;
         
         int len = in.readInt();
         
         correlationIDBytes = new byte[len];
         
         in.readFully(correlationIDBytes);
      }
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
      {
         throw new IllegalArgumentException("The name of a property must not be null.");
      }

      if (name.equals(""))
      {
         throw new IllegalArgumentException("The name of a property must not be an empty String.");
      }

      if (!Strings.isValidJavaIdentifier(name))
      {
         throw new IllegalArgumentException("The property name '" + name +
                                            "' is not a valid java identifier.");
      }

      if (reservedIdentifiers.contains(name))
      {
         throw new IllegalArgumentException("The property name '" + name +
                                            "' is reserved due to selector syntax.");
      }

      if (name.regionMatches(false, 0, "JMSX", 0, 4) &&
         !name.equals("JMSXGroupID") && !name.equals("JMSXGroupSeq"))
      {
         throw new JMSException("Can only set JMSXGroupId, JMSXGroupSeq");
      }           
   }

   // Protected -----------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
