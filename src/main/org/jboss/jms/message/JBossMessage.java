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
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.impl.message.MessageSupport;
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
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:bershath@yahoo.com">Tyronne Wickramarathne</a>
 * 
 * Partially ported from JBossMQ implementation originally written by:
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author Hiram Chirino (Cojonudo14@hotmail.com)
 * @author David Maplesden (David.Maplesden@orion.co.nz)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 *
 * $Id$
 */
public class JBossMessage extends MessageSupport implements javax.jms.Message, Serializable
{
   // Constants -----------------------------------------------------
   private static final long serialVersionUID = 2833181306818971346L;

   public static final byte TYPE = 1;
   
   private static final char PROPERTY_PREFIX_CHAR = 'P';
   
   private static final String PROPERTY_PREFIX = "P";
   
   private static final String DESTINATION_HEADER_NAME = "H.DEST";
   
   private static final String REPLYTO_HEADER_NAME = "H.REPLYTO";
   
   private static final String CORRELATIONID_HEADER_NAME = "H.CORRELATIONID";

   // When the message is sent through the cluster, it needs to keep the original messageID
   private static final String JBM_MESSAGE_ID = "JBM_MESSAGE_ID";
   
   private static final String CORRELATIONIDBYTES_HEADER_NAME = "H.CORRELATIONIDBYTES";
   
   private static final String TYPE_HEADER_NAME = "H.TYPE";
   
   public static final String JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME = "JMS_JBOSS_SCHEDULED_DELIVERY";
   
   //Used when sending a message to the DLQ
   public static final String JBOSS_MESSAGING_ORIG_DESTINATION = "JBM_ORIG_DESTINATION";

   //Used when sending a message to the DLQ
   public static final String JBOSS_MESSAGING_ORIG_MESSAGE_ID = "JBM_ORIG_MESSAGE_ID";
   
   //Used when sending a mesage to the DLQ
   public static final String JBOSS_MESSAGING_ACTUAL_EXPIRY_TIME = "JBM_ACTUAL_EXPIRY";
   
   //Used when bridging a message
   public static final String JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST = "JBM_BRIDGE_MSG_ID_LIST";
   
   private static final Logger log = Logger.getLogger(JBossMessage.class);   
      
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
      
      int deliveryCount = 0;
      try
      {
         deliveryCount = m.getIntProperty("JMSXDeliveryCount");
      }
      catch (JMSException e)
      {
         log.error("Failed to get delivery count", e);
      }
                 
      try
      {
         sb.append("              redelivered:   ").append(deliveryCount >= 1).append('\n');
         sb.append("              priority:      ").append(m.priority).append('\n');
         sb.append("              deliveryCount: ").append(deliveryCount).append('\n');
   
         sb.append("              JMS ID:        ").append(m.getJMSMessageID()).append('\n');
         sb.append("              type:          ").append(type).append('\n');
         sb.append("              destination:   ").append(m.getJMSDestination()).append('\n');
         sb.append("              replyTo:       ").append(m.getJMSReplyTo()).append('\n');
         sb.append("              jmsType:       ").append(m.getJMSType()).append('\n');
         sb.append("              properties:    ");
      }
      catch (Exception e)
      {
         log.error("Failed to dump message", e);
      }

      Iterator iter = m.headers.entrySet().iterator();
      
      int count = 0;
      
      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry)iter.next();
         
         String propName = (String)entry.getKey();
         
         if (propName.charAt(0) == PROPERTY_PREFIX_CHAR)
         {
            if (count == 0)
            {
               sb.append("\n");
            }
         
            sb.append("                             ");
            sb.append(propName).append(" - ").append(entry.getValue()).append('\n');
            
            count++;
         }
      }
      
      sb.append("\n");

      return sb.toString();
   }

   // Attributes ----------------------------------------------------

   protected transient String connectionID;
   
   protected transient String jmsMessageID;
   
   //Optimisation - we could just store this as a header like everything else - but we store
   //As an attribute so we can prevent an extra lookup on the server
   private long scheduledDeliveryTime;
   
   //Optimisation - we could just store this as a header like everything else - but we store
   //As an attribute so we can prevent an extra lookup on the server
   private Destination destination;
   
   // Constructors --------------------------------------------------
 
   /*
    * Construct a message for deserialization or streaming
    */
   public JBossMessage()
   {     
   }
   
   /*
    * Construct a message using default values
    */
   public JBossMessage(long messageID)
   {
      super(messageID, true, 0, System.currentTimeMillis(), (byte)4, null, null);
   }
   
   /*
    * Constructor using specified values
    */
   public JBossMessage(long messageID, boolean reliable, long expiration, long timestamp,
                       byte priority, Map headers, byte[] payloadAsByteArray) 
   {
      super (messageID, reliable, expiration, timestamp, priority, headers, payloadAsByteArray);
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
      this.connectionID = other.connectionID;   
      this.scheduledDeliveryTime = other.scheduledDeliveryTime;
      this.destination = other.destination;
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
      
      setJMSReplyTo(foreign.getJMSReplyTo());
      setJMSDestination(foreign.getJMSDestination());
      setJMSDeliveryMode(foreign.getJMSDeliveryMode());
      setJMSExpiration(foreign.getJMSExpiration());
      setJMSPriority(foreign.getJMSPriority());
      setJMSType(foreign.getJMSType());

      for (Enumeration props = foreign.getPropertyNames(); props.hasMoreElements(); )
      {
         String name = (String)props.nextElement();
         
         Object prop = foreign.getObjectProperty(name);

         this.setObjectProperty(name, prop);                       
      }
   }
   

   // Routable implementation ---------------------------------------

   public boolean isReference()
   {
      return false;
   }

   // javax.jmx.Message implementation ------------------------------
   
   public String getJMSMessageID()
   {
      if (jmsMessageID == null)
      {
         String headerID = (String)headers.get(JBM_MESSAGE_ID);
         if (headerID == null)
         {
            jmsMessageID = "ID:JBM-" + messageID;
         }
         else
         {
            jmsMessageID = headerID;
         }
      }
      return jmsMessageID;
   }

   public void setJMSMessageID(String jmsMessageID) throws JMSException
   {
      if (jmsMessageID != null && !jmsMessageID.startsWith("ID:"))
      {
         throw new JMSException("JMSMessageID must start with ID:");
      }
      if (jmsMessageID == null)
      {
         headers.remove(JBM_MESSAGE_ID);
      }
      else
      {
         headers.put(JBM_MESSAGE_ID, jmsMessageID);
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
      return (byte[]) headers.get(CORRELATIONIDBYTES_HEADER_NAME);
   }

   public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
   {
      if (correlationID == null || correlationID.length == 0)
      {
         throw new JMSException("Please specify a non-zero length byte[]");
      }
      headers.put(CORRELATIONIDBYTES_HEADER_NAME, correlationID);
      
      headers.remove(CORRELATIONID_HEADER_NAME);
   }

   public void setJMSCorrelationID(String correlationID) throws JMSException
   {
      headers.put(CORRELATIONID_HEADER_NAME, correlationID);
      
      headers.remove(CORRELATIONIDBYTES_HEADER_NAME);
   }

   public String getJMSCorrelationID() throws JMSException
   {
      return (String)headers.get(CORRELATIONID_HEADER_NAME);
   }

   public Destination getJMSReplyTo() throws JMSException
   {
      return (Destination)headers.get(REPLYTO_HEADER_NAME);
   }

   public void setJMSReplyTo(Destination replyTo) throws JMSException
   {
      headers.put(REPLYTO_HEADER_NAME, replyTo);
   }

   public Destination getJMSDestination() throws JMSException
   {
      if (destination != null)
      {
         return destination;
      }
      else
      {
         return (Destination)headers.get(DESTINATION_HEADER_NAME);
      }
   }

   public void setJMSDestination(Destination destination) throws JMSException
   {
      //We don't store as a header when setting - this allows us to avoid a lookup on the server
      //when routing the message
      this.destination = destination; 
   }
   
   //We need to override getHeaders - so the JMSDestination header gets persisted to the db
   //This is called by the persistence manager
   public Map getHeaders()
   {
      if (destination != null)
      {
         headers.put(DESTINATION_HEADER_NAME, destination);
      }
      
      destination = null;
      
      return headers;
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
      throw new IllegalStateException("This should never be called directly");
   }

   public void setJMSRedelivered(boolean redelivered) throws JMSException
   {
      //Always dealt with on the proxy
      throw new IllegalStateException("This should never be called directly");
   }

   /**
    * 
    * @return java.lang.String type
    * @throws JMSException
    */
   public String getJMSType() throws JMSException
   {
      return (String)headers.get(TYPE_HEADER_NAME);
   }

   /**
    * 
    * @param type
    * @throws JMSException
    */
   public void setJMSType(String type) throws JMSException
   {
      headers.put(TYPE_HEADER_NAME, type);
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
      Iterator iter = headers.keySet().iterator();
      
      while (iter.hasNext())
      {
         String propName = (String)iter.next();
         
         if (propName.charAt(0) == PROPERTY_PREFIX_CHAR)
         {
            iter.remove();
         }
      }
   }

   public void clearBody() throws JMSException
   {
      this.setPayload(null);
      
      clearPayloadAsByteArray();
   }

   public boolean propertyExists(String name) throws JMSException
   {
      return headers.containsKey(PROPERTY_PREFIX + name)
             || name.equals("JMSXDeliveryCount");
   }

   public boolean getBooleanProperty(String name) throws JMSException
   {
      Object value = headers.get(PROPERTY_PREFIX + name);
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
      Object value = headers.get(PROPERTY_PREFIX + name);
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
      Object value = headers.get(PROPERTY_PREFIX + name);
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
      Object value = headers.get(PROPERTY_PREFIX + name);

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
      if (JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME.equals(name))
      {         
         if (this.scheduledDeliveryTime == 0)
         {
            throw new NumberFormatException("Message property '" + name + "' not set.");
         }
         else
         {
            return scheduledDeliveryTime;
         }         
      }
      
      Object value = headers.get(PROPERTY_PREFIX + name);

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
      Object value = headers.get(PROPERTY_PREFIX + name);
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
      Object value = headers.get(PROPERTY_PREFIX + name);
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
      if (JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME.equals(name))
      {         
         if (this.scheduledDeliveryTime == 0)
         {
            throw new NumberFormatException("Message property '" + name + "' not set.");
         }
         else
         {
            return String.valueOf(scheduledDeliveryTime);
         }         
      }
      
      Object value = headers.get(PROPERTY_PREFIX + name);
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
      if (JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME.equals(name))
      {         
         if (scheduledDeliveryTime == 0)
         {
            return null;
         }
         else
         {
            return new Long(scheduledDeliveryTime);
         }         
      }
      
      return headers.get(PROPERTY_PREFIX + name);
   }

   public Enumeration getPropertyNames() throws JMSException
   {
      HashSet set = new HashSet();
      
      Iterator iter = headers.keySet().iterator();
      
      while (iter.hasNext())
      {
         String propName = (String)iter.next();
         
         if (propName.charAt(0) == PROPERTY_PREFIX_CHAR)
         {
            String name = propName.substring(1);
            set.add(name);
         }
      }
      
      if (scheduledDeliveryTime != 0)
      {
         set.add(JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME);
      }
      
      return Collections.enumeration(set);
   }

   public void setBooleanProperty(String name, boolean value) throws JMSException
   {
      Boolean b = Primitives.valueOf(value);
      checkProperty(name, b);
      headers.put(PROPERTY_PREFIX + name, b);
   }

   public void setByteProperty(String name, byte value) throws JMSException
   {
      Byte b = new Byte(value);
      checkProperty(name, b);
      headers.put(PROPERTY_PREFIX + name, b);
   }

   public void setShortProperty(String name, short value) throws JMSException
   {
      Short s = new Short(value);
      checkProperty(name, s);
      headers.put(PROPERTY_PREFIX + name, s);
   }

   public void setIntProperty(String name, int value) throws JMSException
   {
      Integer i = new Integer(value);
      checkProperty(name, i);
      headers.put(PROPERTY_PREFIX + name, i);
   }

   public void setLongProperty(String name, long value) throws JMSException
   {
      // Optimisation - we don't actually store this as a header - but as an attribute      
      if (JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME.equals(name))
      {         
         this.scheduledDeliveryTime = value;
      }
      else
      {      
         Long l = new Long(value);
         checkProperty(name, l);
         headers.put(PROPERTY_PREFIX + name, l);      
      }      
   }

   public void setFloatProperty(String name, float value) throws JMSException
   {
      Float f = new Float(value);
      checkProperty(name, f);
      headers.put(PROPERTY_PREFIX + name, f);
   }

   public void setDoubleProperty(String name, double value) throws JMSException
   {
      Double d = new Double(value);
      checkProperty(name, d);
      headers.put(PROPERTY_PREFIX + name, d);
   }

   public void setStringProperty(String name, String value) throws JMSException
   {
      checkProperty(name, value);
      headers.put(PROPERTY_PREFIX + name, value);
   }

   public void setObjectProperty(String name, Object value) throws JMSException
   {
      if (JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME.equals(name) && value instanceof Long)
      {         
         this.scheduledDeliveryTime = ((Long)value).longValue();
         return;
      }
      
      checkProperty(name, value);

      if (value instanceof Boolean)
      {
         headers.put(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Byte)
      {
         headers.put(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Short)
      {
         headers.put(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Integer)
      {
         headers.put(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Long)
      {
         headers.put(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Float)
      {
         headers.put(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Double)
      {
         headers.put(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof String)
      {
         headers.put(PROPERTY_PREFIX + name, value);
      }
      else if (value == null)
      {
         headers.put(PROPERTY_PREFIX + name, null);
      }
      else
      {
         throw new MessageFormatException("Invalid object type");
      }
   }

   // Public --------------------------------------------------------
   
   public void doBeforeSend() throws JMSException
   {      
   }

   public byte getType()
   {
      return JBossMessage.TYPE;
   }   
   
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
   
   public JBossMessage doCopy() throws JMSException
   {
      return new JBossMessage(this);
   }
      
   public void acknowledge()
   {
      throw new IllegalStateException("Should not be handled here!");
   }
   
   public void setMessageId(long messageID)
   {
      this.messageID = messageID;
   }
   
   public long getScheduledDeliveryTime()
   {
      return scheduledDeliveryTime;
   }
        
   /* Only used for testing */
   public Map getJMSProperties()
   {      
      Map newHeaders = new HashMap();
      
      Iterator iter = headers.entrySet().iterator();
      
      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry)iter.next();
         
         String key = (String)entry.getKey();
         
         if (key.charAt(0) == PROPERTY_PREFIX_CHAR)
         {
            newHeaders.put(key, entry.getValue());
         }
      }
      
      return newHeaders;
   }
   
   //Only used for testing
   public boolean isCorrelationIDBytes()
   {
      return headers.get(CORRELATIONIDBYTES_HEADER_NAME) != null;
   }
  

   // Streamable implementation ---------------------------------
   
   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
                   
      out.writeLong(scheduledDeliveryTime);
      
      JBossDestination.writeDestination(out, destination);      
   }

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);
           
      scheduledDeliveryTime = in.readLong();
      
      destination = JBossDestination.readDestination(in);
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

      if (name.startsWith("JMSX") &&
         !name.equals("JMSXGroupID") &&
         !name.equals("JMSXGroupSeq") &&
         !name.equals("JMSXDeliveryCount"))
      {
         throw new JMSException("Can only set JMSXGroupId, JMSXGroupSeq, JMSXDeliveryCount");
      }           
   }

   // Protected -----------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
