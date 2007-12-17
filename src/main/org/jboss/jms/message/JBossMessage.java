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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.newcore.impl.MessageImpl;
import org.jboss.util.Primitives;
import org.jboss.util.Strings;

/**
 * 
 * Implementation of a JMS Message
 * 
 * JMS Messages only live on the client side - the server only deals with MessageImpl instances
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
 * $Id: JBossMessage.java 3466 2007-12-10 18:44:52Z timfox $
 */
public class JBossMessage implements javax.jms.Message
{
   // Constants -----------------------------------------------------

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
   
   protected static final byte NULL = 0;
   
   protected static final byte NOT_NULL = 1;
   
   private static final int TYPE = 0;
   
   // Static --------------------------------------------------------

   private static final HashSet<String> reservedIdentifiers = new HashSet<String>();
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
      
   private static final Logger log = Logger.getLogger(JBossMessage.class);

   
   public static JBossMessage createMessage(org.jboss.messaging.newcore.Message message,
                                            long deliveryID, int deliveryCount)
   {
      int type = message.getType();
      
      switch(type)
      {
         case JBossMessage.TYPE:
            return new JBossMessage(message, deliveryID, deliveryCount);
         case JBossBytesMessage.TYPE:
            return new JBossBytesMessage(message, deliveryID, deliveryCount);
         case JBossMapMessage.TYPE:
            return new JBossMapMessage(message, deliveryID, deliveryCount);
         case JBossObjectMessage.TYPE:
            return new JBossObjectMessage(message, deliveryID, deliveryCount);
         case JBossStreamMessage.TYPE:
            return new JBossStreamMessage(message, deliveryID, deliveryCount);
         case JBossTextMessage.TYPE:
            return new JBossTextMessage(message, deliveryID, deliveryCount);
         default:
            throw new IllegalArgumentException("Invalid message type " + type);
      }
   }
   
   // Attributes ----------------------------------------------------

   //The underlying message
   protected org.jboss.messaging.newcore.Message message;
   
   //The SessionDelegate - we need this when acknowledging the message directly
   private SessionDelegate delegate;
   
   //From a connection consumer?   
   private boolean cc;
   
   //The delivery count
   private int deliveryCount;
   
   //The delivery id
   private long deliveryID;
   
   //Read-only?
   protected boolean readOnly;
      
   // Constructors --------------------------------------------------
     
   protected JBossMessage(int type)
   {
      message =
         new MessageImpl(-1, type, true, 0, System.currentTimeMillis(), (byte)4);
   }
   
   public JBossMessage()
   {
      this (JBossMessage.TYPE);
   }
   
   /**
    * Constructor for when receiving a message from the server
    */
   public JBossMessage(org.jboss.messaging.newcore.Message message, long deliveryID, int deliveryCount)
   {
      this.message = message;
      
      this.deliveryID = deliveryID;
      
      this.deliveryCount = deliveryCount;
      
      this.readOnly = true;
   }

   /*
    * A constructor that takes a foreign message
    */  
   public JBossMessage(Message foreign) throws JMSException
   {
      this(foreign, JBossMessage.TYPE);
   }
      
   protected JBossMessage(Message foreign, int type) throws JMSException
   {
      this(type);

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
      
      //We can't avoid a cast warning here since getPropertyNames() is on the JMS API
      for (Enumeration<String> props = foreign.getPropertyNames(); props.hasMoreElements(); )
      {
         String name = (String)props.nextElement();
         
         Object prop = foreign.getObjectProperty(name);

         this.setObjectProperty(name, prop);                       
      }
   }
   
   // javax.jmx.Message implementation ------------------------------
   
   public String getJMSMessageID()
   {
      String id = (String)message.getHeader(JBM_MESSAGE_ID);
      
      if (id == null)
      {
         id = "ID:JBM-" + message.getMessageID();
         
         //We cache the JMSMessageID in the header - this is because when moving a message between clusters
         //the underlying message id can change but we want to preserve the JMSMessageID
         message.putHeader(JBM_MESSAGE_ID, id);
      }
      
      return id;
   }

   public void setJMSMessageID(String jmsMessageID) throws JMSException
   {
      if (jmsMessageID != null && !jmsMessageID.startsWith("ID:"))
      {
         throw new JMSException("JMSMessageID must start with ID:");
      }
      if (jmsMessageID == null)
      {
         message.removeHeader(JBM_MESSAGE_ID);
      }
      else
      {
         message.putHeader(JBM_MESSAGE_ID, jmsMessageID);
      }
   }

   public long getJMSTimestamp() throws JMSException
   {
      return message.getTimestamp();
   }

   public void setJMSTimestamp(long timestamp) throws JMSException
   {
      message.setTimestamp(timestamp);
   }

   public byte[] getJMSCorrelationIDAsBytes() throws JMSException
   {
      return (byte[]) message.getHeader(CORRELATIONIDBYTES_HEADER_NAME);
   }

   public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
   {
      if (correlationID == null || correlationID.length == 0)
      {
         throw new JMSException("Please specify a non-zero length byte[]");
      }
      message.putHeader(CORRELATIONIDBYTES_HEADER_NAME, correlationID);
      
      message.removeHeader(CORRELATIONID_HEADER_NAME);
   }

   public void setJMSCorrelationID(String correlationID) throws JMSException
   {
      message.putHeader(CORRELATIONID_HEADER_NAME, correlationID);
      
      message.removeHeader(CORRELATIONIDBYTES_HEADER_NAME);
   }

   public String getJMSCorrelationID() throws JMSException
   {
      return (String)message.getHeader(CORRELATIONID_HEADER_NAME);
   }

   public Destination getJMSReplyTo() throws JMSException
   {
      return (Destination)message.getHeader(REPLYTO_HEADER_NAME);
   }

   public void setJMSReplyTo(Destination replyTo) throws JMSException
   {
      message.putHeader(REPLYTO_HEADER_NAME, replyTo);
   }

   public Destination getJMSDestination() throws JMSException
   {
      return (Destination)message.getHeader(DESTINATION_HEADER_NAME);      
   }

   public void setJMSDestination(Destination destination) throws JMSException
   {
      message.putHeader(DESTINATION_HEADER_NAME, destination);
   }
   
   public int getJMSDeliveryMode() throws JMSException
   {
      return message.isReliable() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
   }

   public void setJMSDeliveryMode(int deliveryMode) throws JMSException
   {
      if (deliveryMode == DeliveryMode.PERSISTENT)
      {
         message.setReliable(true);
      }
      else if (deliveryMode == DeliveryMode.NON_PERSISTENT)
      {
         message.setReliable(false);
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
    * @return The JMSType header
    * @throws JMSException
    */
   public String getJMSType() throws JMSException
   {
      return (String)message.getHeader(TYPE_HEADER_NAME);
   }

   /**
    * 
    * @param type
    * @throws JMSException
    */
   public void setJMSType(String type) throws JMSException
   {
      message.putHeader(TYPE_HEADER_NAME, type);
   }

   public long getJMSExpiration() throws JMSException
   {
      return message.getExpiration();
   }

   public void setJMSExpiration(long expiration) throws JMSException
   {
      message.setExpiration(expiration);
   }

   public int getJMSPriority() throws JMSException
   {
      return message.getPriority();
   }

   public void setJMSPriority(int priority) throws JMSException
   {
      message.setPriority((byte)priority);
   }

   public void clearProperties() throws JMSException
   {
      Iterator<String> iter = message.getHeaders().keySet().iterator();
      
      while (iter.hasNext())
      {
         String propName = iter.next();
         
         if (propName.charAt(0) == PROPERTY_PREFIX_CHAR)
         {
            iter.remove();
         }
      }
   }

   public void clearBody() throws JMSException
   {
      readOnly = false;
   }

   public boolean propertyExists(String name) throws JMSException
   {
      return message.containsHeader(PROPERTY_PREFIX + name)
             || name.equals("JMSXDeliveryCount");
   }

   public boolean getBooleanProperty(String name) throws JMSException
   {
      Object value = message.getHeader(PROPERTY_PREFIX + name);
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
      Object value = message.getHeader(PROPERTY_PREFIX + name);
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
      Object value = message.getHeader(PROPERTY_PREFIX + name);
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
      
      Object value = message.getHeader(PROPERTY_PREFIX + name);

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
      if ("JMSXDeliveryCount".equals(name))
      {
         return deliveryCount;
      }
      
      Object value = message.getHeader(PROPERTY_PREFIX + name);

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
      Object value = message.getHeader(PROPERTY_PREFIX + name);
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
      Object value = message.getHeader(PROPERTY_PREFIX + name);
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
      if ("JMSXDeliveryCount".equals(name))
      {
         return Integer.toString(deliveryCount);
      }
      
      Object value = message.getHeader(PROPERTY_PREFIX + name);
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
      return message.getHeader(PROPERTY_PREFIX + name);
   }

   public Enumeration getPropertyNames() throws JMSException
   {
      HashSet<String> set = new HashSet<String>();
      
      for (String propName: message.getHeaders().keySet())
      {
         if (propName.charAt(0) == PROPERTY_PREFIX_CHAR)
         {
            String name = propName.substring(1);
            set.add(name);
         }
      }
      
      return Collections.enumeration(set);
   }

   public void setBooleanProperty(String name, boolean value) throws JMSException
   {
      Boolean b = Primitives.valueOf(value);
      checkProperty(name, b);
      message.putHeader(PROPERTY_PREFIX + name, b);
   }

   public void setByteProperty(String name, byte value) throws JMSException
   {
      Byte b = new Byte(value);
      checkProperty(name, b);
      message.putHeader(PROPERTY_PREFIX + name, b);
   }

   public void setShortProperty(String name, short value) throws JMSException
   {
      Short s = new Short(value);
      checkProperty(name, s);
      message.putHeader(PROPERTY_PREFIX + name, s);
   }

   public void setIntProperty(String name, int value) throws JMSException
   {
      Integer i = new Integer(value);
      checkProperty(name, i);
      message.putHeader(PROPERTY_PREFIX + name, i);
   }

   public void setLongProperty(String name, long value) throws JMSException
   {     
      Long l = new Long(value);
      checkProperty(name, l);
      message.putHeader(PROPERTY_PREFIX + name, l);                
   }

   public void setFloatProperty(String name, float value) throws JMSException
   {
      Float f = new Float(value);
      checkProperty(name, f);
      message.putHeader(PROPERTY_PREFIX + name, f);
   }

   public void setDoubleProperty(String name, double value) throws JMSException
   {
      Double d = new Double(value);
      checkProperty(name, d);
      message.putHeader(PROPERTY_PREFIX + name, d);
   }

   public void setStringProperty(String name, String value) throws JMSException
   {
      checkProperty(name, value);
      message.putHeader(PROPERTY_PREFIX + name, value);
   }

   public void setObjectProperty(String name, Object value) throws JMSException
   {
      checkProperty(name, value);

      if (value instanceof Boolean)
      {
         message.putHeader(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Byte)
      {
         message.putHeader(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Short)
      {
         message.putHeader(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Integer)
      {
         message.putHeader(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Long)
      {
         message.putHeader(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Float)
      {
         message.putHeader(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof Double)
      {
         message.putHeader(PROPERTY_PREFIX + name, value);
      }
      else if (value instanceof String)
      {
         message.putHeader(PROPERTY_PREFIX + name, value);
      }
      else if (value == null)
      {
         message.putHeader(PROPERTY_PREFIX + name, null);
      }
      else
      {
         throw new MessageFormatException("Invalid object type");
      }
   }
   
   public void acknowledge() throws JMSException
   {
      if (!cc)
      {
         //Only acknowledge for client ack if is not in connection consumer
         delegate.acknowledgeAll();
      }
   }
    
   // Public --------------------------------------------------------
   
   public org.jboss.messaging.newcore.Message getCoreMessage()
   {
      return message;
   }
   
   public void doBeforeSend() throws Exception
   {
      //NOOP
   }
   
   public void doBeforeReceive() throws Exception
   {
      //NOOP
   }
   
   protected void beforeSend() throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
      
      DataOutputStream daos = new DataOutputStream(baos);
            
      writePayload(daos);
      
      daos.close();
                  
      message.setPayload(baos.toByteArray());   
   }
   
   protected void beforeReceive() throws Exception
   {
      DataInputStream dais = new DataInputStream(new ByteArrayInputStream(message.getPayload()));
      
      readPayload(dais);
   }
   
   protected void writePayload(DataOutputStream daos) throws Exception
   {      
   }
   
   protected void readPayload(DataInputStream dais) throws Exception
   {      
   }

   public byte getType()
   {
      return JBossMessage.TYPE;
   }   
   
   public void setSessionDelegate(SessionDelegate sd, boolean isConnectionConsumer)
   {
      this.delegate = sd;
      this.cc = isConnectionConsumer;
   }
   
   public SessionDelegate getSessionDelegate()
   {
      return delegate;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }
   
   public void incDeliveryCount()
   {
      this.deliveryCount++;            
   }
   
   public long getDeliveryId()
   {
      return deliveryID;
   }
   
   public void copyMessage()
   {
      message = message.copy();
   }
   
   public String toString()
   {
      StringBuffer sb = new StringBuffer("JBossMessage[");
      sb.append("");
      sb.append(getJMSMessageID());
      sb.append("]:");
      sb.append(message.isReliable() ? "PERSISTENT" : "NON-PERSISTENT");
      return sb.toString();
   }
      
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
        
   protected void checkWrite() throws JMSException
   {
      if (readOnly)
      {
         throw new MessageNotWriteableException("Message is read-only");
      }
   }
   
   protected void checkRead() throws JMSException
   {
      if (!readOnly)
      {
         throw new MessageNotReadableException("Message is write-only");
      }
   }
   
   // Private ------------------------------------------------------------
   
   private void checkProperty(String name, Object value) throws JMSException
   {
      checkWrite();
      
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
   
   // Inner classes -------------------------------------------------
}
