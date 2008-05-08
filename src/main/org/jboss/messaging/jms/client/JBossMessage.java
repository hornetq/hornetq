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
package org.jboss.messaging.jms.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.ClientMessage;
import org.jboss.messaging.core.message.impl.ClientMessageImpl;
import org.jboss.messaging.jms.JBossDestination;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

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

   private static final SimpleString REPLYTO_HEADER_NAME = new SimpleString("JMSReplyTo");
   
   private static final SimpleString CORRELATIONID_HEADER_NAME = new SimpleString("JMSCorrelationID");

   private static final SimpleString JBM_MESSAGE_ID = new SimpleString("JMSMessageID");
   
   private static final SimpleString TYPE_HEADER_NAME = new SimpleString("JMSType");
   
   private static final SimpleString JMS = new SimpleString("JMS");
   
   private static final SimpleString JMSX = new SimpleString("JMSX");
   
   private static final SimpleString JMS_ = new SimpleString("JMS_");
   
   private static final String JMSXDELIVERYCOUNT = "JMSXDeliveryCount";
   
   //Used when bridging a message
   public static final String JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST = "JBM_BRIDGE_MSG_ID_LIST";
   
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
  
   public static JBossMessage createMessage(final ClientMessage message, final ClientSession session)
   {
      int type = message.getType();
      
      JBossMessage msg;
      
      switch(type)
      {
         case JBossMessage.TYPE:
            msg =  new JBossMessage(message, session);
            break;
         case JBossBytesMessage.TYPE:
            msg = new JBossBytesMessage(message, session);
            break;
         case JBossMapMessage.TYPE:
            msg =  new JBossMapMessage(message, session);
            break;
         case JBossObjectMessage.TYPE:
            msg =  new JBossObjectMessage(message, session);
            break;
         case JBossStreamMessage.TYPE:
            msg = new JBossStreamMessage(message, session);
            break;
         case JBossTextMessage.TYPE:
            msg = new JBossTextMessage(message, session);
            break;
         default:
            throw new IllegalArgumentException("Invalid message type " + type);
      }
      
      return msg;      
   }
   
   // Attributes ----------------------------------------------------

   //The underlying message
   protected ClientMessage message;
   
   protected MessagingBuffer body;
   
   private ClientSession session;
   
   //Read-only?
   protected boolean readOnly;
   
   //Cache it
   private Destination dest;
   
   //Cache it
   private String msgID;

   //Cache it
   private Destination replyTo;

   //Cache it
   private String jmsCorrelationID;
   
   //Cache it
   private String jmsType;
              
   // Constructors --------------------------------------------------
     
   /*
    * Create a new message prior to sending
    */
   protected JBossMessage(final int type)
   {
      message = new ClientMessageImpl(type, true, 0, System.currentTimeMillis(), (byte)4);
      
      //TODO - can we lazily create this?
      body = message.getBody();
   }
   
   public JBossMessage()
   {
      this (JBossMessage.TYPE);
   }
   
   /**
    * Constructor for when receiving a message from the server
    */
   public JBossMessage(final ClientMessage message, ClientSession session)
   {
      this.message = message;
      
      this.readOnly = true;
      
      this.session = session;
      
      this.body = message.getBody();
   }

   /*
    * A constructor that takes a foreign message
    */  
   public JBossMessage(final Message foreign) throws JMSException
   {
      this(foreign, JBossMessage.TYPE);
   }
      
   protected JBossMessage(final Message foreign, final int type) throws JMSException
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
      if (msgID == null)
      {
         SimpleString id = (SimpleString)message.getProperty(JBM_MESSAGE_ID);
      
         msgID = id == null ? null : id.toString();    
      }
      return msgID;
   }
   
   public void setJMSMessageID(final String jmsMessageID) throws JMSException
   {
      if (jmsMessageID != null && !jmsMessageID.startsWith("ID:"))
      {
         throw new JMSException("JMSMessageID must start with ID:");
      }
      if (jmsMessageID == null)
      {
         message.removeProperty(JBM_MESSAGE_ID);
      }
      else
      {
         message.putStringProperty(JBM_MESSAGE_ID, new SimpleString(jmsMessageID));
      }
      msgID = jmsMessageID;
   }

   public long getJMSTimestamp() throws JMSException
   {
      return message.getTimestamp();
   }

   public void setJMSTimestamp(final long timestamp) throws JMSException
   {
      message.setTimestamp(timestamp);
   }

   public byte[] getJMSCorrelationIDAsBytes() throws JMSException
   {
      Object obj = message.getProperty(CORRELATIONID_HEADER_NAME);
      
      if (obj instanceof byte[])
      {
         return (byte[])obj;
      }
      else
      {
         return null;
      }      
   }

   public void setJMSCorrelationIDAsBytes(final byte[] correlationID) throws JMSException
   {
      if (correlationID == null || correlationID.length == 0)
      {
         throw new JMSException("Please specify a non-zero length byte[]");
      }
      message.putBytesProperty(CORRELATIONID_HEADER_NAME, correlationID);
   }

   public void setJMSCorrelationID(final String correlationID) throws JMSException
   {
      if (correlationID == null)
      {
         message.removeProperty(CORRELATIONID_HEADER_NAME);
         
         jmsCorrelationID = null;
      }
      else
      {
         message.putStringProperty(CORRELATIONID_HEADER_NAME, new SimpleString(correlationID));
         
         jmsCorrelationID = correlationID;
      }
   }
   
   public String getJMSCorrelationID() throws JMSException
   {
      if (jmsCorrelationID == null)
      {
         Object obj = message.getProperty(CORRELATIONID_HEADER_NAME);
         
         if (obj != null)
         {
            jmsCorrelationID = ((SimpleString)obj).toString();
         }  
      }
      
      return jmsCorrelationID;         
   }
   
   public Destination getJMSReplyTo() throws JMSException
   {
      if (replyTo == null)
      {
         SimpleString repl = (SimpleString)message.getProperty(REPLYTO_HEADER_NAME);
         
         if (repl != null)
         {
            replyTo = JBossDestination.fromAddress(repl.toString());
         }
      }
      return replyTo;
   }

   public void setJMSReplyTo(final Destination dest) throws JMSException
   {
      if (dest == null)
      {
         message.removeProperty(REPLYTO_HEADER_NAME);
         
         replyTo = null;
      }
      else
      {
         if (dest instanceof JBossDestination == false)
         {
            throw new InvalidDestinationException("Not a JBoss destination " + dest);
         }
         
         JBossDestination jbd = (JBossDestination)dest;
         
         message.putStringProperty(REPLYTO_HEADER_NAME, jbd.getSimpleAddress());
         
         replyTo = jbd;
      }
   }
   
   public Destination getJMSDestination() throws JMSException
   {
      if (dest == null)
      {
         SimpleString sdest = message.getDestination();
         
         dest = sdest == null ? null : JBossDestination.fromAddress(sdest.toString());
      }

      return dest;         
   }

   public void setJMSDestination(final Destination destination) throws JMSException
   {
      this.dest = destination;
   }
   
   public int getJMSDeliveryMode() throws JMSException
   {
      return message.isDurable() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
   }

   public void setJMSDeliveryMode(final int deliveryMode) throws JMSException
   {
      if (deliveryMode == DeliveryMode.PERSISTENT)
      {
         message.setDurable(true);
      }
      else if (deliveryMode == DeliveryMode.NON_PERSISTENT)
      {
         message.setDurable(false);
      }
      else
      {
         throw new JMSException("DeliveryImpl mode must be either DeliveryMode.PERSISTENT "
               + "or DeliveryMode.NON_PERSISTENT");
      }
   }

   public boolean getJMSRedelivered() throws JMSException
   {
      return message.getDeliveryCount() > 1;
   }

   public void setJMSRedelivered(final boolean redelivered) throws JMSException
   {      
      if (message.getDeliveryCount() > 1)
      {
         //do nothing
      }
      else
      {
         message.setDeliveryCount(2);
      }
   }
  
   public void setJMSType(final String type) throws JMSException
   {
      if (type != null)
      {
         message.putStringProperty(TYPE_HEADER_NAME, new SimpleString(type));
         
         jmsType = type;
      }
   }

   public String getJMSType() throws JMSException
   {
      if (jmsType == null)
      {
         SimpleString ss = (SimpleString)message.getProperty(TYPE_HEADER_NAME);
         
         if (ss != null)
         {
            jmsType = ss.toString();
         }
      }
      return jmsType;
   }

   public long getJMSExpiration() throws JMSException
   {
      return message.getExpiration();
   }

   public void setJMSExpiration(final long expiration) throws JMSException
   {
      message.setExpiration(expiration);
   }

   public int getJMSPriority() throws JMSException
   {
      return message.getPriority();
   }

   public void setJMSPriority(final int priority) throws JMSException
   {
      message.setPriority((byte)priority);
   }
      
   public void clearProperties() throws JMSException
   {     
      List<SimpleString> toRemove = new ArrayList<SimpleString>();
      
      for (SimpleString propName: message.getPropertyNames())
      {
         if (!propName.startsWith(JMS) || propName.startsWith(JMSX) || propName.startsWith(JMS_))
         {
            toRemove.add(propName);            
         }
      }     
      
      for (SimpleString propName: toRemove)
      {
         message.removeProperty(propName);
      }
   }

   public void clearBody() throws JMSException
   {
      readOnly = false;
   }

   public boolean propertyExists(final String name) throws JMSException
   {
      return message.containsProperty(new SimpleString(name))
             || name.equals(JMSXDELIVERYCOUNT);
   }

   public boolean getBooleanProperty(final String name) throws JMSException
   {
      Object value = message.getProperty(new SimpleString(name));
      if (value == null)
         return Boolean.valueOf(null).booleanValue();

      if (value instanceof Boolean)
         return ((Boolean) value).booleanValue();
      else if (value instanceof SimpleString)
         return Boolean.valueOf(((SimpleString) value).toString()).booleanValue();
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public byte getByteProperty(final String name) throws JMSException
   {
      Object value = message.getProperty(new SimpleString(name));
      if (value == null)
         throw new NumberFormatException("Message property '" + name + "' not set.");

      if (value instanceof Byte)
         return ((Byte) value).byteValue();
      else if (value instanceof SimpleString)
         return Byte.parseByte(((SimpleString) value).toString());
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public short getShortProperty(final String name) throws JMSException
   {
      Object value = message.getProperty(new SimpleString(name));
      if (value == null)
         throw new NumberFormatException("Message property '" + name + "' not set.");

      if (value instanceof Byte)
         return ((Byte) value).shortValue();
      else if (value instanceof Short)
         return ((Short) value).shortValue();
      else if (value instanceof SimpleString)
         return Short.parseShort(((SimpleString) value).toString());
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public int getIntProperty(final String name) throws JMSException
   {       
      if (JMSXDELIVERYCOUNT.equals(name))
      {
         return message.getDeliveryCount();
      }
      
      Object value = message.getProperty(new SimpleString(name));

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
      else if (value instanceof SimpleString)
      {
         return Integer.parseInt(((SimpleString) value).toString());
      }
      else
      {
         throw new MessageFormatException("Invalid conversion");
      }
   }

   public long getLongProperty(final String name) throws JMSException
   {
      if (JMSXDELIVERYCOUNT.equals(name))
      {
         return message.getDeliveryCount();
      }
      
      Object value = message.getProperty(new SimpleString(name));

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
      else if (value instanceof SimpleString)
      {
         return Long.parseLong(((SimpleString) value).toString());
      }
      else
      {
         throw new MessageFormatException("Invalid conversion");
      }
   }

   public float getFloatProperty(final String name) throws JMSException
   {
      Object value = message.getProperty(new SimpleString(name));
      if (value == null)
         return Float.valueOf(null).floatValue();

      if (value instanceof Float)
         return ((Float) value).floatValue();
      else if (value instanceof SimpleString)
         return Float.parseFloat(((SimpleString) value).toString());
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public double getDoubleProperty(final String name) throws JMSException
   {
      Object value = message.getProperty(new SimpleString(name));
      if (value == null)
         return Double.valueOf(null).doubleValue();

      if (value instanceof Float)
         return ((Float) value).doubleValue();
      else if (value instanceof Double)
         return ((Double) value).doubleValue();
      else if (value instanceof SimpleString)
         return Double.parseDouble(((SimpleString) value).toString());
      else
         throw new MessageFormatException("Invalid conversion");
   }

   public String getStringProperty(final String name) throws JMSException
   {
      if (JMSXDELIVERYCOUNT.equals(name))
      {
         return String.valueOf(message.getDeliveryCount());
      }
      Object value = message.getProperty(new SimpleString(name));
      if (value == null)
         return null;

      if (value instanceof SimpleString)
      {
         return ((SimpleString) value).toString();
      }
      else if (value instanceof Boolean)
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
      else
      {
         throw new MessageFormatException("Invalid conversion");
      }
   }

   public Object getObjectProperty(final String name) throws JMSException                                                              
   {
      if (JMSXDELIVERYCOUNT.equals(name))
      {
         return String.valueOf(message.getDeliveryCount());
      }
      Object val = message.getProperty(new SimpleString(name));
      if (val instanceof SimpleString)
      {
         val = ((SimpleString)val).toString();
      }
      return val;
   }

   public Enumeration getPropertyNames() throws JMSException
   {
      HashSet<String> set = new HashSet<String>();
      
      for (SimpleString propName: message.getPropertyNames())
      {
         if (!propName.startsWith(JMS) || propName.startsWith(JMSX) || propName.startsWith(JMS_))
         {
            set.add(propName.toString());
         }
      }
      
      set.add(JMSXDELIVERYCOUNT);
      
      return Collections.enumeration(set);
   }

   public void setBooleanProperty(final String name, final boolean value) throws JMSException
   {
      Boolean b = Boolean.valueOf(value);
      checkProperty(name, b);
      message.putBooleanProperty(new SimpleString(name), b);
   }

   public void setByteProperty(final String name, final byte value) throws JMSException
   {
      Byte b = new Byte(value);
      checkProperty(name, b);
      message.putByteProperty(new SimpleString(name), value);
   }

   public void setShortProperty(final String name, final short value) throws JMSException
   {
      Short s = new Short(value);
      checkProperty(name, s);
      message.putShortProperty(new SimpleString(name), value);
   }

   public void setIntProperty(final String name, final int value) throws JMSException
   {
      Integer i = new Integer(value);
      checkProperty(name, i);
      message.putIntProperty(new SimpleString(name), value);
   }

   public void setLongProperty(final String name, final long value) throws JMSException
   {     
      Long l = new Long(value);
      checkProperty(name, l);
      message.putLongProperty(new SimpleString(name), value);               
   }

   public void setFloatProperty(final String name, final float value) throws JMSException
   {
      Float f = new Float(value);
      checkProperty(name, f);
      message.putFloatProperty(new SimpleString(name), f);
   }

   public void setDoubleProperty(final String name, final double value) throws JMSException
   {
      Double d = new Double(value);
      checkProperty(name, d);
      message.putDoubleProperty(new SimpleString(name), d);
   }

   public void setStringProperty(final String name, final String value) throws JMSException
   {
      checkProperty(name, value);
      message.putStringProperty(new SimpleString(name), new SimpleString(value));
   }

   public void setObjectProperty(final String name, final Object value) throws JMSException
   {
      checkProperty(name, value);
      
      SimpleString key = new SimpleString(name);

      if (value instanceof Boolean)
      {
         message.putBooleanProperty(key, (Boolean)value);
      }
      else if (value instanceof Byte)
      {
         message.putByteProperty(key, (Byte)value);
      }
      else if (value instanceof Short)
      {
         message.putShortProperty(key, (Short)value);
      }
      else if (value instanceof Integer)
      {
         message.putIntProperty(key, (Integer)value);
      }
      else if (value instanceof Long)
      {
         message.putLongProperty(key, (Long)value);
      }
      else if (value instanceof Float)
      {
         message.putFloatProperty(key, (Float)value);
      }
      else if (value instanceof Double)
      {
         message.putDoubleProperty(key, (Double)value);
      }
      else if (value instanceof String)
      {
         message.putStringProperty(key, new SimpleString((String)value));
      }
      else
      {
         throw new MessageFormatException("Invalid property type");
      }
   }
   
   public void acknowledge() throws JMSException
   {
      try
      {
         session.commit();
      }
      catch (MessagingException e)
      {
         JMSException je = new JMSException(e.toString());
         
         je.initCause(e);
         
         throw je;         
      } 
   }
    
   // Public --------------------------------------------------------
   
   public ClientMessage getCoreMessage()
   {
      return message;
   }
   
   public void doBeforeSend() throws Exception
   {
      body.flip();
      
      message.setBody(body);
   }
   
   public void doBeforeReceive() throws Exception
   {
      body = message.getBody();
   }
   
   public byte getType()
   {
      return JBossMessage.TYPE;
   }   
   
   public ClientSession getSession()
   {
      return session;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("JBossMessage[");
      sb.append("");
      sb.append(getJMSMessageID());
      sb.append("]:");
      sb.append(message.isDurable() ? "PERSISTENT" : "NON-PERSISTENT");
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
   
   private void checkProperty(final String name, final Object value) throws JMSException
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

      if (!isValidJavaIdentifier(name))
      {
         throw new IllegalArgumentException("The property name '" + name +
                                            "' is not a valid java identifier.");
      }

      if (reservedIdentifiers.contains(name))
      {
         throw new IllegalArgumentException("The property name '" + name +
                                            "' is reserved due to selector syntax.");
      }
      
      if (name.startsWith("JMS"))
      {
         if (name.length() > 3)
         {
            char c = name.charAt(3);
            if (c != 'X' && c != '_')
            {
               //See http://java.sun.com/javaee/5/docs/api/
               //(java.jms.Message javadoc)
               //"Property names must obey the rules for a message selector identifier"
               //"Any name that does not begin with 'JMS' is an application-specific property name"
               throw new IllegalArgumentException("The property name '" + name + "' is illegal since it starts with JMS");
            }
         }
         else
         {
            throw new IllegalArgumentException("The property name '" + name + "' is illegal since it starts with JMS");
         }
      }
   }
   
   private boolean isValidJavaIdentifier(final String s)
   {
      if (s == null || s.length() == 0)
      {
         return false;
      }

      char[] c = s.toCharArray();
      
      if (!Character.isJavaIdentifierStart(c[0]))
      {
         return false;
      }

      for (int i = 1; i < c.length; i++)
      {
         if (!Character.isJavaIdentifierPart(c[i]))
         {
            return false;
         }
      }

      return true;
   }
   
   // Inner classes -------------------------------------------------
}
