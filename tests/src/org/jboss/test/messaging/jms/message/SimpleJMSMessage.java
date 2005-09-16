/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import javax.jms.Message;
import javax.jms.JMSException;
import javax.jms.Destination;
import javax.jms.DeliveryMode;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

/**
 * Foreign message implementation. Used for testing only.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SimpleJMSMessage implements Message
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Message implementation ----------------------------------------

   private String hiddenPrivateField;
   
   private String messageID;

   public String getHiddenPrivateField()
   {
      return hiddenPrivateField;
   }
   
   public void setHiddenPrivateField(String f)
   {
      hiddenPrivateField = f;
   }
   
   public String getJMSMessageID() throws JMSException
   {
      return messageID;
   }


   public void setJMSMessageID(String id) throws JMSException
   {
      messageID = id;
   }

   private long timestamp;

   public long getJMSTimestamp() throws JMSException
   {
      return timestamp;
   }


   public void setJMSTimestamp(long timestamp) throws JMSException
   {
      this.timestamp = timestamp;
   }

   //
   // TODO Is this really the spec?
   //

   private byte[] correlationIDBytes;
   private String correlationIDString;
   private boolean isCorrelationIDBytes;


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
      this.correlationIDString = correlationID;
      isCorrelationIDBytes = false;
   }

   public String getJMSCorrelationID() throws JMSException
   {
      if (isCorrelationIDBytes)
      {
         throw new JMSException("CorrelationID is a byte[] for this message");
      }
      return correlationIDString;
   }


   private Destination replyTo;

   public Destination getJMSReplyTo() throws JMSException
   {
      return replyTo;
   }


   public void setJMSReplyTo(Destination replyTo) throws JMSException
   {
      this.replyTo = replyTo;
   }


   private Destination destination;

   public Destination getJMSDestination() throws JMSException
   {
      return destination;
   }


   public void setJMSDestination(Destination destination) throws JMSException
   {
      this.destination = destination;
   }


   private int deliveryMode = DeliveryMode.PERSISTENT;

   public int getJMSDeliveryMode() throws JMSException
   {
      return deliveryMode;
   }


   public void setJMSDeliveryMode(int deliveryMode) throws JMSException
   {
      this.deliveryMode = deliveryMode;
   }

   private boolean redelivered;

   public boolean getJMSRedelivered() throws JMSException
   {
      return redelivered;
   }


   public void setJMSRedelivered(boolean redelivered) throws JMSException
   {
      this.redelivered = redelivered;
   }

   private String type;

   public String getJMSType() throws JMSException
   {
      return type;
   }


   public void setJMSType(String type) throws JMSException
   {
      this.type = type;
   }

   private long expiration;

   public long getJMSExpiration() throws JMSException
   {
      return expiration;
   }

   public void setJMSExpiration(long expiration) throws JMSException
   {
      this.expiration = expiration;
   }

   private int priority;

   public int getJMSPriority() throws JMSException
   {
      return priority;
   }

   public void setJMSPriority(int priority) throws JMSException
   {
      this.priority = priority;
   }

   private Map properties = new HashMap();

   public void clearProperties() throws JMSException
   {
      properties.clear();
   }

   public boolean propertyExists(String name) throws JMSException
   {
      return properties.containsKey(name);
   }


   public boolean getBooleanProperty(String name) throws JMSException
   {
      return ((Boolean)properties.get(name)).booleanValue();
   }


   public byte getByteProperty(String name) throws JMSException
   {
       return ((Byte)properties.get(name)).byteValue();
   }


   public short getShortProperty(String name) throws JMSException
   {
      return ((Short)properties.get(name)).shortValue();
   }


   public int getIntProperty(String name) throws JMSException
   {
      return ((Integer)properties.get(name)).intValue();
   }


   public long getLongProperty(String name) throws JMSException
   {
      return ((Long)properties.get(name)).longValue();
   }


   public float getFloatProperty(String name) throws JMSException
   {
      return ((Float)properties.get(name)).floatValue();
   }


   public double getDoubleProperty(String name) throws JMSException
   {
      return ((Double)properties.get(name)).doubleValue();
   }


   public String getStringProperty(String name) throws JMSException
   {
      return (String)properties.get(name);
   }


   public Object getObjectProperty(String name) throws JMSException
   {
      return properties.get(name);
   }


   public Enumeration getPropertyNames() throws JMSException
   {
      return Collections.enumeration(properties.keySet());
   }


   public void setBooleanProperty(String name, boolean value) throws JMSException
   {
      properties.put(name, new Boolean(value));
   }


   public void setByteProperty(String name, byte value) throws JMSException
   {
      properties.put(name, new Byte(value));
   }


   public void setShortProperty(String name, short value) throws JMSException
   {
      properties.put(name, new Short(value));
   }


   public void setIntProperty(String name, int value) throws JMSException
   {
      properties.put(name, new Integer(value));
   }


   public void setLongProperty(String name, long value) throws JMSException
   {
      properties.put(name, new Long(value));
   }


   public void setFloatProperty(String name, float value) throws JMSException
   {
      properties.put(name, new Float(value));
   }


   public void setDoubleProperty(String name, double value) throws JMSException
   {
      properties.put(name, new Double(value));
   }


   public void setStringProperty(String name, String value) throws JMSException
   {
      properties.put(name, value);
   }


   public void setObjectProperty(String name, Object value) throws JMSException
   {
      properties.put(name, value);
   }


   public void acknowledge() throws JMSException
   {

   }


   public void clearBody() throws JMSException
   {

   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
