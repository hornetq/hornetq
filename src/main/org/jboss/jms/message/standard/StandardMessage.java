/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.message.standard;

import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.jboss.jms.client.SessionDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.util.MessageProperties;
import org.jboss.util.id.GUID;

/**
 * A standard message
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class StandardMessage
   implements JBossMessage
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The session */
   private transient SessionDelegate session;

   /** The message properties */
   private MessageProperties properties = new MessageProperties();

   /** The message id */
   private String messageID;

   /** The destination */
   private Destination destination;

   /** The delivery mode */
   private int deliveryMode;

   /** The priority */
   private int priority;

   /** The send timestamp */
   private long timestamp;

   /** The expiration timestamp */
   private long expiration;

   /** The message type */
   private String type;

   /** The reply to destination */
   private Destination replyTo;

   /** The correlation id */
   private String correlationID;

   /** Whether the message has been redelivered */
   private boolean redelivered;

   /** Whether the message is read only */
   private boolean readonly = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public StandardMessage(SessionDelegate session)
      throws JMSException
   {
      this.session = session;
   }

   // Message implementation ----------------------------------------

   public void acknowledge() throws JMSException
   {
      session.acknowledge(this, true);
   }

   public void clearBody() throws JMSException
   {
      readonly = false;
   }

   public void clearProperties() throws JMSException
   {
      properties.clear();
   }

   public boolean getBooleanProperty(String name) throws JMSException
   {
      return properties.getBoolean(name);
   }

   public byte getByteProperty(String name) throws JMSException
   {
      return properties.getByte(name);
   }

   public double getDoubleProperty(String name) throws JMSException
   {
      return properties.getDouble(name);
   }

   public float getFloatProperty(String name) throws JMSException
   {
      return properties.getFloat(name);
   }

   public int getIntProperty(String name) throws JMSException
   {
      return properties.getInt(name);
   }

   public String getJMSCorrelationID() throws JMSException
   {
      return correlationID;
   }

   public byte[] getJMSCorrelationIDAsBytes() throws JMSException
   {
      // TODO getJMSCorrelationIDAsBytes
      return null;
   }

   public int getJMSDeliveryMode() throws JMSException
   {
      return deliveryMode;
   }

   public Destination getJMSDestination() throws JMSException
   {
      return destination;
   }

   public long getJMSExpiration() throws JMSException
   {
      return expiration;
   }

   public String getJMSMessageID() throws JMSException
   {
      return messageID;
   }

   public int getJMSPriority() throws JMSException
   {
      return priority;
   }

   public boolean getJMSRedelivered() throws JMSException
   {
      return redelivered;
   }

   public Destination getJMSReplyTo() throws JMSException
   {
      return replyTo;
   }

   public long getJMSTimestamp() throws JMSException
   {
      return timestamp;
   }

   public String getJMSType() throws JMSException
   {
      return type;
   }

   public long getLongProperty(String name) throws JMSException
   {
      return properties.getLong(name);
   }

   public Object getObjectProperty(String name) throws JMSException
   {
      return properties.getObject(name);
   }

   public Enumeration getPropertyNames() throws JMSException
   {
      return properties.getMapNames();
   }

   public short getShortProperty(String name) throws JMSException
   {
      return properties.getShort(name);
   }

   public String getStringProperty(String name) throws JMSException
   {
      return properties.getString(name);
   }

   public boolean propertyExists(String name) throws JMSException
   {
      return properties.itemExists(name);
   }

   public void setBooleanProperty(String name, boolean value) throws JMSException
   {
      properties.setBoolean(name, value);
   }

   public void setByteProperty(String name, byte value) throws JMSException
   {
      properties.setByte(name, value);
   }

   public void setDoubleProperty(String name, double value) throws JMSException
   {
      properties.setDouble(name, value);
   }

   public void setFloatProperty(String name, float value) throws JMSException
   {
      properties.setFloat(name, value);
   }

   public void setIntProperty(String name, int value) throws JMSException
   {
      properties.setInt(name, value);
   }

   public void setJMSCorrelationID(String correlationID) throws JMSException
   {
      checkReadOnly();
      this.correlationID = correlationID;
   }

   public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
   {
      checkReadOnly();
      // TODO setJMSCorrelationIDAsBytes
   }

   public void setJMSDeliveryMode(int deliveryMode) throws JMSException
   {
      checkReadOnly();
      this.deliveryMode = deliveryMode;
   }

   public void setJMSDestination(Destination destination) throws JMSException
   {
      checkReadOnly();
      this.destination = destination;
   }

   public void setJMSExpiration(long expiration) throws JMSException
   {
      checkReadOnly();
      this.expiration = expiration;
   }

   public void setJMSMessageID(String id) throws JMSException
   {
      checkReadOnly();
      this.messageID = id;
   }

   public void setJMSPriority(int priority) throws JMSException
   {
      checkReadOnly();
      this.priority = priority;
   }

   public void setJMSRedelivered(boolean redelivered) throws JMSException
   {
      checkReadOnly();
      this.redelivered = redelivered;
   }

   public void setJMSReplyTo(Destination replyTo) throws JMSException
   {
      checkReadOnly();
      this.replyTo = replyTo;
   }

   public void setJMSTimestamp(long timestamp) throws JMSException
   {
      checkReadOnly();
      this.timestamp = timestamp;
   }

   public void setJMSType(String type) throws JMSException
   {
      checkReadOnly();
      this.type = type;
   }

   public void setLongProperty(String name, long value) throws JMSException
   {
      properties.setLong(name, value);
   }

   public void setObjectProperty(String name, Object value) throws JMSException
   {
      properties.setObject(name, value);
   }

   public void setShortProperty(String name, short value) throws JMSException
   {
      properties.setShort(name, value);
   }

   public void setStringProperty(String name, String value) throws JMSException
   {
      properties.setString(name, value);
   }

   // JBossMessage implementation -----------------------------------

   public SessionDelegate getSessionDelegate() throws JMSException
   {
      return session;
   }
   
   public void generateMessageID() throws JMSException
   {
      checkReadOnly();
      messageID = GUID.asString();
   }

   public void generateTimestamp() throws JMSException
   {
      checkReadOnly();
      timestamp = System.currentTimeMillis();
   }

   public void makeReadOnly() throws JMSException
   {
      readonly = true;
      properties.setReadOnly(true);
   }

   // Object implementation ------------------------------------------
   
   public Object clone()
      throws CloneNotSupportedException
   {
      return super.clone();
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   /**
    * Checks whether a message is read only
    * 
    * @throws JMSException for a read only message
    */
   private void checkReadOnly()
      throws JMSException
   {
      if (readonly)
         throw new JMSException("The message is read only");
   }

   // Inner Classes --------------------------------------------------

}
