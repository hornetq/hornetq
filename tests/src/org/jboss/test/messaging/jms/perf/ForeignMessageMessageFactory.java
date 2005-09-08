/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.io.Serializable;
import java.util.Collections;
import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ForeignMessageMessageFactory extends AbstractMessageFactory
{

   public Message getMessage(Session sess, int size) throws JMSException
   {
      return new ForeignMessage(size);
   }

   static class ForeignMessage implements Message, Serializable
   {

      private static final long serialVersionUID = 6055142727806821394L;

      
      private byte[] hidden;
      ForeignMessage(int size)
      {
         hidden = getBytes(size);
      }
      
      public byte[] getHidden()
      {         
         //Never gets called
         //But it's to prevent the compiler optimising the hidden field away!!
         return hidden;
      }

      public void acknowledge() throws JMSException
      {
         // FIXME acknowledge

      }

      public void clearBody() throws JMSException
      {
         // FIXME clearBody

      }

      public void clearProperties() throws JMSException
      {
         // FIXME clearProperties

      }

      public boolean getBooleanProperty(String name) throws JMSException
      {
         // FIXME getBooleanProperty
         return false;
      }

      public byte getByteProperty(String name) throws JMSException
      {
         throw new NumberFormatException();
      }

      public double getDoubleProperty(String name) throws JMSException
      {
         // FIXME getDoubleProperty
         throw new NumberFormatException();
      }

      public float getFloatProperty(String name) throws JMSException
      {
         // FIXME getFloatProperty
         throw new NumberFormatException();
      }

      public int getIntProperty(String name) throws JMSException
      {
         // FIXME getIntProperty
         throw new NumberFormatException();
      }

      private String correlationID;

      public String getJMSCorrelationID() throws JMSException
      {
         return correlationID;
      }

      private byte[] correlationIDBytes;

      public byte[] getJMSCorrelationIDAsBytes() throws JMSException
      {
         // FIXME getJMSCorrelationIDAsBytes
         return correlationIDBytes;
      }

      private int deliveryMode;

      public int getJMSDeliveryMode() throws JMSException
      {
         return deliveryMode;
      }

      private Destination dest;

      public Destination getJMSDestination() throws JMSException
      {
         return dest;
      }

      private long expiration;

      public long getJMSExpiration() throws JMSException
      {
         return expiration;
      }

      private String messageID;

      public String getJMSMessageID() throws JMSException
      {
         return messageID;
      }

      private int priority;

      public int getJMSPriority() throws JMSException
      {
         return priority;
      }

      private boolean redelivered;

      public boolean getJMSRedelivered() throws JMSException
      {
         return redelivered;
      }

      private Destination replyTo;

      public Destination getJMSReplyTo() throws JMSException
      {
         return replyTo;
      }

      private long timestamp;

      public long getJMSTimestamp() throws JMSException
      {
         return timestamp;
      }

      private String type;

      public String getJMSType() throws JMSException
      {
         return type;
      }

      public long getLongProperty(String name) throws JMSException
      {
         throw new NumberFormatException();
      }

      public Object getObjectProperty(String name) throws JMSException
      {
         return null;
      }

      public Enumeration getPropertyNames() throws JMSException
      {
         return Collections.enumeration(Collections.EMPTY_SET);
      }

      public short getShortProperty(String name) throws JMSException
      {
         throw new NumberFormatException();
      }

      public String getStringProperty(String name) throws JMSException
      {
         return null;
      }

      public boolean propertyExists(String name) throws JMSException
      {
         return false;
      }

      public void setBooleanProperty(String name, boolean value) throws JMSException
      {
      }

      public void setByteProperty(String name, byte value) throws JMSException
      {
      }

      public void setDoubleProperty(String name, double value) throws JMSException
      {
      }

      public void setFloatProperty(String name, float value) throws JMSException
      {
      }

      public void setIntProperty(String name, int value) throws JMSException
      {
      }

      public void setJMSCorrelationID(String correlationID) throws JMSException
      {
         this.correlationID = correlationID;
      }

      public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
      {
         this.correlationIDBytes = correlationID;

      }

      public void setJMSDeliveryMode(int deliveryMode) throws JMSException
      {
         this.deliveryMode = deliveryMode;
      }

      public void setJMSDestination(Destination destination) throws JMSException
      {
         this.dest = destination;
      }

      public void setJMSExpiration(long expiration) throws JMSException
      {
         this.expiration = expiration;

      }

      public void setJMSMessageID(String id) throws JMSException
      {
         this.messageID = id;
      }

      public void setJMSPriority(int priority) throws JMSException
      {
         this.priority = priority;
      }

      public void setJMSRedelivered(boolean redelivered) throws JMSException
      {
         this.redelivered = redelivered;
      }

      public void setJMSReplyTo(Destination replyTo) throws JMSException
      {
         this.replyTo = replyTo;
      }

      public void setJMSTimestamp(long timestamp) throws JMSException
      {
         this.timestamp = timestamp;
      }

      public void setJMSType(String type) throws JMSException
      {
         this.type = type;
      }

      public void setLongProperty(String name, long value) throws JMSException
      {
      }

      public void setObjectProperty(String name, Object value) throws JMSException
      {

      }

      public void setShortProperty(String name, short value) throws JMSException
      {

      }

      public void setStringProperty(String name, String value) throws JMSException
      {

      }

   }

}
