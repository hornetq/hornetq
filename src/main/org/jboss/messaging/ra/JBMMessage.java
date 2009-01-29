/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.messaging.core.logging.Logger;

/**
 * A wrapper for a message
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMMessage implements Message
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMMessage.class);
   
   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The message */
   protected Message message;
   
   /** The session */
   protected JBMSession session;

   /**
    * Create a new wrapper
    * @param message the message
    * @param session the session
    */
   public JBMMessage(Message message, JBMSession session)
   {
      if (trace)
         log.trace("constructor(" + message + ", " + session + ")");

      this.message = message;
      this.session = session;
   }

   /**
    * Acknowledge
    * @exception JMSException Thrown if an error occurs
    */
   public void acknowledge() throws JMSException
   {
      if (trace)
         log.trace("acknowledge()");

      session.getSession(); // Check for closed
      message.acknowledge();
   }
   
   /**
    * Clear body
    * @exception JMSException Thrown if an error occurs
    */
   public void clearBody() throws JMSException
   {
      if (trace)
         log.trace("clearBody()");

      message.clearBody();
   }
   
   /**
    * Clear properties
    * @exception JMSException Thrown if an error occurs
    */
   public void clearProperties() throws JMSException
   {
      if (trace)
         log.trace("clearProperties()");

      message.clearProperties();
   }
   
   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public boolean getBooleanProperty(String name) throws JMSException
   {
      if (trace)
         log.trace("getBooleanProperty(" + name + ")");

      return message.getBooleanProperty(name);
   }
   
   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public byte getByteProperty(String name) throws JMSException
   {
      if (trace)
         log.trace("getByteProperty(" + name + ")");

      return message.getByteProperty(name);
   }

   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public double getDoubleProperty(String name) throws JMSException
   {
      if (trace)
         log.trace("getDoubleProperty(" + name + ")");

      return message.getDoubleProperty(name);
   }
   
   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public float getFloatProperty(String name) throws JMSException
   {
      if (trace)
         log.trace("getFloatProperty(" + name + ")");

      return message.getFloatProperty(name);
   }
   
   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int getIntProperty(String name) throws JMSException
   {
      if (trace)
         log.trace("getIntProperty(" + name + ")");

      return message.getIntProperty(name);
   }
   
   /**
    * Get correlation id
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public String getJMSCorrelationID() throws JMSException
   {
      if (trace)
         log.trace("getJMSCorrelationID()");

      return message.getJMSCorrelationID();
   }
   
   /**
    * Get correlation id
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public byte[] getJMSCorrelationIDAsBytes() throws JMSException
   {
      if (trace)
         log.trace("getJMSCorrelationIDAsBytes()");

      return message.getJMSCorrelationIDAsBytes();
   }
   
   /**
    * Get delivery mode
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int getJMSDeliveryMode() throws JMSException
   {
      if (trace)
         log.trace("getJMSDeliveryMode()");

      return message.getJMSDeliveryMode();
   }
   
   /**
    * Get destination
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public Destination getJMSDestination() throws JMSException
   {
      if (trace)
         log.trace("getJMSDestination()");

      return message.getJMSDestination();
   }
   
   /**
    * Get expiration
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public long getJMSExpiration() throws JMSException
   {
      if (trace)
         log.trace("getJMSExpiration()");

      return message.getJMSExpiration();
   }
   
   /**
    * Get message id
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public String getJMSMessageID() throws JMSException
   {
      if (trace)
         log.trace("getJMSMessageID()");

      return message.getJMSMessageID();
   }
   
   /**
    * Get priority
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int getJMSPriority() throws JMSException
   {
      if (trace)
         log.trace("getJMSPriority()");

      return message.getJMSPriority();
   }
   
   /**
    * Get redelivered status
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public boolean getJMSRedelivered() throws JMSException
   {
      if (trace)
         log.trace("getJMSRedelivered()");

      return message.getJMSRedelivered();
   }
   
   /**
    * Get reply to destination
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public Destination getJMSReplyTo() throws JMSException
   {
      if (trace)
         log.trace("getJMSReplyTo()");

      return message.getJMSReplyTo();
   }
   
   /**
    * Get timestamp
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public long getJMSTimestamp() throws JMSException
   {
      if (trace)
         log.trace("getJMSTimestamp()");

      return message.getJMSTimestamp();
   }
   
   /**
    * Get type
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public String getJMSType() throws JMSException
   {
      if (trace)
         log.trace("getJMSType()");

      return message.getJMSType();
   }
   
   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public long getLongProperty(String name) throws JMSException
   {
      if (trace)
         log.trace("getLongProperty(" + name + ")");

      return message.getLongProperty(name);
   }
   
   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public Object getObjectProperty(String name) throws JMSException
   {
      if (trace)
         log.trace("getObjectProperty(" + name + ")");

      return message.getObjectProperty(name);
   }
   
   /**
    * Get property names
    * @return The values
    * @exception JMSException Thrown if an error occurs
    */
   public Enumeration getPropertyNames() throws JMSException
   {
      if (trace)
         log.trace("getPropertyNames()");

      return message.getPropertyNames();
   }
   
   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public short getShortProperty(String name) throws JMSException
   {
      if (trace)
         log.trace("getShortProperty(" + name + ")");

      return message.getShortProperty(name);
   }
   
   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public String getStringProperty(String name) throws JMSException
   {
      if (trace)
         log.trace("getStringProperty(" + name + ")");

      return message.getStringProperty(name);
   }
   
   /**
    * Do property exist
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public boolean propertyExists(String name) throws JMSException
   {
      if (trace)
         log.trace("propertyExists(" + name + ")");

      return message.propertyExists(name);
   }
   
   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setBooleanProperty(String name, boolean value) throws JMSException
   {
      if (trace)
         log.trace("setBooleanProperty(" + name + ", " + value + ")");

      message.setBooleanProperty(name, value);
   }
   
   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setByteProperty(String name, byte value) throws JMSException
   {
      if (trace)
         log.trace("setByteProperty(" + name + ", " + value + ")");

      message.setByteProperty(name, value);
   }
   
   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setDoubleProperty(String name, double value) throws JMSException
   {
      if (trace)
         log.trace("setDoubleProperty(" + name + ", " + value + ")");

      message.setDoubleProperty(name, value);
   }
   
   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setFloatProperty(String name, float value) throws JMSException
   {
      if (trace)
         log.trace("setFloatProperty(" + name + ", " + value + ")");

      message.setFloatProperty(name, value);
   }
   
   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setIntProperty(String name, int value) throws JMSException
   {
      if (trace)
         log.trace("setIntProperty(" + name + ", " + value + ")");

      message.setIntProperty(name, value);
   }
   
   /**
    * Set correlation id
    * @param correlationID The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSCorrelationID(String correlationID) throws JMSException
   {
      if (trace)
         log.trace("setJMSCorrelationID(" + correlationID + ")");

      message.setJMSCorrelationID(correlationID);
   }

   /**
    * Set correlation id
    * @param correlationID The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
   {
      if (trace)
         log.trace("setJMSCorrelationIDAsBytes(" + correlationID + ")");

      message.setJMSCorrelationIDAsBytes(correlationID);
   }

   /**
    * Set delivery mode
    * @param deliveryMode The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSDeliveryMode(int deliveryMode) throws JMSException
   {
      if (trace)
         log.trace("setJMSDeliveryMode(" + deliveryMode + ")");

      message.setJMSDeliveryMode(deliveryMode);
   }

   /**
    * Set destination
    * @param destination The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSDestination(Destination destination) throws JMSException
   {
      if (trace)
         log.trace("setJMSDestination(" + destination + ")");

      message.setJMSDestination(destination);
   }
   
   /**
    * Set expiration
    * @param expiration The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSExpiration(long expiration) throws JMSException
   {
      if (trace)
         log.trace("setJMSExpiration(" + expiration + ")");

      message.setJMSExpiration(expiration);
   }
   
   /**
    * Set message id
    * @param id The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSMessageID(String id) throws JMSException
   {
      if (trace)
         log.trace("setJMSMessageID(" + id + ")");

      message.setJMSMessageID(id);
   }
   
   /**
    * Set priority
    * @param priority The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSPriority(int priority) throws JMSException
   {
      if (trace)
         log.trace("setJMSPriority(" + priority + ")");

      message.setJMSPriority(priority);
   }
   
   /**
    * Set redelivered status
    * @param redelivered The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSRedelivered(boolean redelivered) throws JMSException
   {
      if (trace)
         log.trace("setJMSRedelivered(" + redelivered + ")");

      message.setJMSRedelivered(redelivered);
   }

   /**
    * Set reply to
    * @param replyTo The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSReplyTo(Destination replyTo) throws JMSException
   {
      if (trace)
         log.trace("setJMSReplyTo(" + replyTo + ")");

      message.setJMSReplyTo(replyTo);
   }

   /**
    * Set timestamp
    * @param timestamp The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSTimestamp(long timestamp) throws JMSException
   {
      if (trace)
         log.trace("setJMSTimestamp(" + timestamp + ")");

      message.setJMSTimestamp(timestamp);
   }
   
   /**
    * Set type
    * @param type The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSType(String type) throws JMSException
   {
      if (trace)
         log.trace("setJMSType(" + type + ")");

      message.setJMSType(type);
   }
   
   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setLongProperty(String name, long value) throws JMSException
   {
      if (trace)
         log.trace("setLongProperty(" + name + ", " + value + ")");

      message.setLongProperty(name, value);
   }
   
   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setObjectProperty(String name, Object value) throws JMSException
   {
      if (trace)
         log.trace("setObjectProperty(" + name + ", " + value + ")");

      message.setObjectProperty(name, value);
   }
   
   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setShortProperty(String name, short value) throws JMSException
   {
      if (trace)
         log.trace("setShortProperty(" + name + ", " + value + ")");

      message.setShortProperty(name, value);
   }
   
   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setStringProperty(String name, String value) throws JMSException
   {
      if (trace)
         log.trace("setStringProperty(" + name + ", " + value + ")");

      message.setStringProperty(name, value);
   }
   
   /**
    * Return the hash code
    * @return The hash code
    */
   public int hashCode()
   {
      if (trace)
         log.trace("hashCode()");

      return message.hashCode();
   }
   
   /**
    * Check for equality
    * @param object The other object
    * @return True / false
    */
   public boolean equals(Object object)
   {
      if (trace)
         log.trace("equals(" + object + ")");

      if (object != null && object instanceof JBMMessage)
         return message.equals(((JBMMessage) object).message);
      else
         return message.equals(object);
   }
   
   /**
    * Return string representation
    * @return The string
    */
   public String toString()
   {
      if (trace)
         log.trace("toString()");

      return message.toString();
   }
}
