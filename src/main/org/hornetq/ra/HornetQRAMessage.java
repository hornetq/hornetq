/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.ra;

import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.hornetq.core.logging.Logger;

/**
 * A wrapper for a message
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class HornetQRAMessage implements Message
{
   /** The logger */
   private static final Logger log = Logger.getLogger(HornetQRAMessage.class);

   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The message */
   protected Message message;

   /** The session */
   protected HornetQRASession session;

   /**
    * Create a new wrapper
    * @param message the message
    * @param session the session
    */
   public HornetQRAMessage(final Message message, final HornetQRASession session)
   {
      if (trace)
      {
         log.trace("constructor(" + message + ", " + session + ")");
      }

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
      {
         log.trace("acknowledge()");
      }

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
      {
         log.trace("clearBody()");
      }

      message.clearBody();
   }

   /**
    * Clear properties
    * @exception JMSException Thrown if an error occurs
    */
   public void clearProperties() throws JMSException
   {
      if (trace)
      {
         log.trace("clearProperties()");
      }

      message.clearProperties();
   }

   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public boolean getBooleanProperty(final String name) throws JMSException
   {
      if (trace)
      {
         log.trace("getBooleanProperty(" + name + ")");
      }

      return message.getBooleanProperty(name);
   }

   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public byte getByteProperty(final String name) throws JMSException
   {
      if (trace)
      {
         log.trace("getByteProperty(" + name + ")");
      }

      return message.getByteProperty(name);
   }

   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public double getDoubleProperty(final String name) throws JMSException
   {
      if (trace)
      {
         log.trace("getDoubleProperty(" + name + ")");
      }

      return message.getDoubleProperty(name);
   }

   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public float getFloatProperty(final String name) throws JMSException
   {
      if (trace)
      {
         log.trace("getFloatProperty(" + name + ")");
      }

      return message.getFloatProperty(name);
   }

   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public int getIntProperty(final String name) throws JMSException
   {
      if (trace)
      {
         log.trace("getIntProperty(" + name + ")");
      }

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
      {
         log.trace("getJMSCorrelationID()");
      }

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
      {
         log.trace("getJMSCorrelationIDAsBytes()");
      }

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
      {
         log.trace("getJMSDeliveryMode()");
      }

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
      {
         log.trace("getJMSDestination()");
      }

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
      {
         log.trace("getJMSExpiration()");
      }

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
      {
         log.trace("getJMSMessageID()");
      }

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
      {
         log.trace("getJMSPriority()");
      }

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
      {
         log.trace("getJMSRedelivered()");
      }

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
      {
         log.trace("getJMSReplyTo()");
      }

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
      {
         log.trace("getJMSTimestamp()");
      }

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
      {
         log.trace("getJMSType()");
      }

      return message.getJMSType();
   }

   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public long getLongProperty(final String name) throws JMSException
   {
      if (trace)
      {
         log.trace("getLongProperty(" + name + ")");
      }

      return message.getLongProperty(name);
   }

   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public Object getObjectProperty(final String name) throws JMSException
   {
      if (trace)
      {
         log.trace("getObjectProperty(" + name + ")");
      }

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
      {
         log.trace("getPropertyNames()");
      }

      return message.getPropertyNames();
   }

   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public short getShortProperty(final String name) throws JMSException
   {
      if (trace)
      {
         log.trace("getShortProperty(" + name + ")");
      }

      return message.getShortProperty(name);
   }

   /**
    * Get property
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public String getStringProperty(final String name) throws JMSException
   {
      if (trace)
      {
         log.trace("getStringProperty(" + name + ")");
      }

      return message.getStringProperty(name);
   }

   /**
    * Do property exist
    * @param name The name
    * @return The value
    * @exception JMSException Thrown if an error occurs
    */
   public boolean propertyExists(final String name) throws JMSException
   {
      if (trace)
      {
         log.trace("propertyExists(" + name + ")");
      }

      return message.propertyExists(name);
   }

   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setBooleanProperty(final String name, final boolean value) throws JMSException
   {
      if (trace)
      {
         log.trace("setBooleanProperty(" + name + ", " + value + ")");
      }

      message.setBooleanProperty(name, value);
   }

   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setByteProperty(final String name, final byte value) throws JMSException
   {
      if (trace)
      {
         log.trace("setByteProperty(" + name + ", " + value + ")");
      }

      message.setByteProperty(name, value);
   }

   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setDoubleProperty(final String name, final double value) throws JMSException
   {
      if (trace)
      {
         log.trace("setDoubleProperty(" + name + ", " + value + ")");
      }

      message.setDoubleProperty(name, value);
   }

   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setFloatProperty(final String name, final float value) throws JMSException
   {
      if (trace)
      {
         log.trace("setFloatProperty(" + name + ", " + value + ")");
      }

      message.setFloatProperty(name, value);
   }

   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setIntProperty(final String name, final int value) throws JMSException
   {
      if (trace)
      {
         log.trace("setIntProperty(" + name + ", " + value + ")");
      }

      message.setIntProperty(name, value);
   }

   /**
    * Set correlation id
    * @param correlationID The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSCorrelationID(final String correlationID) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSCorrelationID(" + correlationID + ")");
      }

      message.setJMSCorrelationID(correlationID);
   }

   /**
    * Set correlation id
    * @param correlationID The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSCorrelationIDAsBytes(final byte[] correlationID) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSCorrelationIDAsBytes(" + correlationID + ")");
      }

      message.setJMSCorrelationIDAsBytes(correlationID);
   }

   /**
    * Set delivery mode
    * @param deliveryMode The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSDeliveryMode(final int deliveryMode) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSDeliveryMode(" + deliveryMode + ")");
      }

      message.setJMSDeliveryMode(deliveryMode);
   }

   /**
    * Set destination
    * @param destination The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSDestination(final Destination destination) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSDestination(" + destination + ")");
      }

      message.setJMSDestination(destination);
   }

   /**
    * Set expiration
    * @param expiration The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSExpiration(final long expiration) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSExpiration(" + expiration + ")");
      }

      message.setJMSExpiration(expiration);
   }

   /**
    * Set message id
    * @param id The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSMessageID(final String id) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSMessageID(" + id + ")");
      }

      message.setJMSMessageID(id);
   }

   /**
    * Set priority
    * @param priority The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSPriority(final int priority) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSPriority(" + priority + ")");
      }

      message.setJMSPriority(priority);
   }

   /**
    * Set redelivered status
    * @param redelivered The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSRedelivered(final boolean redelivered) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSRedelivered(" + redelivered + ")");
      }

      message.setJMSRedelivered(redelivered);
   }

   /**
    * Set reply to
    * @param replyTo The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSReplyTo(final Destination replyTo) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSReplyTo(" + replyTo + ")");
      }

      message.setJMSReplyTo(replyTo);
   }

   /**
    * Set timestamp
    * @param timestamp The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSTimestamp(final long timestamp) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSTimestamp(" + timestamp + ")");
      }

      message.setJMSTimestamp(timestamp);
   }

   /**
    * Set type
    * @param type The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setJMSType(final String type) throws JMSException
   {
      if (trace)
      {
         log.trace("setJMSType(" + type + ")");
      }

      message.setJMSType(type);
   }

   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setLongProperty(final String name, final long value) throws JMSException
   {
      if (trace)
      {
         log.trace("setLongProperty(" + name + ", " + value + ")");
      }

      message.setLongProperty(name, value);
   }

   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setObjectProperty(final String name, final Object value) throws JMSException
   {
      if (trace)
      {
         log.trace("setObjectProperty(" + name + ", " + value + ")");
      }

      message.setObjectProperty(name, value);
   }

   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setShortProperty(final String name, final short value) throws JMSException
   {
      if (trace)
      {
         log.trace("setShortProperty(" + name + ", " + value + ")");
      }

      message.setShortProperty(name, value);
   }

   /**
    * Set property
    * @param name The name
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setStringProperty(final String name, final String value) throws JMSException
   {
      if (trace)
      {
         log.trace("setStringProperty(" + name + ", " + value + ")");
      }

      message.setStringProperty(name, value);
   }

   /**
    * Return the hash code
    * @return The hash code
    */
   @Override
   public int hashCode()
   {
      if (trace)
      {
         log.trace("hashCode()");
      }

      return message.hashCode();
   }

   /**
    * Check for equality
    * @param object The other object
    * @return True / false
    */
   @Override
   public boolean equals(final Object object)
   {
      if (trace)
      {
         log.trace("equals(" + object + ")");
      }

      if (object != null && object instanceof HornetQRAMessage)
      {
         return message.equals(((HornetQRAMessage)object).message);
      }
      else
      {
         return message.equals(object);
      }
   }

   /**
    * Return string representation
    * @return The string
    */
   @Override
   public String toString()
   {
      if (trace)
      {
         log.trace("toString()");
      }

      return message.toString();
   }
}
