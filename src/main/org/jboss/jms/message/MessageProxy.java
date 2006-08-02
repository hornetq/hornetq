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

import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashMap;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;

import org.jboss.jms.delegate.SessionDelegate;

/**
 * 
 * MessageProxy is a thin proxy for JBossMessage.
 *
 * JMS Users actually handle MessageProxy instances rather than JBossMessage instances. The purpose
 * of this class and subclasses is to prevent unnecessary copying of a message. After a message is
 * sent, the message can be changed, but this should not affect the sent message. This class
 * accomplishes this by intercepting any methods which change the state of the message and copying
 * either the headers, jms properties or body as appropriate. This enables up to make the minimum
 * amount of copies while still preserving JMS semantics. Similarly on receive.
 *
 * See JMS1.1 Spec 3.9, 3.10 for more details.
 *
 * If nothing is changed, nothing is copied.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * MessageProxy.java,v 1.1 2006/03/08 08:00:34 timfox Exp
 */
public class MessageProxy implements Message, Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 5903095946142192468L;
   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected JBossMessage message;

   protected transient SessionDelegate delegate;
   
   protected transient boolean cc;

   protected transient boolean messageCopied;

   protected transient boolean propertiesCopied;

   protected transient boolean bodyCopied;

   protected transient int state;

   protected static final int STATE_NEW = 0;

   protected static final int STATE_SENT = 1;

   protected static final int STATE_RECEIVED = 2;

   protected transient boolean propertiesReadOnly;

   protected transient boolean bodyReadOnly;

   protected int deliveryCount;

   protected transient boolean jmsRedelivered;

   // Constructors --------------------------------------------------

   public MessageProxy()
   {
   }

   public MessageProxy(JBossMessage message, int deliveryCount)
   {
      this.message = message;
      this.state = STATE_NEW;
      this.deliveryCount = deliveryCount;
   }

   // Message implementation ----------------------------------------

   public String getJMSMessageID() throws JMSException
   {
      return message.getJMSMessageID();
   }

   public void setJMSMessageID(String id) throws JMSException
   {
      headerChange();
      message.setJMSMessageID(id);
   }

   public long getJMSTimestamp() throws JMSException
   {
      return message.getJMSTimestamp();
   }

   public void setJMSTimestamp(long timestamp) throws JMSException
   {
      headerChange();
      message.setJMSTimestamp(timestamp);
   }

   public byte[] getJMSCorrelationIDAsBytes() throws JMSException
   {
      return message.getJMSCorrelationIDAsBytes();
   }

   public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException
   {
      headerChange();
      message.setJMSCorrelationIDAsBytes(correlationID);
   }

   public void setJMSCorrelationID(String correlationID) throws JMSException
   {
      headerChange();
      message.setJMSCorrelationID(correlationID);
   }

   public String getJMSCorrelationID() throws JMSException
   {
      return message.getJMSCorrelationID();
   }

   public Destination getJMSReplyTo() throws JMSException
   {
      return message.getJMSReplyTo();
   }

   public void setJMSReplyTo(Destination replyTo) throws JMSException
   {
      headerChange();
      message.setJMSReplyTo(replyTo);
   }

   public Destination getJMSDestination() throws JMSException
   {
      return message.getJMSDestination();
   }

   public void setJMSDestination(Destination destination) throws JMSException
   {
      headerChange();
      message.setJMSDestination(destination);
   }

   public int getJMSDeliveryMode() throws JMSException
   {
      return message.getJMSDeliveryMode();
   }

   public void setJMSDeliveryMode(int deliveryMode) throws JMSException
   {
      headerChange();
      message.setJMSDeliveryMode(deliveryMode);
   }

   public boolean getJMSRedelivered() throws JMSException
   {
      //Always handled in the delegate
      return jmsRedelivered;
   }

   public void setJMSRedelivered(boolean redelivered) throws JMSException
   {
      //Always handled in the delegate
      jmsRedelivered = redelivered;
   }

   public String getJMSType() throws JMSException
   {
      return message.getJMSType();
   }

   public void setJMSType(String type) throws JMSException
   {
      headerChange();
      message.setJMSType(type);
   }

   public long getJMSExpiration() throws JMSException
   {
      return message.getJMSExpiration();
   }

   public void setJMSExpiration(long expiration) throws JMSException
   {
      headerChange();
      message.setJMSExpiration(expiration);
   }

   public int getJMSPriority() throws JMSException
   {
      return message.getJMSPriority();
   }

   public void setJMSPriority(int priority) throws JMSException
   {
      headerChange();
      message.setJMSPriority(priority);
   }

   public void clearProperties() throws JMSException
   {
      propertiesClear();
      message.clearProperties();
      propertiesReadOnly = false;
   }

   public boolean propertyExists(String name) throws JMSException
   {
      return message.propertyExists(name);
   }

   public boolean getBooleanProperty(String name) throws JMSException
   {
      return message.getBooleanProperty(name);
   }

   public byte getByteProperty(String name) throws JMSException
   {
      return message.getByteProperty(name);
   }

   public short getShortProperty(String name) throws JMSException
   {
      return message.getShortProperty(name);
   }

   public int getIntProperty(String name) throws JMSException
   {
      if ("JMSXDeliveryCount".equals(name))
      {
         return deliveryCount;
      }
      return message.getIntProperty(name);
   }

   public long getLongProperty(String name) throws JMSException
   {
      return message.getLongProperty(name);
   }

   public float getFloatProperty(String name) throws JMSException
   {
      return message.getFloatProperty(name);
   }

   public double getDoubleProperty(String name) throws JMSException
   {
      return message.getDoubleProperty(name);
   }

   public String getStringProperty(String name) throws JMSException
   {
      return message.getStringProperty(name);
   }

   public Object getObjectProperty(String name) throws JMSException
   {
      return message.getObjectProperty(name);
   }

   public Enumeration getPropertyNames() throws JMSException
   {
      return message.getPropertyNames();
   }

   public void setBooleanProperty(String name, boolean value) throws JMSException
   {
      if (propertiesReadOnly)
         throw new MessageNotWriteableException("Properties are read-only");
      propertyChange();
      message.setBooleanProperty(name, value);
   }

   public void setByteProperty(String name, byte value) throws JMSException
   {
      if (propertiesReadOnly)
         throw new MessageNotWriteableException("Properties are read-only");
      propertyChange();
      message.setByteProperty(name, value);
   }

   public void setShortProperty(String name, short value) throws JMSException
   {
      if (propertiesReadOnly)
         throw new MessageNotWriteableException("Properties are read-only");
      propertyChange();
      message.setShortProperty(name, value);
   }

   public void setIntProperty(String name, int value) throws JMSException
   {
      if (propertiesReadOnly)
         throw new MessageNotWriteableException("Properties are read-only");
      propertyChange();
      message.setIntProperty(name, value);
   }

   public void setLongProperty(String name, long value) throws JMSException
   {
      if (propertiesReadOnly)
         throw new MessageNotWriteableException("Properties are read-only");
      propertyChange();
      message.setLongProperty(name, value);
   }

   public void setFloatProperty(String name, float value) throws JMSException
   {
      if (propertiesReadOnly)
         throw new MessageNotWriteableException("Properties are read-only");
      propertyChange();
      message.setFloatProperty(name, value);
   }

   public void setDoubleProperty(String name, double value) throws JMSException
   {
      if (propertiesReadOnly)
         throw new MessageNotWriteableException("Properties are read-only");
      propertyChange();
      message.setDoubleProperty(name, value);
   }

   public void setStringProperty(String name, String value) throws JMSException
   {
      if (propertiesReadOnly)
         throw new MessageNotWriteableException("Properties are read-only");
      propertyChange();
      message.setStringProperty(name, value);
   }

   public void setObjectProperty(String name, Object value) throws JMSException
   {
      if (propertiesReadOnly)
         throw new MessageNotWriteableException("Properties are read-only");
      propertyChange();
      message.setObjectProperty(name, value);
   }

   public void acknowledge() throws JMSException
   {
      if (!cc)
      {
         //Only acknowledge for client ack if is not in connection consumer
         delegate.acknowledgeAll();
      }
   }

   public void clearBody() throws JMSException
   {
      bodyClear();
      message.clearBody();
      bodyReadOnly = false;
   }


   // Public --------------------------------------------------------

   public void setSessionDelegate(SessionDelegate sd, boolean isConnectionConsumer)
   {
      this.delegate = sd;
      this.cc = isConnectionConsumer;
   }
   
   public SessionDelegate getSessionDelegate()
   {
      return delegate;
   }

   public void setSent()
   {
      state = STATE_SENT;
   }

   public void setReceived()
   {
      state = STATE_RECEIVED;

      propertiesReadOnly = true;

      bodyReadOnly = true;

      this.jmsRedelivered = deliveryCount > 0;
   }

   public JBossMessage getMessage()
   {
      return message;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }
   
   public void incDeliveryCount()
   {
      this.deliveryCount++;
   }

   public String toString()
   {
      return "delegator->" + message;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void headerChange() throws JMSException
   {
      if ((isSent() || isReceived()))
      {
         // A header value is to be changed - we must make a shallow copy of the message.
         // This basically only copies the headers
         copyMessage();
      }
   }

   protected void copyMessage() throws JMSException
   {
      if (!messageCopied)
      {
         message = message.doShallowCopy();
         messageCopied = true;
      }
   }

   public boolean isSent()
   {
      return state == STATE_SENT;
   }

   public boolean isReceived()
   {
      return state == STATE_RECEIVED;
   }

   protected void propertyChange() throws JMSException
   {
      if (!propertiesCopied)
      {
         if (isSent())
         {
            //The message has been sent - we need to copy properties to avoid changing the properties
            //of the sent message
            copyMessage();
            message.setJMSProperties(new HashMap(message.getJMSProperties()));

         }
         else if (isReceived())
         {
            //No need to copy - any attempt to set read only props will throw an exception
         }
      }
   }

   protected void propertiesClear() throws JMSException
   {
      if (isSent() || isReceived())
      {
         copyMessage();
         message.setJMSProperties(new HashMap());
         propertiesCopied = true;
      }
   }

   protected void bodyClear() throws JMSException
   {
      if (isSent() || isReceived())
      {
         copyMessage();
         bodyCopied = true;
      }
   }

   protected void bodyChange() throws JMSException
   {
      if (isSent())
      {
         // The message has been sent - make a copy of the message to avoid changing the sent
         // messages payload
         copyMessage();

         if (!bodyCopied)
         {
            message.copyPayload(message.getPayload());
            bodyCopied = true;
         }
      }
      else if (isReceived())
      {
         // Do nothing - any attempt to change the payload of the message should throw an exception
         // (readonly)
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
}
