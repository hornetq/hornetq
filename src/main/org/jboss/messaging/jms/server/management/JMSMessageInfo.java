/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms.server.management;

import static javax.management.openmbean.SimpleType.INTEGER;
import static javax.management.openmbean.SimpleType.LONG;
import static javax.management.openmbean.SimpleType.STRING;

import java.util.List;
import java.util.Map;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.jboss.messaging.core.management.PropertiesInfo;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSMessageInfo
{
   // Constants -----------------------------------------------------

   public static final CompositeType TYPE;
   private static final String MESSAGE_TYPE_NAME = "JMSMessageInfo";
   private static final String MESSAGE_TABULAR_TYPE_NAME = "JMSMessageTabularInfo";
   private static final String[] ITEM_NAMES = new String[] { "JMSMessageID",
         "JMSCorrelationID", "JMSDeliveryMode", "JMSPriority", "JMSReplyTo",
         "JMSTimestamp", "JMSType", "expiration", "properties" };
   private static final String[] ITEM_DESCRIPTIONS = new String[] {
         "Message ID", "Correlation ID", "Delivery Mode", "Priority",
         "Reply To", "Timestamp", "JMS Type", "Expiration", "Properties" };
   private static final OpenType[] TYPES;
   private static final TabularType TABULAR_TYPE;

   static
   {
      try
      {
         TYPES = new OpenType[] { STRING, STRING, STRING, INTEGER, STRING,
               LONG, STRING, LONG, PropertiesInfo.TABULAR_TYPE };
         TYPE = new CompositeType(MESSAGE_TYPE_NAME,
               "Information for a JMS Message", ITEM_NAMES, ITEM_DESCRIPTIONS,
               TYPES);
         TABULAR_TYPE = new TabularType(MESSAGE_TABULAR_TYPE_NAME,
               "Information for tabular JMSMessageInfo", TYPE,
               new String[] { "JMSMessageID" });
      } catch (OpenDataException e)
      {
         e.printStackTrace();
         throw new IllegalStateException(e);
      }
   }

   // Attributes ----------------------------------------------------

   private final String messageID;
   private final String correlationID;
   private final String deliveryMode;
   private final int priority;
   private final String replyTo;
   private final long timestamp;
   private final long expiration;
   private final String jmsType;
   private PropertiesInfo properties;

   // Static --------------------------------------------------------

   public static TabularData toTabularData(JMSMessageInfo[] infos)
         throws OpenDataException
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);
      for (JMSMessageInfo messageInfo : infos)
      {
         data.put(messageInfo.toCompositeData());
      }
      return data;
   }

   public static TabularData toTabularData(List<JMSMessageInfo> infos)
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);
      for (JMSMessageInfo messageInfo : infos)
      {
         data.put(messageInfo.toCompositeData());
      }
      return data;
   }

   public static JMSMessageInfo fromServerMessage(ServerMessage message)
   {
      String messageID = message.getProperty(JBossMessage.JBM_MESSAGE_ID)
            .toString();
      SimpleString simpleCorrelationID = (SimpleString) message
            .getProperty(JBossMessage.CORRELATIONID_HEADER_NAME);
      String correlationID = (simpleCorrelationID == null) ? null
            : simpleCorrelationID.toString();
      SimpleString simpleJMSType = (SimpleString) message
            .getProperty(JBossMessage.TYPE_HEADER_NAME);
      String jmsType = (simpleJMSType == null) ? null : simpleJMSType
            .toString();
      String deliveryMode = message.isDurable() ? "PERSISTENT"
            : "NON_PERSISTENT";
      int priority = message.getPriority();
      SimpleString replyAddress = (SimpleString) message
            .getProperty(JBossMessage.REPLYTO_HEADER_NAME);
      String replyTo = (replyAddress == null) ? null : replyAddress.toString();
      long timestamp = message.getTimestamp();
      long expiration = message.getExpiration();

      JMSMessageInfo info = new JMSMessageInfo(messageID, correlationID,
            deliveryMode, priority, replyTo, timestamp, expiration, jmsType);
      for (SimpleString key : message.getPropertyNames())
      {
         info.putProperty(key.toString(), message.getProperty(key).toString());
      }
      return info;
   }

   // Constructors --------------------------------------------------

   public JMSMessageInfo(final String messageID, final String correlationID,
         final String deliveryMode, final int priority, final String replyTo,
         final long timestamp, final long expiration, final String jmsType)
   {
      this.messageID = messageID;
      this.correlationID = correlationID;
      this.deliveryMode = deliveryMode;
      this.priority = priority;
      this.replyTo = replyTo;
      this.timestamp = timestamp;
      this.expiration = expiration;
      this.jmsType = jmsType;
      this.properties = new PropertiesInfo();
   }

   // Public --------------------------------------------------------

   public String getJMSMessageID()
   {
      return messageID;
   }

   public String getJMSCorrelationID()
   {
      return correlationID;
   }

   public String getJMSDeliveryMode()
   {
      return deliveryMode;
   }

   public int getJMSPriority()
   {
      return priority;
   }

   public String getJMSReplyTo()
   {
      return replyTo;
   }

   public long getJMSTimestamp()
   {
      return timestamp;
   }

   public long getExpiration()
   {
      return expiration;
   }

   public String getJMSType()
   {
      return jmsType;
   }

   public void putProperty(final String key, final String value)
   {
      properties.put(key, value);
   }

   public Map<String, String> getProperties()
   {
      return properties.entries();
   }

   public CompositeData toCompositeData()
   {
      try
      {
         return new CompositeDataSupport(TYPE, ITEM_NAMES, new Object[] {
               messageID, correlationID, deliveryMode, priority, replyTo,
               timestamp, jmsType, expiration, properties.toTabularData() });
      } catch (OpenDataException e)
      {
         e.printStackTrace();
         return null;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
