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

package org.jboss.messaging.core.management;

import static javax.management.openmbean.SimpleType.BOOLEAN;
import static javax.management.openmbean.SimpleType.BYTE;
import static javax.management.openmbean.SimpleType.INTEGER;
import static javax.management.openmbean.SimpleType.LONG;
import static javax.management.openmbean.SimpleType.STRING;

import java.util.ArrayList;
import java.util.Collection;
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

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessageInfo
{
   // Constants -----------------------------------------------------

   public static final CompositeType TYPE;

   private static final String MESSAGE_TYPE_NAME = "MessageInfo";

   private static final String MESSAGE_TABULAR_TYPE_NAME = "MessageTabularInfo";

   private static final String[] ITEM_NAMES = new String[] { "id",
                                                            "destination",
                                                            "durable",
                                                            "timestamp",
                                                            "type",
                                                            "size",
                                                            "priority",
                                                            "expired",
                                                            "expiration",
                                                            "properties" };

   private static final String[] ITEM_DESCRIPTIONS = new String[] { "Message ID",
                                                                   "destination of the message",
                                                                   "Is the message durable?",
                                                                   "Timestamp of the message",
                                                                   "Type of the message",
                                                                   "Size of the encoded messag",
                                                                   "Priority of the message",
                                                                   "Is the message expired?",
                                                                   "Expiration of the message",
                                                                   "Properties of the message" };

   private static final OpenType[] TYPES;

   private static final TabularType TABULAR_TYPE;

   static
   {
      try
      {
         TYPES = new OpenType[] { LONG,
                                 STRING,
                                 BOOLEAN,
                                 LONG,
                                 BYTE,
                                 INTEGER,
                                 BYTE,
                                 BOOLEAN,
                                 LONG,
                                 PropertiesInfo.TABULAR_TYPE };
         TYPE = new CompositeType(MESSAGE_TYPE_NAME, "Information for a Message", ITEM_NAMES, ITEM_DESCRIPTIONS, TYPES);
         TABULAR_TYPE = new TabularType(MESSAGE_TABULAR_TYPE_NAME,
                                        "Information for tabular MessageInfo",
                                        TYPE,
                                        new String[] { "id" });
      }
      catch (OpenDataException e)
      {
         e.printStackTrace();
         throw new IllegalStateException(e);
      }
   }

   // Attributes ----------------------------------------------------

   private final long id;

   private final String destination;

   private final boolean durable;

   private final long timestamp;

   private final byte type;

   private final int size;

   private final byte priority;

   private final boolean expired;

   private final long expiration;

   private PropertiesInfo properties;

   // Static --------------------------------------------------------

   public static TabularData toTabularData(final MessageInfo[] infos) throws OpenDataException
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);
      for (MessageInfo messageInfo : infos)
      {
         data.put(messageInfo.toCompositeData());
      }
      return data;
   }

   public static MessageInfo[] from(TabularData msgs)
   {
      Collection values = msgs.values();
      List<MessageInfo> infos = new ArrayList<MessageInfo>();
      for (Object object : values)
      {
         CompositeData compositeData = (CompositeData)object;
         long id = (Long)compositeData.get("id");
         String destination = (String)compositeData.get("destination");
         boolean durable = (Boolean)compositeData.get("durable");
         long timestamp = (Long)compositeData.get("timestamp");
         byte type = (Byte)compositeData.get("type");
         int size = (Integer)compositeData.get("size");
         byte priority = (Byte)compositeData.get("priority");
         boolean expired = (Boolean)compositeData.get("expired");
         long expiration = (Long)compositeData.get("expiration");

         TabularData properties = (TabularData)compositeData.get("properties");
         PropertiesInfo propertiesInfo = PropertiesInfo.from(properties);
         infos.add(new MessageInfo(id, destination, durable, timestamp, type, size, priority, expired, expiration, propertiesInfo));
      }

      return (MessageInfo[])infos.toArray(new MessageInfo[infos.size()]);
   }

   // Constructors --------------------------------------------------

   public MessageInfo(final long id,
                      final String destination,
                      final boolean durable,
                      final long timestamp,
                      final byte type,
                      final int size,
                      final byte priority,
                      final boolean expired,
                      final long expiration)
   {
      this.id = id;
      this.destination = destination;
      this.durable = durable;
      this.timestamp = timestamp;
      this.type = type;
      this.size = size;
      this.priority = priority;
      this.expired = expired;
      this.expiration = expiration;
      this.properties = new PropertiesInfo();
   }

   public MessageInfo(final long id,
                      final String destination,
                      final boolean durable,
                      final long timestamp,
                      final byte type,
                      final int size,
                      final byte priority,
                      final boolean expired,
                      final long expiration,
                      final PropertiesInfo properties)
   {
      this.id = id;
      this.destination = destination;
      this.durable = durable;
      this.timestamp = timestamp;
      this.type = type;
      this.size = size;
      this.priority = priority;
      this.expired = expired;
      this.expiration = expiration;
      this.properties = properties;
   }

   // Public --------------------------------------------------------

   public long getID()
   {
      return id;
   }

   public String getDestination()
   {
      return destination;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public long getTimestamp()
   {
      return timestamp;
   }

   public byte getType()
   {
      return type;
   }

   public int getSize()
   {
      return size;
   }

   public byte getPriority()
   {
      return priority;
   }

   public boolean isExpired()
   {
      return expired;
   }

   public long getExpiration()
   {
      return expiration;
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

         return new CompositeDataSupport(TYPE, ITEM_NAMES, new Object[] { id,
                                                                         destination,
                                                                         durable,
                                                                         timestamp,
                                                                         type,
                                                                         size,
                                                                         priority,
                                                                         expired,
                                                                         expiration,
                                                                         properties.toTabularData() });
      }
      catch (OpenDataException e)
      {
         e.printStackTrace();
         return null;
      }
   }

   @Override
   public String toString()
   {
      return "MessageInfo[id=" + id +
             ", destination=" +
             destination +
             ", durable=" +
             durable +
             ", timestamp=" +
             timestamp +
             ", type=" +
             type +
             ", size=" +
             size +
             ", priority=" +
             priority +
             ", expired=" +
             expired +
             ", expiration=" +
             expiration +
             "]";
   }
}
