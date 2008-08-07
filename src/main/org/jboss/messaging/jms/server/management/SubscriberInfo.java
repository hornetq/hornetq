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

import static javax.management.openmbean.SimpleType.BOOLEAN;
import static javax.management.openmbean.SimpleType.INTEGER;
import static javax.management.openmbean.SimpleType.STRING;

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
public class SubscriberInfo
{
   // Constants -----------------------------------------------------

   public static final CompositeType TYPE;
   private static final TabularType TABULAR_TYPE;
   private static final String SUBSCRIBER_TYPE_NAME = "SubscriberInfo";
   private static final String SUBSCRIBER_TABULAR_TYPE_NAME = "SubscriberTabularInfo";
   private static final String[] ITEM_NAMES = new String[] { "id", "clientID",
         "name", "durable", "selector", "messageCount", "maxSizeBytes" };
   private static final String[] ITEM_DESCRIPTIONS = new String[] {
         "ID of the subscriber", "ClientID of the subscription",
         "name of the subscription", "Is the subscriber durable?", "Selector",
         "Number of messages", "Maximum size in bytes" };
   private static final OpenType[] ITEM_TYPES = new OpenType[] { STRING,
         STRING, STRING, BOOLEAN, STRING, INTEGER, INTEGER };

   static
   {
      try
      {
         TYPE = createSubscriberInfoType();
         TABULAR_TYPE = createSubscriberInfoTabularType();
      } catch (OpenDataException e)
      {
         throw new IllegalStateException(e);
      }
   }

   // Attributes ----------------------------------------------------

   private final String id;
   private final String clientID;
   private final String name;
   private final boolean durable;
   private final String selector;
   private final int messageCount;
   private final int maxSizeBytes;

   // Static --------------------------------------------------------

   public static TabularData toTabularData(final SubscriberInfo[] infos)
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);
      for (SubscriberInfo subscriberInfo : infos)
      {
         data.put(subscriberInfo.toCompositeData());
      }
      return data;
   }

   private static CompositeType createSubscriberInfoType()
         throws OpenDataException
   {
      return new CompositeType(SUBSCRIBER_TYPE_NAME,
            "Information for a Topic Subscriber", ITEM_NAMES,
            ITEM_DESCRIPTIONS, ITEM_TYPES);
   }

   private static TabularType createSubscriberInfoTabularType()
         throws OpenDataException
   {
      return new TabularType(SUBSCRIBER_TABULAR_TYPE_NAME,
            "Table of SubscriberInfo", TYPE, new String[] { "id" });
   }

   // Constructors --------------------------------------------------

   public SubscriberInfo(final String id, final String clientID,
         final String name, final boolean durable, final String selector,
         final int messageCount, final int maxSizeBytes)
   {
      this.id = id;
      this.clientID = clientID;
      this.name = name;
      this.durable = durable;
      this.selector = selector;
      this.messageCount = messageCount;
      this.maxSizeBytes = maxSizeBytes;
   }

   // Public --------------------------------------------------------

   public String getID()
   {
      return id;
   }

   public String getClientID()
   {
      return clientID;
   }

   public String getName()
   {
      return name;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public String getSelector()
   {
      return selector;
   }

   public int getMessageCount()
   {
      return messageCount;
   }

   public int getMaxSizeBytes()
   {
      return maxSizeBytes;
   }

   public CompositeData toCompositeData()
   {
      try
      {
         return new CompositeDataSupport(TYPE, ITEM_NAMES, new Object[] { id,
               clientID, name, durable, selector, messageCount, maxSizeBytes });
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
