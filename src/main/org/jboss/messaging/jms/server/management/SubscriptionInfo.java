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
public class SubscriptionInfo
{
   // Constants -----------------------------------------------------

   public static final CompositeType TYPE;
   private static final TabularType TABULAR_TYPE;
   private static final String SUBSCRIPTION_TYPE_NAME = "SubscriptionInfo";
   private static final String SUBSCRIPTION_TABULAR_TYPE_NAME = "SubscriptionTabularInfo";
   private static final String[] ITEM_NAMES = new String[] { "queueName", "clientID",
         "name", "durable", "selector", "messageCount", "maxSizeBytes" };
   private static final String[] ITEM_DESCRIPTIONS = new String[] {
         "ID of the subscription", "ClientID of the subscription",
         "name of the subscription", "Is the subscriber durable?", "Selector",
         "Number of messages", "Maximum size in bytes" };
   private static final OpenType[] ITEM_TYPES = new OpenType[] { STRING,
         STRING, STRING, BOOLEAN, STRING, INTEGER, INTEGER };

   static
   {
      try
      {
         TYPE = createSubscriptionInfoType();
         TABULAR_TYPE = createSubscriptionInfoTabularType();
      } catch (OpenDataException e)
      {
         throw new IllegalStateException(e);
      }
   }

   // Attributes ----------------------------------------------------

   private final String queueName;
   private final String clientID;
   private final String name;
   private final boolean durable;
   private final String selector;
   private final int messageCount;
   private final int maxSizeBytes;

   // Static --------------------------------------------------------

   public static TabularData toTabularData(final SubscriptionInfo[] infos)
   {
      TabularData data = new TabularDataSupport(TABULAR_TYPE);
      for (SubscriptionInfo subscriberInfo : infos)
      {
         data.put(subscriberInfo.toCompositeData());
      }
      return data;
   }

   private static CompositeType createSubscriptionInfoType()
         throws OpenDataException
   {
      return new CompositeType(SUBSCRIPTION_TYPE_NAME,
            "Information for a Topic Subscription", ITEM_NAMES,
            ITEM_DESCRIPTIONS, ITEM_TYPES);
   }

   private static TabularType createSubscriptionInfoTabularType()
         throws OpenDataException
   {
      return new TabularType(SUBSCRIPTION_TABULAR_TYPE_NAME,
            "Table of SubscriptionInfo", TYPE, new String[] { "queueName" });
   }

   // Constructors --------------------------------------------------

   public SubscriptionInfo(final String queueName, final String clientID,
         final String name, final boolean durable, final String selector,
         final int messageCount, final int maxSizeBytes)
   {
      this.queueName = queueName;
      this.clientID = clientID;
      this.name = name;
      this.durable = durable;
      this.selector = selector;
      this.messageCount = messageCount;
      this.maxSizeBytes = maxSizeBytes;
   }

   // Public --------------------------------------------------------

   public String getQueueName()
   {
      return queueName;
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
         return new CompositeDataSupport(TYPE, ITEM_NAMES, new Object[] { queueName,
               clientID, name, durable, selector, messageCount, maxSizeBytes });
      } catch (OpenDataException e)
      {
         return null;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
