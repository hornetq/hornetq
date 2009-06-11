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

import org.jboss.messaging.utils.json.JSONArray;
import org.jboss.messaging.utils.json.JSONObject;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SubscriptionInfo
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String queueName;

   private final String clientID;

   private final String name;

   private final boolean durable;

   private final String selector;

   private final int messageCount;

   // Static --------------------------------------------------------

   public static SubscriptionInfo[] from(String jsonString) throws Exception
   {
      JSONArray array = new JSONArray(jsonString);
      SubscriptionInfo[] infos = new SubscriptionInfo[array.length()];
      for (int i = 0; i < array.length(); i++)
      {
         JSONObject sub = array.getJSONObject(i);
         SubscriptionInfo info = new SubscriptionInfo(sub.getString("queueName"),
                                                      sub.optString("clientID", null),
                                                      sub.optString("name", null),
                                                      sub.getBoolean("durable"),
                                                      sub.optString("selector", null),
                                                      sub.getInt("messageCount"));
         infos[i] = info;
      }

      return infos;
   }

   // Constructors --------------------------------------------------

   private SubscriptionInfo(final String queueName,
                            final String clientID,
                            final String name,
                            final boolean durable,
                            final String selector,
                            final int messageCount)
   {
      this.queueName = queueName;
      this.clientID = clientID;
      this.name = name;
      this.durable = durable;
      this.selector = selector;
      this.messageCount = messageCount;
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
