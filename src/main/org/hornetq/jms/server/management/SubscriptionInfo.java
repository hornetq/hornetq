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

package org.hornetq.jms.server.management;

import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

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
