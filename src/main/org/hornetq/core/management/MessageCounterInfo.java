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

package org.hornetq.core.management;

import java.text.DateFormat;
import java.util.Date;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.messagecounter.MessageCounter;
import org.hornetq.utils.json.JSONObject;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MessageCounterInfo
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageCounterInfo.class);

   // Attributes ----------------------------------------------------

   private final String name;

   private final String subscription;

   private final boolean durable;

   private final long count;

   private final long countDelta;

   private final int depth;

   private final int depthDelta;

   private final String lastAddTimestamp;

   private final String udpateTimestamp;

   // Static --------------------------------------------------------

   public static String toJSon(final MessageCounter counter) throws Exception
   {
      DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);

      JSONObject json = new JSONObject(counter);
      String lastAddTimestamp = dateFormat.format(new Date(counter.getLastAddedMessageTime()));
      json.put("lastAddTimestamp", lastAddTimestamp);
      String updateTimestamp = dateFormat.format(new Date(counter.getLastUpdate()));
      json.put("updateTimestamp", updateTimestamp);

      return json.toString();
   }

   public static MessageCounterInfo fromJSON(final String jsonString) throws Exception
   {
      JSONObject data = new JSONObject(jsonString);
      String name = data.getString("destinationName");
      String subscription = data.getString("destinationSubscription");
      boolean durable = data.getBoolean("destinationDurable");
      long count = data.getLong("count");
      long countDelta = data.getLong("countDelta");
      int depth = data.getInt("messageCount");
      int depthDelta = data.getInt("messageCountDelta");
      String lastAddTimestamp = data.getString("lastAddTimestamp");
      String updateTimestamp = data.getString("updateTimestamp");

      return new MessageCounterInfo(name,
                                    subscription,
                                    durable,
                                    count,
                                    countDelta,
                                    depth,
                                    depthDelta,
                                    lastAddTimestamp,
                                    updateTimestamp);
   }

   // Constructors --------------------------------------------------

   public MessageCounterInfo(final String name,
                             final String subscription,
                             final boolean durable,
                             final long count,
                             final long countDelta,
                             final int depth,
                             final int depthDelta,
                             final String lastAddTimestamp,
                             final String udpateTimestamp)
   {
      this.name = name;
      this.subscription = subscription;
      this.durable = durable;
      this.count = count;
      this.countDelta = countDelta;
      this.depth = depth;
      this.depthDelta = depthDelta;
      this.lastAddTimestamp = lastAddTimestamp;
      this.udpateTimestamp = udpateTimestamp;
   }

   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
   }

   public String getSubscription()
   {
      return subscription;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public long getCount()
   {
      return count;
   }

   public long getCountDelta()
   {
      return countDelta;
   }

   public int getDepth()
   {
      return depth;
   }

   public int getDepthDelta()
   {
      return depthDelta;
   }

   public String getLastAddTimestamp()
   {
      return lastAddTimestamp;
   }

   public String getUdpateTimestamp()
   {
      return udpateTimestamp;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
