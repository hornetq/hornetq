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

import java.text.DateFormat;
import java.util.Date;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.utils.json.JSONObject;

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

   private static final DateFormat DATE_FORMAT = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);
   
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

   public static String toJSon(MessageCounter counter) throws Exception
   {
      JSONObject json = new JSONObject(counter);
      String lastAddTimestamp = DATE_FORMAT.format(new Date(counter.getLastAddedMessageTime()));
      json.put("lastAddTimestamp", lastAddTimestamp);
      String updateTimestamp = DATE_FORMAT.format(new Date(counter.getLastUpdate()));
      json.put("updateTimestamp", updateTimestamp);
      
      return json.toString();
   }
   
   public static MessageCounterInfo fromJSON(String jsonString) throws Exception
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
