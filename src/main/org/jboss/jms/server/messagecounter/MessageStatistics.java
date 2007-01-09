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
package org.jboss.jms.server.messagecounter;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.Date;

/**
 * Message statistics
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version <tt>$Revision: 1.3 $</tt>
 */
public class MessageStatistics implements Serializable
{
   // Constants -----------------------------------------------------

   /** The serialVersionUID */
   static final long serialVersionUID = 8056884098781414022L;

   // Attributes ----------------------------------------------------

   /** Whether we are topic */
   private boolean topic;

   /** Whether we are durable */
   private boolean durable;

   /** The name */
   private String name;

   /** The subscription id */
   private String subscriptionID;

   /** The message count */
   private int count;

   /** The message count delta */
   private int countDelta;

   /** The message depth */
   private int depth;

   /** The message depth delta */
   private int depthDelta;

   /** The last update */
   private long timeLastUpdate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Construct a new Message Statistics
    */
   public MessageStatistics()
   {
   }

   // Public --------------------------------------------------------

   /**
    * Get the count.
    * 
    * @return Returns the count.
    */
   public int getCount()
   {
      return count;
   }

   /**
    * Set the count.
    * 
    * @param count The count to set.
    */
   public void setCount(int count)
   {
      this.count = count;
   }

   /**
    * Get the countDelta.
    * 
    * @return Returns the countDelta.
    */
   public int getCountDelta()
   {
      return countDelta;
   }

   /**
    * Set the countDelta.
    * 
    * @param countDelta The countDelta to set.
    */
   public void setCountDelta(int countDelta)
   {
      this.countDelta = countDelta;
   }

   /**
    * Get the depth.
    * 
    * @return Returns the depth.
    */
   public int getDepth()
   {
      return depth;
   }

   /**
    * Set the depth.
    * 
    * @param depth The depth to set.
    */
   public void setDepth(int depth)
   {
      this.depth = depth;
   }

   /**
    * Get the depthDelta.
    * 
    * @return Returns the depthDelta.
    */
   public int getDepthDelta()
   {
      return depthDelta;
   }

   /**
    * Set the depthDelta.
    * 
    * @param depthDelta The depthDelta to set.
    */
   public void setDepthDelta(int depthDelta)
   {
      this.depthDelta = depthDelta;
   }

   /**
    * Get the durable.
    * 
    * @return Returns the durable.
    */
   public boolean isDurable()
   {
      return durable;
   }

   /**
    * Set the durable.
    * 
    * @param durable The durable to set.
    */
   public void setDurable(boolean durable)
   {
      this.durable = durable;
   }

   /**
    * Get the name.
    * 
    * @return Returns the name.
    */
   public String getName()
   {
      return name;
   }

   /**
    * Set the name.
    * 
    * @param name The name to set.
    */
   public void setName(String name)
   {
      this.name = name;
   }

   /**
    * Get the subscriptionID.
    * 
    * @return Returns the subscriptionID.
    */
   public String getSubscriptionID()
   {
      return subscriptionID;
   }

   /**
    * Set the subscriptionID.
    * 
    * @param subscriptionID The subscriptionID to set.
    */
   public void setSubscriptionID(String subscriptionID)
   {
      this.subscriptionID = subscriptionID;
   }

   /**
    * Get the timeLastUpdate.
    * 
    * @return Returns the timeLastUpdate.
    */
   public long getTimeLastUpdate()
   {
      return timeLastUpdate;
   }

   /**
    * Set the timeLastUpdate.
    * 
    * @param timeLastUpdate The timeLastUpdate to set.
    */
   public void setTimeLastUpdate(long timeLastUpdate)
   {
      this.timeLastUpdate = timeLastUpdate;
   }

   /**
    * Get the topic.
    * 
    * @return Returns the topic.
    */
   public boolean isTopic()
   {
      return topic;
   }

   /**
    * Set the topic.
    * 
    * @param topic The topic to set.
    */
   public void setTopic(boolean topic)
   {
      this.topic = topic;
   }

   /**
    * Get message data as string in format
    *
    *  "Topic/Queue, Name, Subscription, Durable, Count, CountDelta,
    *  Depth, DepthDelta, Timestamp Last Increment"  
    *
    * @return  String data as a string
    */
   public String getAsString()
   {
      StringBuffer buffer = new StringBuffer(50);

      // Topic/Queue
      if (topic)
         buffer.append("Topic,");
      else
         buffer.append("Queue,");

      // name 
      buffer.append(name).append(',');

      // subscription
      if (subscriptionID != null)
         buffer.append(subscriptionID).append(',');
      else
         buffer.append("-,");

      // Durable subscription
      if (topic)
      {
         // Topic
         if (durable)
            buffer.append("DURABLE,");
         else
            buffer.append("NONDURABLE,");
      }
      else
      {
         buffer.append("-,");
      }

      // counter values
      buffer.append(count).append(',').append(countDelta).append(',').append(depth).append(',').append(depthDelta)
            .append(',');

      // timestamp last counter update
      if (timeLastUpdate > 0)
      {
         DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);

         buffer.append(dateFormat.format(new Date(timeLastUpdate)));
      }
      else
      {
         buffer.append('-');
      }

      return buffer.toString();
   }

   // Object overrides ----------------------------------------------

   public String toString()
   {
      return getAsString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}