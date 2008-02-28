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
package org.jboss.messaging.core.messagecounter;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.Queue;

/**
 * This class stores message count informations for a given queue
 * 
 * At intervals this class samples the queue for message count data
 * 
 * Note that the underlying queue *does not* update statistics every time a message
 * is added since that would reall slow things down, instead we *sample* the queues at
 * regular intervals - this means we are less intrusive on the queue
 *
 * @author <a href="mailto:u.schroeter@mobilcom.de">Ulf Schroeter</a>
 * @author <a href="mailto:s.steinbacher@mobilcom.de">Stephan Steinbacher</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision: 1.8 $
 */
public class MessageCounter
{
   protected static final Logger log = Logger.getLogger(MessageCounter.class);

   // destination related information
   private String destName;
   private boolean destDurable;

   // destination queue
   private Queue destQueue;

   // counter
   private long timeLastUpdate;

   // per hour day counter history
   private int messageCount;
   private int dayCounterMax;
   private ArrayList<DayCounter> dayCounter;

   /**
    *    Constructor
    *
    * @param name             destination name
    * @param queue            internal queue object
    * @param durable          durable subsciption flag
    * @param daycountmax      max message history day count
    */
   public MessageCounter(String name,
                         Queue queue,
                         boolean durable,
                         int daycountmax)
   {
      // store destination related information
      destName = name;
      destDurable = durable;
      destQueue = queue;      
      messageCount = destQueue.getMessageCount();
      // initialize counter
      resetCounter();

      // initialize message history
      dayCounter = new ArrayList<DayCounter>();

      setHistoryLimit(daycountmax);
   }

   /**
   * Get string representation
   */
   public String toString()
   {
      return getCounterAsString();
   }

   private int lastMessagesAdded;
   
   /*
    * This method is called periodically to update statistics from the queue
    */
   public synchronized void sample()
   {
      
      //update timestamp
      timeLastUpdate = System.currentTimeMillis();
      
      // update message history
      updateHistory(true);
   }

   /**
    * Gets the related destination name
    *
    * @return String    destination name
    */
   public String getDestinationName()
   {
      return destName;
   }

   /**
    * Gets the related destination durable subscription flag
    *
    * @return boolean   true : durable subscription,
    *                   false: non-durable subscription
    */
   public boolean getDestinationDurable()
   {
      return destDurable;
   }

   public int getTotalMessages()
   {
      return this.destQueue.getMessagesAdded();
   }

   public int getCurrentMessageCount()
   {
      return this.destQueue.getMessageCount();
   }

   /**
    * Gets the current message count of pending messages
    * within the destination waiting for dispatch
    *
    * @return int message queue depth
    */
   public int getMessageCount()
   {
      return destQueue.getMessagesAdded() - messageCount;
   }


   /**
    * Gets the timestamp of the last message add
    *
    * @return long      system time
    */
   public long getLastUpdate()
   {
      return timeLastUpdate;
   }

   /**
    * Reset message counter values
    */
   public void resetCounter()
   {
      messageCount = destQueue.getMessageCount();
      timeLastUpdate = 0;
   }

   /**
    * Get message counter data as string in format
    *
    *  "Topic/Queue, Name, Subscription, Durable, Count, CountDelta,
    *  Depth, DepthDelta, Timestamp Last Increment"  
    *
    * @return  String   message counter data string
    */
   public String getCounterAsString()
   {
      StringBuffer ret = new StringBuffer();

      // name 
      ret.append(destName).append(",");

      // counter values
      ret.append("total messages received = ").append(getTotalMessages()).append(",").
              append("current messages in queue = ").append(getCurrentMessageCount()).append(",").
              append("current message count = ").append(getMessageCount()).append(",");

      // timestamp last counter update
      if (timeLastUpdate > 0)
      {
         DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);

         ret.append("last reset at ").append(dateFormat.format(new Date(timeLastUpdate)));
      }
      else
      {
         ret.append("-");
      }

      return ret.toString();
   }

   /**
    * Get message counter history day count limit
    *
    * <0: unlimited, 0: history disabled, >0: day count
    */
   public int getHistoryLimit()
   {
      return dayCounterMax;
   }

   /**
    * Set message counter history day count limit
    *
    * <0: unlimited, 0: history disabled, >0: day count
    */
   public void setHistoryLimit(int daycountmax)
   {
      boolean bInitialize = false;

      // store new maximum day count
      dayCounterMax = daycountmax;

      // update day counter array
      synchronized (dayCounter)
      {
         if (dayCounterMax > 0)
         {
            // limit day history to specified day count
            int delta = dayCounter.size() - dayCounterMax;

            for (int i = 0; i < delta; i++)
            {
               // reduce array size to requested size by dropping
               // oldest day counters
               dayCounter.remove(0);
            }

            // create initial day counter when empty
            bInitialize = dayCounter.isEmpty();
         }
         else if (dayCounterMax == 0)
         {
            // disable history
            dayCounter.clear();
         }
         else
         {
            // unlimited day history

            // create initial day counter when empty
            bInitialize = dayCounter.isEmpty();
         }

         // optionally initialize first day counter entry
         if (bInitialize)
         {
            dayCounter.add(new DayCounter(new GregorianCalendar(), true));
         }
      }
   }

   /**
    * Update message counter history
    */
   private void updateHistory(boolean incrementCounter)
   {
      // check history activation
      if (dayCounter.isEmpty())
      {
         return;
      }

      // calculate day difference between current date and date of last day counter entry
      synchronized (dayCounter)
      {
         DayCounter counterLast = dayCounter.get(dayCounter.size() - 1);

         GregorianCalendar calNow = new GregorianCalendar();
         GregorianCalendar calLast = counterLast.getDate();

         // clip day time part for day delta calulation
         calNow.clear(Calendar.AM_PM);
         calNow.clear(Calendar.HOUR);
         calNow.clear(Calendar.HOUR_OF_DAY);
         calNow.clear(Calendar.MINUTE);
         calNow.clear(Calendar.SECOND);
         calNow.clear(Calendar.MILLISECOND);

         calLast.clear(Calendar.AM_PM);
         calLast.clear(Calendar.HOUR);
         calLast.clear(Calendar.HOUR_OF_DAY);
         calLast.clear(Calendar.MINUTE);
         calLast.clear(Calendar.SECOND);
         calLast.clear(Calendar.MILLISECOND);

         long millisPerDay = 86400000; // 24 * 60 * 60 * 1000
         long millisDelta = calNow.getTime().getTime() - calLast.getTime().getTime();

         int dayDelta = (int) (millisDelta / millisPerDay);

         if (dayDelta > 0)
         {
            // finalize last day counter
            counterLast.finalizeDayCounter();

            // add new intermediate empty day counter entries
            DayCounter counterNew;

            for (int i = 1; i < dayDelta; i++)
            {
               // increment date
               calLast.add(Calendar.DAY_OF_YEAR, 1);

               counterNew = new DayCounter(calLast, false);
               counterNew.finalizeDayCounter();

               dayCounter.add(counterNew);
            }

            // add new day counter entry for current day
            counterNew = new DayCounter(calNow, false);

            dayCounter.add(counterNew);

            // ensure history day count limit
            setHistoryLimit(dayCounterMax);
         }

         // update last day counter entry
         counterLast = dayCounter.get(dayCounter.size() - 1);
         counterLast.updateDayCounter(incrementCounter);
      }
   }

   /**
    * Reset message counter history
    */
   public void resetHistory()
   {
      int max = dayCounterMax;

      setHistoryLimit(0);
      setHistoryLimit(max);
   }

   /**
    * Get message counter history data as string in format
    * 
    * "day count\n  
    *  Date 1, hour counter 0, hour counter 1, ..., hour counter 23\n
    *  Date 2, hour counter 0, hour counter 1, ..., hour counter 23\n
    *  .....
    *  .....
    *  Date n, hour counter 0, hour counter 1, ..., hour counter 23\n"
    *
    * @return  String   message history data string
    */
   public String getHistoryAsString()
   {
      String ret = "";

      // ensure history counters are up to date
      updateHistory(false);

      // compile string       
      synchronized (dayCounter)
      {
         // first line: history day count  
         ret += dayCounter.size() + "\n";

         // following lines: day counter data
         for (DayCounter counter : dayCounter)
         {
            ret += counter.getDayCounterAsString() + "\n";
         }
      }

      return ret;
   }

   /**
    * Internal day counter class for one day hour based counter history
    */
   private static class DayCounter
   {
      static final int HOURS = 24;

      GregorianCalendar date = null;
      int[] counters = new int[HOURS];

      /**
       *    Constructor
       *
       * @param date          day counter date
       * @param isStartDay    true  first day counter
       *                      false follow up day counter
       */
      DayCounter(GregorianCalendar date, boolean isStartDay)
      {
         // store internal copy of creation date
         this.date = (GregorianCalendar) date.clone();

         // initialize the array with '0'- values to current hour (if it is not the
         // first monitored day) and the rest with default values ('-1')
         int hour = date.get(Calendar.HOUR_OF_DAY);

         for (int i = 0; i < HOURS; i++)
         {
            if (i < hour)
            {
               if (isStartDay)
                  counters[i] = -1;
               else
                  counters[i] = 0;
            }
            else
            {
               counters[i] = -1;
            }
         }

         // set the array element of the current hour to '0'
         counters[hour] = 0;
      }

      /**
       * Gets copy of day counter date
       *
       * @return GregorianCalendar        day counter date
       */
      GregorianCalendar getDate()
      {
         return (GregorianCalendar) date.clone();
      }

      /**
       * Update day counter hour array elements  
       *
       * @param incrementCounter      update current hour counter
       */
      void updateDayCounter(boolean incrementCounter)
      {
         // get the current hour of the day
         GregorianCalendar cal = new GregorianCalendar();

         int currentIndex = cal.get(Calendar.HOUR_OF_DAY);

         // check if the last array update is more than 1 hour ago, if so fill all
         // array elements between the last index and the current index with '0' values
         boolean bUpdate = false;

         for (int i = 0; i <= currentIndex; i++)
         {
            if (counters[i] > -1)
            {
               // found first initialized hour counter
               // -> set all following uninitialized
               //    counter values to 0
               bUpdate = true;
            }

            if (bUpdate)
            {
               if (counters[i] == -1)
                  counters[i] = 0;
            }
         }

         // optionally increment current counter
         if (incrementCounter)
         {
            counters[currentIndex]++;
         }
      }

      /**
       * Finalize day counter hour array elements  
       */
      void finalizeDayCounter()
      {
         // a new day has began, so fill all array elements from index to end with
         // '0' values
         boolean bFinalize = false;

         for (int i = 0; i < HOURS; i++)
         {
            if (counters[i] > -1)
            {
               // found first initialized hour counter
               // -> finalize all following uninitialized
               //    counter values
               bFinalize = true;
            }

            if (bFinalize)
            {
               if (counters[i] == -1)
                  counters[i] = 0;
            }
         }
      }

      /**
       * Return day counter data as string with format
       * "Date, hour counter 0, hour counter 1, ..., hour counter 23"
       * 
       * @return  String   day counter data
       */
      String getDayCounterAsString()
      {
         // first element day counter date
         DateFormat dateFormat = DateFormat.getDateInstance(DateFormat.SHORT);

         String strData = dateFormat.format(date.getTime());

         // append 24 comma separated hour counter values           
         for (int i = 0; i < HOURS; i++)
         {
            strData += "," + counters[i];
         }

         return strData;
      }
   }
}

