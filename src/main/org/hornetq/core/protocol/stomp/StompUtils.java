/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.protocol.stomp;

import org.hornetq.api.core.HornetQException;

/**
 * A StompUtils
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
class StompUtils
{

   public static String HQ_QUEUE_PREFIX = "jms.queue.";

   public static String STOMP_QUEUE_PREFIX = "/queue/";

   public static String HQ_TEMP_QUEUE_PREFIX = "jms.tempqueue.";

   public static String STOMP_TEMP_QUEUE_PREFIX = "/temp-queue/";

   public static String HQ_TOPIC_PREFIX = "jms.topic.";

   public static String STOMP_TOPIC_PREFIX = "/topic/";

   public static String HQ_TEMP_TOPIC_PREFIX = "jms.temptopic.";

   public static String STOMP_TEMP_TOPIC_PREFIX = "/temp-topic/";

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static String toHornetQAddress(String stompDestination) throws HornetQException
   {
      if (stompDestination == null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "No destination is specified!");
      }
      else if (stompDestination.startsWith(STOMP_QUEUE_PREFIX))
      {
         return convert(stompDestination, STOMP_QUEUE_PREFIX, HQ_QUEUE_PREFIX);
      }
      else if (stompDestination.startsWith(STOMP_TOPIC_PREFIX))
      {
         return convert(stompDestination, STOMP_TOPIC_PREFIX, HQ_TOPIC_PREFIX);
      }
      else if (stompDestination.startsWith(STOMP_TEMP_QUEUE_PREFIX))
      {
         return convert(stompDestination, STOMP_TEMP_QUEUE_PREFIX, HQ_TEMP_QUEUE_PREFIX);
      }
      else if (stompDestination.startsWith(STOMP_TEMP_TOPIC_PREFIX))
      {
         return convert(stompDestination, STOMP_TEMP_TOPIC_PREFIX, HQ_TEMP_TOPIC_PREFIX);
      }
      else
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Illegal destination name: [" + stompDestination +
                                                                    "] -- StompConnect destinations " +
                                                                    "must begin with one of: /queue/ /topic/ /temp-queue/ /temp-topic/");
      }
   }

   public static String toStompDestination(String hornetqAddress) throws HornetQException
   {
      if (hornetqAddress == null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "No destination is specified!");
      }
      else if (hornetqAddress.startsWith(HQ_QUEUE_PREFIX))
      {
         return convert(hornetqAddress, HQ_QUEUE_PREFIX, STOMP_QUEUE_PREFIX);
      }
      else if (hornetqAddress.startsWith(HQ_TOPIC_PREFIX))
      {
         return convert(hornetqAddress, HQ_TOPIC_PREFIX, STOMP_TOPIC_PREFIX);
      }
      else if (hornetqAddress.startsWith(HQ_TEMP_QUEUE_PREFIX))
      {
         return convert(hornetqAddress, HQ_TEMP_QUEUE_PREFIX, STOMP_TEMP_QUEUE_PREFIX);
      }
      else if (hornetqAddress.startsWith(HQ_TEMP_TOPIC_PREFIX))
      {
         return convert(hornetqAddress, HQ_TEMP_TOPIC_PREFIX, STOMP_TEMP_TOPIC_PREFIX);
      }
      else
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Illegal address name: [" + hornetqAddress +
                                                                    "] -- Acceptable address must comply to JMS semantics");
      }
   }
   
   private static String convert(String str, String oldPrefix, String newPrefix) 
   {
      String sub = str.substring(oldPrefix.length(), str.length());
      return new String(newPrefix + sub);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
