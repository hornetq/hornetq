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


package org.hornetq.jms;

import javax.jms.JMSException;
import javax.jms.Topic;

import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class HornetQTopic extends HornetQDestination implements Topic
{
   // Constants -----------------------------------------------------

	private static final long serialVersionUID = 7873614001276404156L;

	public static final String JMS_TOPIC_ADDRESS_PREFIX = "jms.topic.";

   private static final char SEPARATOR = '.';
      
   // Static --------------------------------------------------------
      
   public static String createQueueNameForDurableSubscription(final String clientID, final String subscriptionName)
   {
   	return escape(clientID) + SEPARATOR + escape(subscriptionName);
   }
   
   public static Pair<String, String> decomposeQueueNameForDurableSubscription(final String queueName)
   {
      StringBuffer[] parts = new StringBuffer[2];
      int currentPart = 0;
      
      parts[0] = new StringBuffer();
      parts[1] = new StringBuffer();
      
      int pos = 0;
      while (pos < queueName.length())
      {
         char ch = queueName.charAt(pos);
         pos++;

         if (ch == SEPARATOR)
         {
            currentPart++;
            if (currentPart >= parts.length)
            {
               throw new IllegalArgumentException("Invalid message queue name: " + queueName);
            }
            
            continue;
         }

         if (ch == '\\')
         {
            if (pos >= queueName.length())
            {
               throw new IllegalArgumentException("Invalid message queue name: " + queueName);
            }
            ch = queueName.charAt(pos);
            pos++;
         }

         parts[currentPart].append(ch);
      }
      
      if (currentPart != 1)
      {
         throw new IllegalArgumentException("Invalid message queue name: " + queueName);
      }
      
      Pair<String, String> pair = new Pair<String, String>(parts[0].toString(), parts[1].toString());

      return pair;
   }
         
   public static SimpleString createAddressFromName(String name)
   {
      return new SimpleString(JMS_TOPIC_ADDRESS_PREFIX + name);
   }

   // Attributes ----------------------------------------------------     
   
   // Constructors --------------------------------------------------

   public HornetQTopic(final String name)
   {
      super(JMS_TOPIC_ADDRESS_PREFIX + name, name);
   }

   public HornetQTopic(final String address, final String name)
   {
      super(address, name);
   }

   // Topic implementation ------------------------------------------

   public String getTopicName() throws JMSException
   {
      return name;
   }

   // Public --------------------------------------------------------

   public boolean isTemporary()
   {
      return false;
   }   
   
   public String toString()
   {
      return "HornetQTopic[" + name + "]";
   }

     
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
