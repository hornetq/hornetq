/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms;

import javax.jms.JMSException;
import javax.jms.Topic;

import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossTopic extends JBossDestination implements Topic
{
   // Constants -----------------------------------------------------

	private static final long serialVersionUID = 7873614001276404156L;

	public static final String JMS_TOPIC_ADDRESS_PREFIX = "topicjms.";

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

   public JBossTopic(final String name)
   {
      super(JMS_TOPIC_ADDRESS_PREFIX + name, name);
   }

   protected JBossTopic(final String address, final String name)
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
      return "JBossTopic[" + name + "]";
   }

     
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
