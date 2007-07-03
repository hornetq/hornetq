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
package org.jboss.messaging.util;


/**
 * A MessageQueueNameHelper
 *
 * By convention, we name durable topic message queue names in the following way:
 * 
 * <clientid>.<sub_name>
 * 
 * This is a helper class to aid in converting between the string form and the client id
 * and subscription name and vice versa
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class MessageQueueNameHelper
{
   private String clientId;
   
   private String subName;
   
   private static char ESCAPE = '\\';
   private static char SEPARATOR_CHAR = '.';
   private static String SEPARATOR = ".";
   
   private MessageQueueNameHelper(String messageQueueName)
   {
      if (messageQueueName == null)
      {
         throw new IllegalArgumentException("Message queue name is null");
      }
      
      StringBuffer[] parts = new StringBuffer[2];
      int currentPart = 0;
      
      parts[0] = new StringBuffer();
      parts[1] = new StringBuffer();
      
      int pos = 0;
      while (pos < messageQueueName.length())
      {
         char ch = messageQueueName.charAt(pos);
         pos++;

         if (ch == SEPARATOR_CHAR)
         {
            currentPart++;
            if (currentPart >= parts.length)
            {
               throw new IllegalArgumentException("Invalid message queue name: " + messageQueueName);
            }
            
            continue;
         }

         if (ch == ESCAPE)
         {
            if (pos >= messageQueueName.length())
            {
               throw new IllegalArgumentException("Invalid message queue name: " + messageQueueName);
            }
            ch = messageQueueName.charAt(pos);
            pos++;
         }

         parts[currentPart].append(ch);
      }
      
      if (currentPart != 1)
      {
         throw new IllegalArgumentException("Invalid message queue name: " + messageQueueName);
      }
      
      clientId = parts[0].toString();
      subName = parts[1].toString();
   }
   
   private static String escape(String input)
   {
      return input.replace("\\", "\\\\").replace(".", "\\.");
   }

   public String getClientId()
   {
      return clientId;
   }
   
   public String getSubName()
   {
      return subName;
   }
   
   public static MessageQueueNameHelper createHelper(String messageQueueName)
   {
      return new MessageQueueNameHelper(messageQueueName);
   }
   
   public static String createSubscriptionName(String clientID, String subName)
   {
      if (clientID == null)
      {
         throw new IllegalArgumentException("clientID name is null");
      }
      if (subName == null)
      {
         throw new IllegalArgumentException("Subscription name is null");
      }
      
      return escape(clientID) + SEPARATOR + escape(subName);
   }      
}
