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

package org.hornetq.jms.client;

import javax.jms.Topic;

import org.hornetq.api.core.SimpleString;

/**
 * HornetQ implementation of a JMS Topic.
 * <br>
 * This class can be instantiated directly.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 8737 $</tt>
 *
 * $Id: HornetQTopic.java 8737 2010-01-06 12:41:30Z jmesnil $
 */
public class HornetQTopic extends HornetQDestination implements Topic
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 7873614001276404156L;
   // Static --------------------------------------------------------

   public static SimpleString createAddressFromName(final String name)
   {
      return new SimpleString(HornetQDestination.JMS_TOPIC_ADDRESS_PREFIX + name);
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public HornetQTopic(final String name)
   {
      super(HornetQDestination.JMS_TOPIC_ADDRESS_PREFIX + name, name, false, false, null);
   }


   /**
    * @param address
    * @param name
    * @param temporary
    * @param queue
    * @param session
    */
   protected HornetQTopic(String address, String name, boolean temporary, HornetQSession session)
   {
      super(address, name, temporary, false, session);
   }


   // Topic implementation ------------------------------------------

   public String getTopicName()
   {
      return name;
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      return "HornetQTopic[" + name + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
