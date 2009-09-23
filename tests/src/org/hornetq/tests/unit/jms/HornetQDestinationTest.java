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


package org.hornetq.tests.unit.jms;

import static org.hornetq.tests.util.RandomUtil.randomString;

import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import org.hornetq.jms.HornetQDestination;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.HornetQTemporaryQueue;
import org.hornetq.jms.HornetQTemporaryTopic;
import org.hornetq.jms.HornetQTopic;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class HornetQDestinationTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testEquals() throws Exception
   {
      String destinationName = randomString();
      String address = HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = HornetQDestination.fromAddress(address);
      HornetQDestination sameDestination = HornetQDestination.fromAddress(address);
      HornetQDestination differentDestination = HornetQDestination.fromAddress(address + randomString());
      
      assertFalse(destination.equals(null));
      assertTrue(destination.equals(destination));
      assertTrue(destination.equals(sameDestination));
      assertFalse(destination.equals(differentDestination));
   }
   
   public void testFromAddressWithQueueAddressPrefix() throws Exception
   {
      String destinationName = randomString();
      String address = HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = HornetQDestination.fromAddress(address);
      assertTrue(destination instanceof Queue);
      assertEquals(destinationName, ((Queue)destination).getQueueName());
   }

   public void testFromAddressWithTopicAddressPrefix() throws Exception
   {
      String destinationName = randomString();
      String address = HornetQTopic.JMS_TOPIC_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = HornetQDestination.fromAddress(address);
      assertTrue(destination instanceof Topic);
      assertEquals(destinationName, ((Topic)destination).getTopicName());
   }
   
   public void testFromAddressWithTemporaryQueueAddressPrefix() throws Exception
   {
      String destinationName = randomString();
      String address = HornetQTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = HornetQDestination.fromAddress(address);
      assertTrue(destination instanceof TemporaryQueue);
      assertEquals(destinationName, ((TemporaryQueue)destination).getQueueName());
   }
   
   public void testFromAddressWithTemporaryTopicAddressPrefix() throws Exception
   {
      String destinationName = randomString();
      String address = HornetQTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = HornetQDestination.fromAddress(address);
      assertTrue(destination instanceof TemporaryTopic);
      assertEquals(destinationName, ((TemporaryTopic)destination).getTopicName());
   }
   
   public void testFromAddressWithInvalidPrefix() throws Exception
   {
      String invalidPrefix = "junk";
      String destinationName = randomString();
      String address = invalidPrefix + destinationName;
      try
      {
         HornetQDestination.fromAddress(address);
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
