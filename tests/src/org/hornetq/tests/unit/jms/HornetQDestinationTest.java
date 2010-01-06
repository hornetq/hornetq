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

import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import junit.framework.Assert;

import org.hornetq.jms.HornetQDestination;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.*;
import org.hornetq.jms.HornetQTopic;
import org.hornetq.tests.util.RandomUtil;
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
      String destinationName = RandomUtil.randomString();
      String address = HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = (HornetQDestination) HornetQDestination.fromAddress(address);
      HornetQDestination sameDestination = (HornetQDestination) HornetQDestination.fromAddress(address);
      HornetQDestination differentDestination = (HornetQDestination) HornetQDestination.fromAddress(address + RandomUtil.randomString());

      Assert.assertFalse(destination.equals(null));
      Assert.assertTrue(destination.equals(destination));
      Assert.assertTrue(destination.equals(sameDestination));
      Assert.assertFalse(destination.equals(differentDestination));
   }

   public void testFromAddressWithQueueAddressPrefix() throws Exception
   {
      String destinationName = RandomUtil.randomString();
      String address = HornetQQueue.JMS_QUEUE_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = (HornetQDestination) HornetQDestination.fromAddress(address);
      Assert.assertTrue(destination instanceof Queue);
      Assert.assertEquals(destinationName, ((Queue)destination).getQueueName());
   }

   public void testFromAddressWithTopicAddressPrefix() throws Exception
   {
      String destinationName = RandomUtil.randomString();
      String address = HornetQTopic.JMS_TOPIC_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = (HornetQDestination) HornetQDestination.fromAddress(address);
      Assert.assertTrue(destination instanceof Topic);
      Assert.assertEquals(destinationName, ((Topic)destination).getTopicName());
   }

   public void testFromAddressWithTemporaryQueueAddressPrefix() throws Exception
   {
      String destinationName = RandomUtil.randomString();
      String address = HornetQTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = (HornetQDestination) HornetQDestination.fromAddress(address);
      Assert.assertTrue(destination instanceof TemporaryQueue);
      Assert.assertEquals(destinationName, ((TemporaryQueue)destination).getQueueName());
   }

   public void testFromAddressWithTemporaryTopicAddressPrefix() throws Exception
   {
      String destinationName = RandomUtil.randomString();
      String address = HornetQTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX + destinationName;
      HornetQDestination destination = (HornetQDestination) HornetQDestination.fromAddress(address);
      Assert.assertTrue(destination instanceof TemporaryTopic);
      Assert.assertEquals(destinationName, ((TemporaryTopic)destination).getTopicName());
   }

   public void testFromAddressWithInvalidPrefix() throws Exception
   {
      String invalidPrefix = "junk";
      String destinationName = RandomUtil.randomString();
      String address = invalidPrefix + destinationName;
      try
      {
         HornetQDestination.fromAddress(address);
         Assert.fail("IllegalArgumentException");
      }
      catch (IllegalArgumentException e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
