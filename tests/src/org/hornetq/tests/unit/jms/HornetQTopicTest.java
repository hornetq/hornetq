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

import org.hornetq.jms.HornetQTopic;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.Pair;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class HornetQTopicTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   public void testIsTemporary() throws Exception
   {
      HornetQTopic topic = new HornetQTopic(randomString());
      assertFalse(topic.isTemporary());
   }
   
   public void testGetTopicName() throws Exception
   {
      String topicName = randomString();
      HornetQTopic queue = new HornetQTopic(topicName);
      assertEquals(topicName, queue.getTopicName());
   }
   
   public void testDecomposeQueueNameForDurableSubscription() throws Exception
   {
      String clientID = randomString();
      String subscriptionName = randomString();
      
      Pair<String, String> pair = HornetQTopic.decomposeQueueNameForDurableSubscription(clientID + '.' + subscriptionName);
      assertEquals(clientID, pair.a);
      assertEquals(subscriptionName, pair.b);
   }
   
   public void testdDecomposeQueueNameForDurableSubscriptionWithInvalidQueueName() throws Exception
   {
      try
      {
         HornetQTopic.decomposeQueueNameForDurableSubscription("queueNameHasNoDot");
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }
   
   public void testdDcomposeQueueNameForDurableSubscriptionWithInvalidQueueName_2() throws Exception
   {
      try
      {
         HornetQTopic.decomposeQueueNameForDurableSubscription("queueName.HasTooMany.Dots");
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
