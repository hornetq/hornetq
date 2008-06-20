/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.jms;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import junit.framework.TestCase;

import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.util.Pair;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class JBossTopicTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   public void testIsTemporary() throws Exception
   {
      JBossTopic topic = new JBossTopic(randomString());
      assertFalse(topic.isTemporary());
   }
   
   public void testGetTopicName() throws Exception
   {
      String topicName = randomString();
      JBossTopic queue = new JBossTopic(topicName);
      assertEquals(topicName, queue.getTopicName());
   }
   
   public void testDecomposeQueueNameForDurableSubscription() throws Exception
   {
      String clientID = randomString();
      String subscriptionName = randomString();
      
      Pair<String, String> pair = JBossTopic.decomposeQueueNameForDurableSubscription(clientID + '.' + subscriptionName);
      assertEquals(clientID, pair.a);
      assertEquals(subscriptionName, pair.b);
   }
   
   public void testdDecomposeQueueNameForDurableSubscriptionWithInvalidQueueName() throws Exception
   {
      try
      {
         JBossTopic.decomposeQueueNameForDurableSubscription("queueNameHasNoDot");
         fail("IllegalArgumentException");
      } catch (IllegalArgumentException e)
      {
      }
   }
   
   public void testdDcomposeQueueNameForDurableSubscriptionWithInvalidQueueName_2() throws Exception
   {
      try
      {
         JBossTopic.decomposeQueueNameForDurableSubscription("queueName.HasTooMany.Dots");
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
