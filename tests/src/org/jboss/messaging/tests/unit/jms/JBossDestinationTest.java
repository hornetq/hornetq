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

import javax.jms.Queue;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

import junit.framework.TestCase;

import org.jboss.messaging.jms.JBossDestination;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTemporaryQueue;
import org.jboss.messaging.jms.JBossTemporaryTopic;
import org.jboss.messaging.jms.JBossTopic;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class JBossDestinationTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFromAddressWithQueueAddressPrefix() throws Exception
   {
      String destinationName = randomString();
      String address = JBossQueue.JMS_QUEUE_ADDRESS_PREFIX + destinationName;
      JBossDestination destination = JBossDestination.fromAddress(address);
      assertTrue(destination instanceof Queue);
      assertEquals(destinationName, ((Queue)destination).getQueueName());
   }

   public void testFromAddressWithTopicAddressPrefix() throws Exception
   {
      String destinationName = randomString();
      String address = JBossTopic.JMS_TOPIC_ADDRESS_PREFIX + destinationName;
      JBossDestination destination = JBossDestination.fromAddress(address);
      assertTrue(destination instanceof Topic);
      assertEquals(destinationName, ((Topic)destination).getTopicName());
   }
   
   public void testFromAddressWithTemporaryQueueAddressPrefix() throws Exception
   {
      String destinationName = randomString();
      String address = JBossTemporaryQueue.JMS_TEMP_QUEUE_ADDRESS_PREFIX + destinationName;
      JBossDestination destination = JBossDestination.fromAddress(address);
      assertTrue(destination instanceof TemporaryQueue);
      assertEquals(destinationName, ((TemporaryQueue)destination).getQueueName());
   }
   
   public void testFromAddressWithTemporaryTopicAddressPrefix() throws Exception
   {
      String destinationName = randomString();
      String address = JBossTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX + destinationName;
      JBossDestination destination = JBossDestination.fromAddress(address);
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
         JBossDestination.fromAddress(address);
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
