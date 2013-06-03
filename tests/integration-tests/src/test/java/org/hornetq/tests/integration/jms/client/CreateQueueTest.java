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

package org.hornetq.tests.integration.jms.client;

import org.junit.Test;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.JMSTestBase;

/**
 *
 * A CreateQueueTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class CreateQueueTest extends JMSTestBase
{
   // Constants -----------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCreateQueueTempQueue() throws Exception
   {
      conn = cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue tempQueue = session.createTemporaryQueue();

         String tempQueueName = tempQueue.getQueueName();

         assertFalse(tempQueueName.startsWith(HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX));

         Queue replyQueue = session.createQueue(tempQueueName);

         MessageProducer producer = session.createProducer(replyQueue);

         producer.send(session.createMessage());

         MessageConsumer consumer= session.createConsumer(replyQueue);

         conn.start();

         assertNotNull(consumer.receive(10000));
   }

   @Test
   public void testCreateQueue() throws Exception
   {
      conn = cf.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = createQueue("TestQueue");

         String queueName = queue.getQueueName();

         log.info("queue name is " + queueName);

         assertFalse(queueName.startsWith(HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX));

         Queue replyQueue = session.createQueue(queueName);

         MessageProducer producer = session.createProducer(replyQueue);

         producer.send(session.createMessage());

         MessageConsumer consumer= session.createConsumer(replyQueue);

         conn.start();

         assertNotNull(consumer.receive(10000));
         }

   @Test
   public void testCreateTopic() throws Exception
   {
      conn = cf.createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Topic topic = createTopic("TestTopic");

         String topicName = topic.getTopicName();

         assertFalse(topicName.startsWith(HornetQDestination.JMS_TOPIC_ADDRESS_PREFIX));

         Topic replyTopic = session.createTopic(topicName);

         MessageConsumer consumer= session.createConsumer(replyTopic);

         conn.start();

         MessageProducer producer = session.createProducer(replyTopic);

         producer.send(session.createMessage());

         assertNotNull(consumer.receive(10000));
         }

   @Test
   public void testCreateTopicTempTopic() throws Exception
   {
      conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Topic tempTopic = session.createTemporaryTopic();

         String tempTopicName = tempTopic.getTopicName();

         assertFalse(tempTopicName.startsWith(HornetQDestination.JMS_TOPIC_ADDRESS_PREFIX));

         Topic replyTopic = session.createTopic(tempTopicName);

         MessageConsumer consumer= session.createConsumer(replyTopic);

         conn.start();

         MessageProducer producer = session.createProducer(replyTopic);

         producer.send(session.createMessage());

         assertNotNull(consumer.receive(10000));
         }
}
