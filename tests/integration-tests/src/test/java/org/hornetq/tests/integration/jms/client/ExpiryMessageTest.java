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

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.hornetq.api.jms.management.TopicControl;
import org.hornetq.core.config.Configuration;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.util.JMSTestBase;

/**
 *
 * A MessageTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ExpiryMessageTest extends JMSTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected Configuration createDefaultConfig(boolean netty) throws Exception
   {
      Configuration conf = super.createDefaultConfig(netty);

      conf.setMessageExpiryScanPeriod(1000);

      return conf;
   }

   @Test
   public void testSendTopicNoSubscription() throws Exception
   {

      Topic topic = createTopic("test-topic");
      TopicControl control = ManagementControlHelper.createTopicControl(topic, mbeanServer);

      System.out.println("size = " + control.getMessageCount());

      Connection conn2 = cf.createConnection();

      conn2.setClientID("client1");

      Session sess2 = conn2.createSession(true, Session.SESSION_TRANSACTED);

      sess2.createDurableSubscriber(topic, "client-sub1");
      sess2.createDurableSubscriber(topic, "client-sub2");

      conn2.close();

      conn = cf.createConnection();
      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer prod = sess.createProducer(topic);
      prod.setTimeToLive(1000);

      for (int i = 0 ; i < 100; i++)
      {
         TextMessage txt = sess.createTextMessage("txt");
         prod.send(txt);
      }

      sess.commit();

      conn.close();

      // minimal time needed
      Thread.sleep(2000);

      long timeout = System.currentTimeMillis() + 10000;

      // We will wait some time, but we will wait as minimal as possible
      while (control.getMessageCount() != 0 && System.currentTimeMillis() > timeout)
      {
         Thread.sleep(100);
      }

      assertEquals(0, control.getMessageCount());


   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   // Inner classes -------------------------------------------------
}
