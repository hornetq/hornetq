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

package org.hornetq.tests.integration.jms.divert;

import java.util.ArrayList;

import javax.jms.*;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.tests.util.JMSTestBase;

/**
 * A DivertAndACKClientTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class DivertAndACKClientTest extends JMSTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAutoACK() throws Exception
   {
      HornetQQueue queueSource = (HornetQQueue)createQueue("Source");
      HornetQQueue queueTarget = (HornetQQueue)createQueue("Dest");

      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      final MessageProducer producer = session.createProducer(queueSource);

      final TextMessage message = session.createTextMessage("message text");
      producer.send(message);

      connection.start();

      final MessageConsumer consumer = session.createConsumer(queueTarget);
      TextMessage receivedMessage = (TextMessage)consumer.receive(1000);

      assertNotNull(receivedMessage);

      connection.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected boolean usePersistence()
   {
      return true;
   }
   

   protected Configuration createDefaultConfig(final boolean netty)
   {
      Configuration config = super.createDefaultConfig(netty);

      DivertConfiguration divert = new DivertConfiguration("local-divert",
                                                           "some-name",
                                                           "jms.queue.Source",
                                                           "jms.queue.Dest",
                                                           true,
                                                           null,
                                                           null);

      ArrayList<DivertConfiguration> divertList = new ArrayList<DivertConfiguration>();
      divertList.add(divert);

      config.setDivertConfigurations(divertList);

      return config;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
