/*
 * Copyright 2013 Red Hat, Inc.
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

package org.hornetq.tests.integration.jms.jms2client;

import javax.jms.*;

import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class BodyTest extends JMSTestBase
{

   private static final String Q_NAME = "SomeQueue";
   private javax.jms.Queue queue;


   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      jmsServer.createQueue(false, Q_NAME, null, true, Q_NAME);
      queue = HornetQJMSClient.createQueue(Q_NAME);
   }

   @Test
   public void testBodyConversion() throws Throwable
   {
      try (
        Connection  conn = cf.createConnection();
      )
      {

         Session sess = conn.createSession();
         MessageProducer producer = sess.createProducer(queue);

         MessageConsumer cons = sess.createConsumer(queue);
         conn.start();

         BytesMessage bytesMessage = sess.createBytesMessage();
         producer.send(bytesMessage);

         Message msg = cons.receiveNoWait();
         assertNotNull(msg);

         try
         {
            msg.getBody(String.class);
            fail("Exception expected");
         }
         catch (MessageFormatException e)
         {
         }
      }

   }
}
