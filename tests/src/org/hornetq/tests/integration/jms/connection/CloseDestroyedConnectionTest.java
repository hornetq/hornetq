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
package org.hornetq.tests.integration.jms.connection;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.JMSTestBase;

/**
 * 
 * A CloseDestroyedConnectionTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class CloseDestroyedConnectionTest extends JMSTestBase
{
   private static final Logger log = Logger.getLogger(CloseDestroyedConnectionTest.class);

   private HornetQConnectionFactory cf;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactory(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      cf.setBlockOnDurableSend(true);
      cf.setPreAcknowledge(true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      cf = null;

      super.tearDown();
   }

   /*
    * Closing a connection that is destroyed should cleanly close everything without throwing exceptions
    */
   public void testCloseDestroyedConnection() throws Exception
   {
      long connectionTTL = 500;
      cf.setClientFailureCheckPeriod(connectionTTL / 2);
      // Need to set connection ttl to a low figure so connections get removed quickly on the server
      cf.setConnectionTTL(connectionTTL);

      Connection conn = cf.createConnection();

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // Give time for the initial ping to reach the server before we fail (it has connection TTL in it)
      Thread.sleep(500);

      String queueName = "myqueue";

      Queue queue = HornetQJMSClient.createQueue(queueName);

      super.createQueue(queueName);

      MessageConsumer consumer = sess.createConsumer(queue);

      MessageProducer producer = sess.createProducer(queue);

      QueueBrowser browser = sess.createBrowser(queue);

      // Now fail the underlying connection

      ClientSessionInternal sessi = (ClientSessionInternal)((HornetQSession)sess).getCoreSession();

      RemotingConnection rc = sessi.getConnection();

      rc.fail(new HornetQException(HornetQException.INTERNAL_ERROR));

      // Now close the connection

      conn.close();

      long start = System.currentTimeMillis();

      while (true)
      {
         int cons = server.getRemotingService().getConnections().size();

         if (cons == 0)
         {
            break;
         }

         long now = System.currentTimeMillis();

         if (now - start > 10000)
         {
            throw new Exception("Timed out waiting for connections to close");
         }

         Thread.sleep(50);
      }
   }
}
