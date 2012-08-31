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
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.jms.client.HornetQTemporaryTopic;
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
   private HornetQConnectionFactory cf;

   private Connection conn;
   private Connection conn2;
   private HornetQSession session2;
   private HornetQSession session1;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      cf = (HornetQConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                       new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      cf.setBlockOnDurableSend(true);
      cf.setPreAcknowledge(true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (session1 != null)
         session1.close();
      if (session2 != null)
         session2.close();
      if (conn != null)
         conn.close();
      if (conn2 != null)
         conn2.close();
      cf = null;

      super.tearDown();
   }

   public void testClosingTemporaryTopicDeletesQueue() throws JMSException, HornetQException
   {
      conn = cf.createConnection();

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());

      session1 = (HornetQSession)conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      HornetQTemporaryTopic topic = (HornetQTemporaryTopic)session1.createTemporaryTopic();
      String address = topic.getAddress();
      session1.close();
      conn.close();
      conn2 = cf.createConnection();
      session2 = (HornetQSession)conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ClientSession cs = session2.getCoreSession();
      try
      {
         cs.createConsumer(address);
         fail("the address from the TemporaryTopic still exists!");
      }
      catch (HornetQException e)
      {
         assertEquals("expecting 'queue does not exist'", HornetQException.QUEUE_DOES_NOT_EXIST, e.getCode());
      }
   }

   /*
    * Closing a connection that is destroyed should cleanly close everything without throwing exceptions
    */
   @SuppressWarnings("unused")
   public void testCloseDestroyedConnection() throws Exception
   {
      long connectionTTL = 500;
      cf.setClientFailureCheckPeriod(connectionTTL / 2);
      // Need to set connection ttl to a low figure so connections get removed quickly on the server
      cf.setConnectionTTL(connectionTTL);

      conn = cf.createConnection();

      Assert.assertEquals(1, server.getRemotingService().getConnections().size());

      session1 = (HornetQSession)conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // Give time for the initial ping to reach the server before we fail (it has connection TTL in it)
      Thread.sleep(500);

      String queueName = "myqueue";

      Queue queue = HornetQJMSClient.createQueue(queueName);

      super.createQueue(queueName);

      MessageConsumer consumer = session1.createConsumer(queue);

      MessageProducer producer = session1.createProducer(queue);

      QueueBrowser browser = session1.createBrowser(queue);

      // Now fail the underlying connection

      ClientSessionInternal sessi = (ClientSessionInternal)((HornetQSession)session1).getCoreSession();

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
