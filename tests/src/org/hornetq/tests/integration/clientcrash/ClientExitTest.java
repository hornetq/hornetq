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

package org.hornetq.tests.integration.clientcrash;

import static org.hornetq.tests.util.RandomUtil.randomString;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.Message;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.tests.util.SpawnedVMSupport;
import org.hornetq.utils.SimpleString;

/**
 * A test that makes sure that a HornetQ client gracefully exists after the last session is
 * closed. Test for http://jira.jboss.org/jira/browse/JBMESSAGING-417.
 *
 * This is not technically a crash test, but it uses the same type of topology as the crash tests
 * (local server, remote VM client).
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * $Id$
 */
public class ClientExitTest extends ClientTestBase
{
   // Constants ------------------------------------------------------------------------------------

   private static final String MESSAGE_TEXT = randomString();
   
   private static final SimpleString QUEUE = new SimpleString("ClientExitTestQueue");
      
   // Static ---------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientExitTest.class);

   // Attributes -----------------------------------------------------------------------------------

   private ClientSession session;

   private ClientConsumer consumer;   

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   public void testGracefulClientExit() throws Exception
   {
      // spawn a JVM that creates a JMS client, which sends a test message
      Process p = SpawnedVMSupport.spawnVM(GracefulClient.class.getName(), QUEUE.toString(), MESSAGE_TEXT);

      // read the message from the queue

      Message message = consumer.receive(15000);

      assertNotNull(message);
      assertEquals(MESSAGE_TEXT, message.getBodyBuffer().readString());

      // the client VM should exit by itself. If it doesn't, that means we have a problem
      // and the test will timeout
      log.debug("waiting for the client VM to exit ...");
      p.waitFor();

      assertEquals(0, p.exitValue());
      
      // FIXME https://jira.jboss.org/jira/browse/JBMESSAGING-1421
//      Thread.sleep(1000);
//      
//      // the local session
//      assertActiveConnections(1);
//      // assertActiveSession(1);
      
      session.close();
      
      // FIXME https://jira.jboss.org/jira/browse/JBMESSAGING-1421
//      Thread.sleep(1000);
//      assertActiveConnections(0);
//      // assertActiveSession(0);
   }
   
   // Package protected ----------------------------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.createQueue(QUEUE, QUEUE, null, false);
      consumer = session.createConsumer(QUEUE);
      session.start();
   }

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
