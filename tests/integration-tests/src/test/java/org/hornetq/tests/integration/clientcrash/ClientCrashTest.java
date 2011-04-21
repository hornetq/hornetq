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

import junit.framework.Assert;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.SpawnedVMSupport;

/**
 * A test that makes sure that a HornetQ server cleans up the associated
 * resources when one of its client crashes.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision: 4032 $</tt>
 */
public class ClientCrashTest extends ClientTestBase
{
   static final int PING_PERIOD = 2000;

   static final int CONNECTION_TTL = 3000;

   // Constants -----------------------------------------------------

   public static final SimpleString QUEUE = new SimpleString("ClientCrashTestQueue");

   public static final String MESSAGE_TEXT_FROM_SERVER = "ClientCrashTest from server";

   public static final String MESSAGE_TEXT_FROM_CLIENT = "ClientCrashTest from client";

   // Static --------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientCrashTest.class);

   // Attributes ----------------------------------------------------

   private ClientSessionFactory sf;

   private ServerLocator locator;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCrashClient() throws Exception
   {
      assertActiveConnections(1);

      // spawn a JVM that creates a Core client, which sends a message
      Process p = SpawnedVMSupport.spawnVM(CrashClient.class.getName());

      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(ClientCrashTest.QUEUE, ClientCrashTest.QUEUE, null, false);
      ClientConsumer consumer = session.createConsumer(ClientCrashTest.QUEUE);
      ClientProducer producer = session.createProducer(ClientCrashTest.QUEUE);

      session.start();

      // receive a message from the queue
      Message messageFromClient = consumer.receive(500000);
      Assert.assertNotNull("no message received", messageFromClient);
      Assert.assertEquals(ClientCrashTest.MESSAGE_TEXT_FROM_CLIENT, messageFromClient.getBodyBuffer().readString());

      assertActiveConnections(1 + 1); // One local and one from the other vm
      assertActiveSession(1 + 1);

      ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                          false,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBodyBuffer().writeString(ClientCrashTest.MESSAGE_TEXT_FROM_SERVER);
      producer.send(message);

      ClientCrashTest.log.debug("waiting for the client VM to crash ...");
      p.waitFor();

      Assert.assertEquals(9, p.exitValue());

      System.out.println("VM Exited");

      Thread.sleep(3 * ClientCrashTest.CONNECTION_TTL);

      assertActiveConnections(1);
      // FIXME https://jira.jboss.org/jira/browse/JBMESSAGING-1421
      assertActiveSession(1);

      session.close();

      Thread.sleep(2 * ClientCrashTest.CONNECTION_TTL);

      // the crash must have been detected and the resources cleaned up
      assertActiveConnections(1);
      // FIXME https://jira.jboss.org/jira/browse/JBMESSAGING-1421
      assertActiveSession(0);
   }
   
   public void testCrashClient2() throws Exception
   {     
      assertActiveConnections(1);

      ClientSession session = sf.createSession(false, true, true);
           
      session.createQueue(ClientCrashTest.QUEUE, ClientCrashTest.QUEUE, null, false);
      
      // spawn a JVM that creates a Core client, which sends a message
      Process p = SpawnedVMSupport.spawnVM(CrashClient2.class.getName());
      
      ClientCrashTest.log.debug("waiting for the client VM to crash ...");
      p.waitFor();

      Assert.assertEquals(9, p.exitValue());

      System.out.println("VM Exited");

      Thread.sleep(3 * ClientCrashTest.CONNECTION_TTL);
      
      ClientConsumer consumer = session.createConsumer(ClientCrashTest.QUEUE);
      
      session.start();

      // receive a message from the queue
      ClientMessage messageFromClient = consumer.receive(10000);
      Assert.assertNotNull("no message received", messageFromClient);
      Assert.assertEquals(ClientCrashTest.MESSAGE_TEXT_FROM_CLIENT, messageFromClient.getBodyBuffer().readString());

      assertEquals(2, messageFromClient.getDeliveryCount());
      
      session.close();

   }

   // Package protected ---------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));

      locator.setClientFailureCheckPeriod(ClientCrashTest.PING_PERIOD);
      locator.setConnectionTTL(ClientCrashTest.CONNECTION_TTL);
      sf = locator.createSessionFactory();
   }

   @Override
   protected void tearDown() throws Exception
   {
      // sf.close();

      sf = null;
      locator.close();
      super.tearDown();
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
