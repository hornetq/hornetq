/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.clientcrash;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.SpawnedVMSupport;
import org.jboss.messaging.utils.SimpleString;

/**
 * A test that makes sure that a Messaging server cleans up the associated
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
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCrashClient() throws Exception
   {
      assertActiveConnections(0);
      
      // spawn a JVM that creates a JMS client, which waits to receive a test
      // message
      Process p = SpawnedVMSupport.spawnVM(CrashClient.class.getName());
      
      ClientSession session = sf.createSession(false, true, true);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      ClientConsumer consumer = session.createConsumer(QUEUE);
      ClientProducer producer = session.createProducer(QUEUE);
      
      session.start();
      
      // send the message to the queue
      Message messageFromClient = consumer.receive(5000);
      assertNotNull("no message received", messageFromClient);
      assertEquals(MESSAGE_TEXT_FROM_CLIENT, messageFromClient.getBody().readString());
      
      assertActiveConnections(1 + 1); // One local and one from the other vm
      assertActiveSession(1 + 1);
      
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                          false,
                                                          0,
                                                          System.currentTimeMillis(),
                                                          (byte)1);
      message.getBody().writeString(ClientCrashTest.MESSAGE_TEXT_FROM_SERVER);
      producer.send(message);
      
      log.debug("waiting for the client VM to crash ...");
      p.waitFor();
      
      assertEquals(9, p.exitValue());
      
      System.out.println("VM Exited");

      Thread.sleep(1000);
      
      assertActiveConnections(1);
      // FIXME https://jira.jboss.org/jira/browse/JBMESSAGING-1421
      // assertActiveSession(1);      

      session.close();

      Thread.sleep(1000);

      // the crash must have been detected and the resources cleaned up
      assertActiveConnections(0);
      // FIXME https://jira.jboss.org/jira/browse/JBMESSAGING-1421
      // assertActiveSession(0);      
   }

   // Package protected ---------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory"),
                                        null,
                                        DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                        PING_PERIOD,
                                        CONNECTION_TTL,
                                        DEFAULT_CALL_TIMEOUT,
                                        DEFAULT_CONSUMER_WINDOW_SIZE,
                                        DEFAULT_CONSUMER_MAX_RATE,
                                        DEFAULT_SEND_WINDOW_SIZE,
                                        DEFAULT_PRODUCER_MAX_RATE,
                                        DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                        DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                        DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                        DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                        DEFAULT_AUTO_GROUP,
                                        DEFAULT_MAX_CONNECTIONS,
                                        DEFAULT_PRE_ACKNOWLEDGE,
                                        DEFAULT_ACK_BATCH_SIZE,                                 
                                        DEFAULT_RETRY_INTERVAL,
                                        DEFAULT_RETRY_INTERVAL_MULTIPLIER,                                        
                                        DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                        DEFAULT_MAX_RETRIES_AFTER_FAILOVER);

   }

   @Override
   protected void tearDown() throws Exception
   {
    //  sf.close();
      
      super.tearDown();
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
