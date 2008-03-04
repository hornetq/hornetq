/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.remoting.ssl.integration;

import static java.lang.Boolean.FALSE;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.impl.RemotingConfiguration;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.test.messaging.jms.SerializedClientSupport;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision: 3716 $</tt>
 * 
 */
public class CoreClientOverSSLTest extends TestCase
{
   // Constants -----------------------------------------------------

   public static final String MESSAGE_TEXT_FROM_CLIENT = "CoreClientOverSSLTest from client";
   public static final String QUEUE = "QueueOverSSL";
   public static final int SSL_PORT = 5402;

   // Static --------------------------------------------------------

   private static final Logger log = Logger
         .getLogger(CoreClientOverSSLTest.class);

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   private ClientConnection connection;

   private ClientConsumer consumer;
   
   // Constructors --------------------------------------------------

   public CoreClientOverSSLTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testSSL() throws Exception
   {
      final Process p = SerializedClientSupport.spawnVM(CoreClientOverSSL.class
            .getName(), Boolean.TRUE.toString(), "messaging.keystore",
            "secureexample");

      Message m = consumer.receive(10000);
      assertNotNull(m);
      assertEquals(MESSAGE_TEXT_FROM_CLIENT, new String(m.getPayload()));

      log.info("waiting for the client VM to exit ...");
      SerializedClientSupport.assertProcessExits(true, 0, p);
   }

   public void testSSLWithIncorrectKeyStorePassword() throws Exception
   {
      Process p = SerializedClientSupport.spawnVM(CoreClientOverSSL.class
            .getName(), Boolean.TRUE.toString(), "messaging.keystore",
            "incorrectKeyStorePassword");

      Message m = consumer.receive(5000);
      assertNull(m);

      log.info("waiting for the client VM to exit ...");
      SerializedClientSupport.assertProcessExits(false, 0, p);
   }

   public void testPlainConnectionToSSLEndpoint() throws Exception
   {
      Process p = SerializedClientSupport.spawnVM(CoreClientOverSSL.class
            .getName(), FALSE.toString(), null, null);

      Message m = consumer.receive(5000);
      assertNull(m);

      log.info("waiting for the client VM to exit ...");
      SerializedClientSupport.assertProcessExits(false, 0, p);
   }

   // Package protected ---------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      RemotingConfiguration remotingConf = new RemotingConfiguration(TCP,
            "localhost", SSL_PORT);
      remotingConf.setSSLEnabled(true);
      remotingConf.setKeyStorePath("messaging.keystore");
      remotingConf.setKeyStorePassword("secureexample");
      remotingConf.setTrustStorePath("messaging.truststore");
      remotingConf.setTrustStorePassword("secureexample");

      server = new MessagingServerImpl(remotingConf);
      server.start();

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(0, remotingConf, server.getVersion());
      connection = cf.createConnection(null, null);
      ClientSession session = connection.createClientSession(false, true, true, -1, false, false);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      consumer = session.createConsumer(QUEUE, null, false, false, true);
      connection.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      consumer.close();
      connection.close();

      server.stop();

      super.tearDown();
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
