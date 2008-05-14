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
package org.jboss.messaging.tests.integration.core.remoting.ssl;

import static java.lang.Boolean.FALSE;
import junit.framework.TestCase;

import org.jboss.messaging.core.client.*;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;
import org.jboss.messaging.tests.unit.core.util.SpawnedVMSupport;
import org.jboss.messaging.util.SimpleString;

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
   public static final SimpleString QUEUE = new SimpleString("QueueOverSSL");
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
      final Process p = SpawnedVMSupport.spawnVM(CoreClientOverSSL.class
            .getName(), Boolean.TRUE.toString(), "messaging.keystore",
            "secureexample");

      Message m = consumer.receive(10000);
      assertNotNull(m);
      assertEquals(MESSAGE_TEXT_FROM_CLIENT, m.getBody().getString());

      log.info("waiting for the client VM to exit ...");
      SpawnedVMSupport.assertProcessExits(true, 0, p);
   }

   public void testSSLWithIncorrectKeyStorePassword() throws Exception
   {
      Process p = SpawnedVMSupport.spawnVM(CoreClientOverSSL.class
            .getName(), Boolean.TRUE.toString(), "messaging.keystore",
            "incorrectKeyStorePassword");

      Message m = consumer.receive(5000);
      assertNull(m);

      log.info("waiting for the client VM to exit ...");
      SpawnedVMSupport.assertProcessExits(false, 0, p);
   }

   public void testPlainConnectionToSSLEndpoint() throws Exception
   {
      Process p = SpawnedVMSupport.spawnVM(CoreClientOverSSL.class
            .getName(), FALSE.toString(), null, null);

      Message m = consumer.receive(5000);
      assertNull(m);

      log.info("waiting for the client VM to exit ...");
      SpawnedVMSupport.assertProcessExits(false, 0, p);
   }

   // Package protected ---------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration(
            "localhost", SSL_PORT);
      config.setSSLEnabled(true);
      config.setKeyStorePath("messaging.keystore");
      config.setKeyStorePassword("secureexample");
      config.setTrustStorePath("messaging.truststore");
      config.setTrustStorePassword("secureexample");

      server = new MessagingServerImpl(config);
      server.start();
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setSSLEnabled(true);
      connectionParams.setKeyStorePath("messaging.keystore");
      connectionParams.setKeyStorePassword("secureexample");
      connectionParams.setTrustStorePath("messaging.truststore");
      connectionParams.setTrustStorePassword("secureexample");
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(config.getLocation(), connectionParams);
      connection = cf.createConnection(null, null);
      ClientSession session = connection.createClientSession(false, true, true, -1, false, false);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      consumer = session.createConsumer(QUEUE);
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
