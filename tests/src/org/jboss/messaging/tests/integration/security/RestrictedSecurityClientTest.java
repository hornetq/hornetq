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

package org.jboss.messaging.tests.integration.security;

import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.net.URL;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.tests.integration.clientcrash.GracefulClient;
import org.jboss.messaging.tests.util.SpawnedVMSupport;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class RestrictedSecurityClientTest extends TestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final String MESSAGE_TEXT = randomString();
   
   private static final SimpleString QUEUE = randomSimpleString();
      
   // Static ---------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RestrictedSecurityClientTest.class);

   // Attributes -----------------------------------------------------------------------------------

   private MessagingService messagingService;

   private ClientSession session;

   private ClientConsumer consumer;   

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   public void testRestrictedSecurityClient() throws Exception
   {
      String policyFile = "restricted-security-client.policy";
      URL policyFileURL = Thread.currentThread().getContextClassLoader().getResource(policyFile);
      assertNotNull(policyFileURL);
      // spawn a JVM that creates a client with a restrictive security manager which sends a test message
      Process p = SpawnedVMSupport.spawnVM(GracefulClient.class.getName(), 
                                           new String[] {"-Djava.security.manager", 
                                                         "-Djava.security.policy==" + policyFileURL.getPath()},
                                           new String[] {QUEUE.toString(), MESSAGE_TEXT});

      // read the message from the queue
      Message message = consumer.receive(15000);

      assertNotNull("did not receive message from the spawned client", message);
      assertEquals(MESSAGE_TEXT, message.getBody().getString());

      // the client VM should exit by itself. If it doesn't, that means we have a problem
      // and the test will timeout
      log.debug("waiting for the client VM to exit ...");
      p.waitFor();

      assertEquals(0, p.exitValue());
   }

   // Package protected ----------------------------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setSecurityEnabled(false);
      config.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));
      messagingService = Messaging.newNullStorageMessagingService(config);
      messagingService.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      consumer = session.createConsumer(QUEUE);
      session.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      consumer.close();
      session.close();

      messagingService.stop();

      super.tearDown();
   }
   
   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
