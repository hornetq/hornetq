/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.tests.util.SpawnedVMSupport;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * A NettySecurityClientTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class NettySecurityClientTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(NettySecurityClientTest.class);

   // Attributes ----------------------------------------------------

   private MessagingServer messagingService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   public void testProducerConsumerClientWithoutSecurityManager() throws Exception
   {
      doTestProducerConsumerClient(false);
   }

   public void testProducerConsumerClientWithSecurityManager() throws Exception
   {
      doTestProducerConsumerClient(true);
   }
   

   // SecurityManagerClientTestBase overrides -----------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      ConfigurationImpl config = new ConfigurationImpl();
      config.setSecurityEnabled(false);
      config.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));
      messagingService = Messaging.newMessagingServer(config, false);
      messagingService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      messagingService.stop();

      super.tearDown();
   }
   
   // Private -------------------------------------------------------

   private void doTestProducerConsumerClient(boolean withSecurityManager) throws Exception
   {
      String[] vmargs = new String[0];
      if (withSecurityManager)
      {
         URL securityPolicyURL = Thread.currentThread().getContextClassLoader().getResource("restricted-security-client.policy");
         vmargs = new String[] { "-Djava.security.manager", "-Djava.security.policy=" + securityPolicyURL.getPath() };
      }

      // spawn a JVM that creates a client withor without a security manager which sends and receives a test message
      Process p = SpawnedVMSupport.spawnVM(SimpleClient.class.getName(),
                                           vmargs,
                                           false,
                                           new String[] { NettyConnectorFactory.class.getName() });

      InputStreamReader isr = new InputStreamReader(p.getInputStream());
      BufferedReader br = new BufferedReader(isr);
      String line = null;
      while ((line = br.readLine()) != null)
      {
         line = line.replace('|', '\n');
         if (line.startsWith("Listening"))
         {
            continue;
         }
         else if ("OK".equals(line.trim()))
         {
            break;
         } else
         {
            fail("Exception when starting the client: " + line);
         }
      }
      
      
      SpawnedVMSupport.startLogger(SimpleClient.class.getName(), p);

      // the client VM should exit by itself. If it doesn't, that means we have a problem
      // and the test will timeout
      log.debug("waiting for the client VM to exit ...");
      p.waitFor();

      assertEquals("client VM did not exit cleanly", 0, p.exitValue());
   }
   
   // Inner classes -------------------------------------------------

}
