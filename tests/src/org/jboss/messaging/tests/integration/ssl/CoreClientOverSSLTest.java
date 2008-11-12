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

package org.jboss.messaging.tests.integration.ssl;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingService;
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

   private MessagingService messagingService;

   private ClientSession session;

   private ClientConsumer consumer;
   
   // Constructors --------------------------------------------------

   public CoreClientOverSSLTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testDummy()
   {
      //This whole test needs to be rewritten - there's no need for it to be spawning vms
   }
   
//   public void testSSL() throws Exception
//   {
//      final Process p = SpawnedVMSupport.spawnVM(CoreClientOverSSL.class
//            .getName(), Boolean.TRUE.toString());
//
//      Message m = consumer.receive(10000);
//      assertNotNull(m);
//      assertEquals(MESSAGE_TEXT_FROM_CLIENT, m.getBody().getString());
//
//      log.debug("waiting for the client VM to exit ...");
//      SpawnedVMSupport.assertProcessExits(true, 0, p);
//   }
//
//   public void testSSLWithIncorrectKeyStorePassword() throws Exception
//   {
//      Process p = SpawnedVMSupport.spawnVM(CoreClientOverSSL.class
//            .getName(), Boolean.TRUE.toString());
//
//      Message m = consumer.receive(5000);
//      assertNull(m);
//
//      log.debug("waiting for the client VM to exit ...");
//      SpawnedVMSupport.assertProcessExits(false, 0, p);
//   }
//
//   public void testPlainConnectionToSSLEndpoint() throws Exception
//   {
//      Process p = SpawnedVMSupport.spawnVM(CoreClientOverSSL.class
//            .getName(), FALSE.toString());
//
//      Message m = consumer.receive(5000);
//      assertNull(m);
//
//      log.debug("waiting for the client VM to exit ...");
//      SpawnedVMSupport.assertProcessExits(false, 0, p);
//   }

   // Package protected ---------------------------------------------

//   @Override
//   protected void setUp() throws Exception
//   {
//      ConfigurationImpl config = new ConfigurationImpl();
//      config.setSecurityEnabled(false);
//      Map<String, Object> params = new HashMap<String, Object>();
//      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
//      config.getAcceptorInfos().add(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory", params));
//      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(config);      
//      messagingService.start();
//      ConnectorFactory cf = new NettyConnectorFactory();
//      ClientSessionFactory sf = new ClientSessionFactoryImpl(cf);    
//      sf.setTransportParams(params);
//      session = sf.createSession(false, true, true, -1, false);
//      session.createQueue(QUEUE, QUEUE, null, false, false);
//      consumer = session.createConsumer(QUEUE);
//      session.start();
//   }
//
//   @Override
//   protected void tearDown() throws Exception
//   {
//      consumer.close();
//      session.close();
//
//      messagingService.stop();
//
//      super.tearDown();
//   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
