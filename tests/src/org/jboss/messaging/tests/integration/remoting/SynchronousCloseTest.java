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

package org.jboss.messaging.tests.integration.remoting;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.tests.util.ServiceTestBase;

/**
 * 
 * A SynchronousCloseTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class SynchronousCloseTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SynchronousCloseTest.class);

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig(isNetty());
      config.setSecurityEnabled(false);
      server = createServer(false, config);
      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      super.tearDown();
   }

   protected boolean isNetty()
   {
      return false;
   }

   protected ClientSessionFactory createSessionFactory()
   {
      ClientSessionFactory sf;
      if (isNetty())
      {
         sf = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      }
      else
      {
         sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      }

      return sf;
   }

   /*
    * Server side resources should be cleaned up befor call to close has returned or client could launch
    * DoS attack
    */
   public void testSynchronousClose() throws Exception
   {
      assertEquals(0, server.getMessagingServerControl().listRemoteAddresses().length);

      ClientSessionFactory sf = createSessionFactory();

      for (int i = 0; i < 100; i++)
      {
         ClientSession session = sf.createSession(false, true, true);

         assertEquals(1, server.getMessagingServerControl().listRemoteAddresses().length);

         log.info("closing session");
         session.close();
         log.info("closed session");

         // Thread.sleep(10000);

         assertEquals(0, server.getMessagingServerControl().listRemoteAddresses().length);
      }

      sf.close();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
