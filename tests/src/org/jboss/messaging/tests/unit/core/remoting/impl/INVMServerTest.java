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

package org.jboss.messaging.tests.unit.core.remoting.impl;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientConnectionInternal;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;

/**
 * 
 * @author <a href="mailto:jeff.mesnil@jboss.com">Jeff Mesnil</a>
 *
 */
public class INVMServerTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service_1;
   private MessagingService service_2;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testConnectionsToTwoINVMServers() throws Exception
   {
      ClientConnectionFactory cf_1 = new ClientConnectionFactoryImpl(new LocationImpl(0));
      ClientConnectionInternal conn_1 = (ClientConnectionInternal) cf_1.createConnection();

      ClientConnectionFactory cf_2 = new ClientConnectionFactoryImpl(new LocationImpl(1));
      ClientConnectionInternal conn_2 = (ClientConnectionInternal) cf_2.createConnection();
      
      assertNotSame(conn_1.getRemotingConnection().getSessionID(), conn_2.getRemotingConnection().getSessionID());

      conn_1.close();
      conn_2.close();      
}

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      ConfigurationImpl config = new ConfigurationImpl();
      config.setServerID(0);
      config.setTransport(TransportType.INVM);
      service_1 = MessagingServiceImpl.newNullStorageMessagingServer(config);
      service_1.start();

      config = new ConfigurationImpl();
      config.setServerID(1);
      config.setTransport(TransportType.INVM);
      service_2 = MessagingServiceImpl.newNullStorageMessagingServer(config);
      service_2.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      service_1.stop();
      service_2.stop();

      super.tearDown();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
