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

package org.jboss.messaging.tests.integration.core.remoting.mina;

import junit.framework.TestCase;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.RemotingConnector;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.RemotingSession;
import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.impl.invm.INVMConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;

import java.io.IOException;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MinaServiceTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------t------------------------

   private RemotingService invmService;

   public void testINVMConnector_OK() throws Exception
   {
      RemotingConnector connector = new INVMConnector(null, null, 1, new PacketDispatcherImpl(null), invmService.getDispatcher());
      RemotingSession session = connector.connect();

      assertTrue(session.isConnected());
      assertTrue(connector.disconnect());
      assertFalse(session.isConnected());
   }

   // disabled test since INVM transport is disabled for JBM2 alpha: JBMESSAGING-1348
   public void _testMinaConnector_Failure() throws Exception
   {
      RemotingConnector connector = new MinaConnector(new LocationImpl(
              TCP, "localhost", 9000), new PacketDispatcherImpl(null));

      try
      {
         connector.connect();
         fail("MINA service started in invm: can not connect to it through TCP");
      }
      catch (IOException e)
      {

      }
   }

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(INVM);
      invmService = new RemotingServiceImpl(config);
      invmService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      invmService.stop();

      super.tearDown();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
