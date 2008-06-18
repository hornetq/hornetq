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

import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnector;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;
import org.jboss.messaging.tests.unit.core.remoting.impl.SessionTestBase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 */
public class MinaSessionTest extends SessionTestBase
{

   private RemotingServiceImpl service;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // ClientTestBase overrides --------------------------------------

   @Override
   protected RemotingConnector createNIOConnector(PacketDispatcher dispatcher)
   {
      return new MinaConnector(new LocationImpl(TCP, "localhost", TestSupport.PORT), dispatcher);
   }

   @Override
   protected Configuration createRemotingConfiguration()
   {
      return ConfigurationHelper.newTCPConfiguration("localhost", TestSupport.PORT);
   }

   @Override
   protected PacketDispatcher startServer() throws Exception
   {
      service = new RemotingServiceImpl(createRemotingConfiguration());
      service.start();
      return service.getDispatcher();
   }

   @Override
   protected void stopServer()
   {
      service.stop();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
