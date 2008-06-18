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

package org.jboss.messaging.tests.unit.core.remoting.impl.invm;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnector;
import static org.jboss.messaging.core.remoting.TransportType.INVM;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.invm.INVMConnector;
import org.jboss.messaging.tests.unit.core.remoting.impl.SessionTestBase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class INVMSessionTest extends SessionTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   PacketDispatcher serverDispatcher = new PacketDispatcherImpl(null);
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // ClientTestBase overrides --------------------------------------
   
   @Override
   protected RemotingConnector createNIOConnector(PacketDispatcher dispatcher)
   {
      return new INVMConnector(null, null, 1, dispatcher, serverDispatcher);
   }
   
   @Override
   protected Configuration createRemotingConfiguration()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(INVM);
      return config;
   }
   
   @Override
   protected PacketDispatcher startServer() throws Exception
   {
      return serverDispatcher;
   }
   
   @Override
   protected void stopServer()
   {
      serverDispatcher = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
