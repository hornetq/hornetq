/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting.impl.invm;

import static org.jboss.messaging.core.remoting.TransportType.INVM;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.PacketDispatcher;
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
   protected NIOConnector createNIOConnector(PacketDispatcher dispatcher)
   {
      return new INVMConnector(1, dispatcher, serverDispatcher);
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
