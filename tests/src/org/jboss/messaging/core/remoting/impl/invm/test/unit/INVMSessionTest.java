/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.invm.test.unit;

import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.PORT;

import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.impl.SessionTestBase;
import org.jboss.messaging.core.remoting.impl.invm.INVMConnector;

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

   PacketDispatcher dispatcher = new PacketDispatcher();
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // ClientTestBase overrides --------------------------------------
   
   @Override
   protected NIOConnector createNIOConnector()
   {
      return new INVMConnector("localhost", PORT, dispatcher);
   }
   
   @Override
   protected RemotingConfiguration createRemotingConfiguration()
   {
      return new RemotingConfiguration(INVM, "localhost", PORT);
   }
   
   @Override
   protected PacketDispatcher startServer() throws Exception
   {
      return dispatcher;
   }
   
   @Override
   protected void stopServer()
   {
      dispatcher = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
