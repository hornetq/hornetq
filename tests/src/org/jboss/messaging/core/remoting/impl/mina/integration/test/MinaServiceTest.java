package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import static org.jboss.messaging.core.remoting.TransportType.TCP;

import java.io.IOException;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.RemotingConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.INVMConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;

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
      NIOConnector connector = new INVMConnector(new PacketDispatcherImpl(), invmService.getDispatcher());
      NIOSession session = connector.connect();

      assertTrue(session.isConnected());
      assertTrue(connector.disconnect());
      assertFalse(session.isConnected());
   }

   public void testMinaConnector_Failure() throws Exception
   {
      NIOConnector connector = new MinaConnector(new RemotingConfigurationImpl(
            TCP, "localhost", 9000), new PacketDispatcherImpl());

      try
      {
         connector.connect();
         fail("MINA service started in invm: can not connect to it through TCP");
      } catch (IOException e)
      {

      }
   }

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      invmService = new MinaService(RemotingConfigurationImpl.newINVMConfiguration());
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
