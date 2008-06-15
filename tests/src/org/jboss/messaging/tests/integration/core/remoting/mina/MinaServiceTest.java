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
