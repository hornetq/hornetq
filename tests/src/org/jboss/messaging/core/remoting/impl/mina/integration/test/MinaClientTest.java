/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.PORT;

import java.io.IOException;

import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.core.remoting.impl.ClientTestBase;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.TextPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MinaClientTest extends ClientTestBase
{

   private MinaService service;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSendBlockingWithTimeout() throws Exception
   {
      client.setBlockingRequestTimeout(500, MILLISECONDS);
      serverPacketHandler.setSleepTime(1000, MILLISECONDS);

      AbstractPacket packet = new TextPacket("testSendBlockingWithTimeout");
      packet.setTargetID(serverPacketHandler.getID());
      
      packet.setVersion((byte) 1);

      try
      {
         client.sendBlocking(packet);
         fail("a IOException should be thrown");
      } catch (IOException e)
      {
      }
   }
   
   // ClientTestBase overrides --------------------------------------
   
   @Override
   protected NIOConnector createNIOConnector()
   {
      return new MinaConnector(TCP, "localhost", PORT);
   }
   
   @Override
   protected ServerLocator createServerLocator()
   {
      return new ServerLocator(TCP, "localhost", PORT);
   }

   @Override
   protected PacketDispatcher startServer() throws Exception
   {
      service = new MinaService(TCP, "localhost", PORT);
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
