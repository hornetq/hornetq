/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.PORT;

import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.core.remoting.impl.ClientTestBase;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;

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
   protected void startServer() throws Exception
   {
      service = new MinaService(TCP, "localhost", PORT);
      service.start();
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
