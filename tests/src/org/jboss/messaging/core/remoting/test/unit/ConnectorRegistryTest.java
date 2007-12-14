/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.test.unit;

import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.PORT;
import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.ConnectorRegistry;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.ServerLocator;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ConnectorRegistryTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testLocatorRegistration() throws Exception
   {
      ServerLocator locator = new ServerLocator(TCP, "localhost", PORT);
      
      assertTrue(ConnectorRegistry.register(locator));
      assertFalse(ConnectorRegistry.register(locator));
      
      assertTrue(ConnectorRegistry.unregister(locator));
      assertFalse(ConnectorRegistry.unregister(locator));

      assertTrue(ConnectorRegistry.register(locator));
      assertTrue(ConnectorRegistry.unregister(locator));
   }
   
   public void testINVMConnectorFromTCPLocator() throws Exception
   {
      ServerLocator locator = new ServerLocator(TCP, "localhost", PORT);
      
      // locator is registered -> client and server are in the same vm
      assertTrue(ConnectorRegistry.register(locator));
      
      NIOConnector connector = ConnectorRegistry.get(locator);
      
      assertTrue(connector.getServerURI().startsWith(INVM.toString()));
      
      assertTrue(ConnectorRegistry.unregister(locator));
   }
   
   
   public void testTCPConnectorFromTCPLocator() throws Exception
   {
      ServerLocator locator = new ServerLocator(TCP, "localhost", PORT);
      
      // locator is not registered -> client and server are not in the same vm
      
      NIOConnector connector = ConnectorRegistry.get(locator);
      
      assertNotNull(connector);
      assertEquals(locator.getURI(), connector.getServerURI());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
