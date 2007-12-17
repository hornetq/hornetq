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
   
   @Override
   protected void setUp() throws Exception
   {
      assertEquals(0, ConnectorRegistry.getRegisteredLocators().length);
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, ConnectorRegistry.getRegisteredLocators().length);
   }
   
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
      
      NIOConnector connector = ConnectorRegistry.getConnector(locator);
      
      assertTrue(connector.getServerURI().startsWith(INVM.toString()));
      
      assertTrue(ConnectorRegistry.unregister(locator));
      
      assertNotNull(ConnectorRegistry.removeConnector(locator));
   }
   
   
   public void testTCPConnectorFromTCPLocator() throws Exception
   {
      ServerLocator locator = new ServerLocator(TCP, "localhost", PORT);
      
      // locator is not registered -> client and server are not in the same vm
      
      NIOConnector connector = ConnectorRegistry.getConnector(locator);
      
      assertNotNull(connector);
      assertEquals(locator.getURI(), connector.getServerURI());
      
      assertNotNull(ConnectorRegistry.removeConnector(locator));
   }
   
   public void testConnectorCount() throws Exception
   {
      ServerLocator locator = new ServerLocator(TCP, "localhost", PORT);
      assertEquals(0, ConnectorRegistry.getConnectorCount(locator));

      NIOConnector connector1 = ConnectorRegistry.getConnector(locator);
      assertEquals(1, ConnectorRegistry.getConnectorCount(locator));

      NIOConnector connector2 = ConnectorRegistry.getConnector(locator);
      assertEquals(2, ConnectorRegistry.getConnectorCount(locator));

      assertSame(connector1, connector2);
      
      assertNull(ConnectorRegistry.removeConnector(locator));
      assertEquals(1, ConnectorRegistry.getConnectorCount(locator));

      NIOConnector connector3 = ConnectorRegistry.getConnector(locator);
      assertEquals(2, ConnectorRegistry.getConnectorCount(locator));

      assertSame(connector1, connector3);
      
      assertNull(ConnectorRegistry.removeConnector(locator));
      assertNotNull(ConnectorRegistry.removeConnector(locator));
      assertEquals(0, ConnectorRegistry.getConnectorCount(locator));
   }
   
   public void testConnectorCount_2() throws Exception
   {
      ServerLocator locator1 = new ServerLocator(TCP, "localhost", PORT);
      ServerLocator locator2 = new ServerLocator(TCP, "127.0.0.1", PORT);

      assertNotSame(locator1, locator2);
      
      assertEquals(0, ConnectorRegistry.getConnectorCount(locator1));
      assertEquals(0, ConnectorRegistry.getConnectorCount(locator2));

      NIOConnector connector1 = ConnectorRegistry.getConnector(locator1);
      assertEquals(1, ConnectorRegistry.getConnectorCount(locator1));

      NIOConnector connector2 = ConnectorRegistry.getConnector(locator2);
      assertEquals(1, ConnectorRegistry.getConnectorCount(locator2));
      
      assertNotSame(connector1, connector2);
      
      assertNotNull(ConnectorRegistry.removeConnector(locator1));
      assertNotNull(ConnectorRegistry.removeConnector(locator2));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
