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
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.core.remoting.impl.ConnectorRegistryImpl;

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
   
   private ConnectorRegistry registry;
   private PacketDispatcher dispatcher;

   @Override
   protected void setUp() throws Exception
   {
      registry = new ConnectorRegistryImpl();
      dispatcher = new PacketDispatcher();
      assertEquals(0, registry.getRegisteredLocators().length);
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, registry.getRegisteredLocators().length);
      registry = null;
   }
   
   public void testLocatorRegistration() throws Exception
   {
      ServerLocator locator = new ServerLocator(TCP, "localhost", PORT);
      
      assertTrue(registry.register(locator, dispatcher));
      assertFalse(registry.register(locator, dispatcher));
      
      assertTrue(registry.unregister(locator));
      assertFalse(registry.unregister(locator));

      assertTrue(registry.register(locator, dispatcher));
      assertTrue(registry.unregister(locator));
   }
   
   public void testINVMConnectorFromTCPLocator() throws Exception
   {
      ServerLocator locator = new ServerLocator(TCP, "localhost", PORT);
      
      // locator is registered -> client and server are in the same vm
      assertTrue(registry.register(locator, dispatcher));
      
      NIOConnector connector = registry.getConnector(locator);
      
      assertTrue(connector.getServerURI().startsWith(INVM.toString()));
      
      assertTrue(registry.unregister(locator));
      
      assertNotNull(registry.removeConnector(locator));
   }
   
   
   public void testTCPConnectorFromTCPLocator() throws Exception
   {
      ServerLocator locator = new ServerLocator(TCP, "localhost", PORT);
      
      // locator is not registered -> client and server are not in the same vm
      
      NIOConnector connector = registry.getConnector(locator);
      
      assertNotNull(connector);
      assertEquals(locator.getURI(), connector.getServerURI());
      
      assertNotNull(registry.removeConnector(locator));
   }
   
   public void testConnectorCount() throws Exception
   {
      ServerLocator locator = new ServerLocator(TCP, "localhost", PORT);
      assertEquals(0, registry.getConnectorCount(locator));

      NIOConnector connector1 = registry.getConnector(locator);
      assertEquals(1, registry.getConnectorCount(locator));

      NIOConnector connector2 = registry.getConnector(locator);
      assertEquals(2, registry.getConnectorCount(locator));

      assertSame(connector1, connector2);
      
      assertNull(registry.removeConnector(locator));
      assertEquals(1, registry.getConnectorCount(locator));

      NIOConnector connector3 = registry.getConnector(locator);
      assertEquals(2, registry.getConnectorCount(locator));

      assertSame(connector1, connector3);
      
      assertNull(registry.removeConnector(locator));
      assertNotNull(registry.removeConnector(locator));
      assertEquals(0, registry.getConnectorCount(locator));
   }
   
   public void testConnectorCount_2() throws Exception
   {
      ServerLocator locator1 = new ServerLocator(TCP, "localhost", PORT);
      ServerLocator locator2 = new ServerLocator(TCP, "127.0.0.1", PORT);

      assertNotSame(locator1, locator2);
      
      assertEquals(0, registry.getConnectorCount(locator1));
      assertEquals(0, registry.getConnectorCount(locator2));

      NIOConnector connector1 = registry.getConnector(locator1);
      assertEquals(1, registry.getConnectorCount(locator1));

      NIOConnector connector2 = registry.getConnector(locator2);
      assertEquals(1, registry.getConnectorCount(locator2));
      
      assertNotSame(connector1, connector2);
      
      assertNotNull(registry.removeConnector(locator1));
      assertNotNull(registry.removeConnector(locator2));
   }
   
   /**
    * Check that 2 ServerLocators which are equals (but not the same object) will
    * return the same NIOConnector
    */
   public void testServerLocatorEquality() throws Exception
   {
      ServerLocator locator1 = new ServerLocator(TCP, "localhost", PORT);
      ServerLocator locator2 = new ServerLocator(TCP, "localhost", PORT);

      assertNotSame(locator1, locator2);
      assertEquals(locator1, locator2);

      NIOConnector connector1 = registry.getConnector(locator1);
      assertEquals(1, registry.getConnectorCount(locator1));

      NIOConnector connector2 = registry.getConnector(locator2);
      assertEquals(2, registry.getConnectorCount(locator2));

      assertSame(connector1, connector2);

      assertNull(registry.removeConnector(locator1));
      assertNotNull(registry.removeConnector(locator2));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
