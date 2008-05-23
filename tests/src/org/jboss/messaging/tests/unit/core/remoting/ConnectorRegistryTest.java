/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting;

import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.tests.integration.core.remoting.mina.TestSupport.PORT;
import junit.framework.TestCase;

import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.ConnectorRegistry;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.ConnectorRegistryImpl;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;

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
      dispatcher = new PacketDispatcherImpl(null);
      assertEquals(0, registry.getRegisteredConfigurationSize());
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, registry.getRegisteredConfigurationSize());
      registry = null;
   }
   
   public void testConfigurationRegistration() throws Exception
   {
      Configuration config = ConfigurationHelper.newTCPConfiguration("localhost", PORT);
      
      assertTrue(registry.register(config.getLocation(), dispatcher));
      assertFalse(registry.register(config.getLocation(), dispatcher));
      
      assertTrue(registry.unregister(config.getLocation()));
      assertFalse(registry.unregister(config.getLocation()));

      assertTrue(registry.register(config.getLocation(), dispatcher));
      assertTrue(registry.unregister(config.getLocation()));
   }
   
   public void testRegistrationForTwoConfigurations() throws Exception
   {
      Configuration config_1 = ConfigurationHelper.newTCPConfiguration("localhost", PORT);
      Configuration config_2 = ConfigurationHelper.newTCPConfiguration("localhost", PORT + 1);     
      PacketDispatcher dispatcher_1 = new PacketDispatcherImpl(null);      
      PacketDispatcher dispatcher_2 = new PacketDispatcherImpl(null);
      
      assertTrue(registry.register(config_1.getLocation(), dispatcher_1));
      assertTrue(registry.register(config_2.getLocation(), dispatcher_2));
      
      assertTrue(registry.unregister(config_1.getLocation()));
      assertTrue(registry.unregister(config_2.getLocation()));
   }

   // TODO run this test when invm transport is reenabled
   public void _testINVMConnectorFromTCPConfiguration() throws Exception
   {
      Configuration config = ConfigurationHelper.newTCPConfiguration("localhost", PORT);
      
      // config is registered -> client and server are in the same vm
      assertTrue(registry.register(config.getLocation(), dispatcher));
      
      NIOConnector connector = registry.getConnector(config.getLocation(), new ConnectionParamsImpl());
      
      assertTrue(connector.getServerURI().startsWith(INVM.toString()));
      
      assertTrue(registry.unregister(config.getLocation()));
      
      assertNotNull(registry.removeConnector(config.getLocation()));
   }
   
   
   public void testTCPConnectorFromTCPConfiguration() throws Exception
   {
      Configuration config = ConfigurationHelper.newTCPConfiguration("localhost", PORT);
      // config is not registered -> client and server are not in the same vm
      
      NIOConnector connector = registry.getConnector(config.getLocation(), config.getConnectionParams());
      
      assertNotNull(connector);
      assertEquals(config.getURI(), connector.getServerURI());
      
      assertNotNull(registry.removeConnector(config.getLocation()));
   }
   
   public void testConnectorCount() throws Exception
   {
      Configuration config = ConfigurationHelper.newTCPConfiguration("localhost", PORT);
      assertEquals(0, registry.getConnectorCount(config.getLocation()));

      NIOConnector connector1 = registry.getConnector(config.getLocation(), new ConnectionParamsImpl());
      assertEquals(1, registry.getConnectorCount(config.getLocation()));

      NIOConnector connector2 = registry.getConnector(config.getLocation(), new ConnectionParamsImpl());
      assertEquals(2, registry.getConnectorCount(config.getLocation()));

      assertSame(connector1, connector2);
      
      assertNull(registry.removeConnector(config.getLocation()));
      assertEquals(1, registry.getConnectorCount(config.getLocation()));

      NIOConnector connector3 = registry.getConnector(config.getLocation(), new ConnectionParamsImpl());
      assertEquals(2, registry.getConnectorCount(config.getLocation()));

      assertSame(connector1, connector3);
      
      assertNull(registry.removeConnector(config.getLocation()));
      assertNotNull(registry.removeConnector(config.getLocation()));
      assertEquals(0, registry.getConnectorCount(config.getLocation()));
   }
   
   public void testConnectorCount_2() throws Exception
   {
      Configuration config1 = ConfigurationHelper.newTCPConfiguration("localhost", PORT);
      Configuration config2 = ConfigurationHelper.newTCPConfiguration("127.0.0.1", PORT);

      assertNotSame(config1, config2);
      
      assertEquals(0, registry.getConnectorCount(config1.getLocation()));
      assertEquals(0, registry.getConnectorCount(config2.getLocation()));

      NIOConnector connector1 = registry.getConnector(config1.getLocation(), new ConnectionParamsImpl());
      assertEquals(1, registry.getConnectorCount(config1.getLocation()));

      NIOConnector connector2 = registry.getConnector(config2.getLocation(), new ConnectionParamsImpl());
      assertEquals(1, registry.getConnectorCount(config2.getLocation()));
      
      assertNotSame(connector1, connector2);
      
      assertNotNull(registry.removeConnector(config1.getLocation()));
      assertNotNull(registry.removeConnector(config2.getLocation()));
   }
   
   /**
    * Check that 2 configuration which are similar (but not the same object) will
    * return the same NIOConnector
    */
   public void testConfigurationEquality() throws Exception
   {
      Configuration config1 = ConfigurationHelper.newTCPConfiguration("localhost", PORT);
      Configuration config2 = ConfigurationHelper.newTCPConfiguration("localhost", PORT);

      assertNotSame(config1, config2);

      NIOConnector connector1 = registry.getConnector(config1.getLocation(), new ConnectionParamsImpl());
      assertEquals(1, registry.getConnectorCount(config1.getLocation()));

      NIOConnector connector2 = registry.getConnector(config2.getLocation(), new ConnectionParamsImpl());
      assertEquals(2, registry.getConnectorCount(config2.getLocation()));

      assertSame(connector1, connector2);

      assertNull(registry.removeConnector(config1.getLocation()));
      assertNotNull(registry.removeConnector(config2.getLocation()));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
