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

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.ConnectorRegistry;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.ConfigurationHelper;
import org.jboss.messaging.core.remoting.impl.ConnectorRegistryImpl;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;

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
      Configuration config = ConfigurationHelper.newConfiguration(TCP, "localhost", PORT);
      
      assertTrue(registry.register(config, dispatcher));
      assertFalse(registry.register(config, dispatcher));
      
      assertTrue(registry.unregister(config));
      assertFalse(registry.unregister(config));

      assertTrue(registry.register(config, dispatcher));
      assertTrue(registry.unregister(config));
   }
   
   public void testRegistrationForTwoConfigurations() throws Exception
   {
      Configuration config_1 = ConfigurationHelper.newConfiguration(TCP, "localhost", PORT);
      Configuration config_2 = ConfigurationHelper.newConfiguration(TCP, "localhost", PORT + 1);     
      PacketDispatcher dispatcher_1 = new PacketDispatcherImpl(null);      
      PacketDispatcher dispatcher_2 = new PacketDispatcherImpl(null);
      
      assertTrue(registry.register(config_1, dispatcher_1));
      assertTrue(registry.register(config_2, dispatcher_2));
      
      assertTrue(registry.unregister(config_1));
      assertTrue(registry.unregister(config_2));
   }
   
   public void testINVMConnectorFromTCPConfiguration() throws Exception
   {
      Configuration config = ConfigurationHelper.newConfiguration(TCP, "localhost", PORT);
      
      // config is registered -> client and server are in the same vm
      assertTrue(registry.register(config, dispatcher));
      
      NIOConnector connector = registry.getConnector(config, dispatcher);
      
      assertTrue(connector.getServerURI().startsWith(INVM.toString()));
      
      assertTrue(registry.unregister(config));
      
      assertNotNull(registry.removeConnector(config));
   }
   
   
   public void testTCPConnectorFromTCPConfiguration() throws Exception
   {
      Configuration config = ConfigurationHelper.newConfiguration(TCP, "localhost", PORT);
      
      // config is not registered -> client and server are not in the same vm
      
      NIOConnector connector = registry.getConnector(config, dispatcher);
      
      assertNotNull(connector);
      assertEquals(config.getURI(), connector.getServerURI());
      
      assertNotNull(registry.removeConnector(config));
   }
   
   public void testConnectorCount() throws Exception
   {
      Configuration config = ConfigurationHelper.newConfiguration(TCP, "localhost", PORT);
      assertEquals(0, registry.getConnectorCount(config));

      NIOConnector connector1 = registry.getConnector(config, dispatcher);
      assertEquals(1, registry.getConnectorCount(config));

      NIOConnector connector2 = registry.getConnector(config, dispatcher);
      assertEquals(2, registry.getConnectorCount(config));

      assertSame(connector1, connector2);
      
      assertNull(registry.removeConnector(config));
      assertEquals(1, registry.getConnectorCount(config));

      NIOConnector connector3 = registry.getConnector(config, dispatcher);
      assertEquals(2, registry.getConnectorCount(config));

      assertSame(connector1, connector3);
      
      assertNull(registry.removeConnector(config));
      assertNotNull(registry.removeConnector(config));
      assertEquals(0, registry.getConnectorCount(config));
   }
   
   public void testConnectorCount_2() throws Exception
   {
      Configuration config1 = ConfigurationHelper.newConfiguration(TCP, "localhost", PORT);
      Configuration config2 = ConfigurationHelper.newConfiguration(TCP, "127.0.0.1", PORT);

      assertNotSame(config1, config2);
      
      assertEquals(0, registry.getConnectorCount(config1));
      assertEquals(0, registry.getConnectorCount(config2));

      NIOConnector connector1 = registry.getConnector(config1, dispatcher);
      assertEquals(1, registry.getConnectorCount(config1));

      NIOConnector connector2 = registry.getConnector(config2, dispatcher);
      assertEquals(1, registry.getConnectorCount(config2));
      
      assertNotSame(connector1, connector2);
      
      assertNotNull(registry.removeConnector(config1));
      assertNotNull(registry.removeConnector(config2));
   }
   
   /**
    * Check that 2 configuration which are similar (but not the same object) will
    * return the same NIOConnector
    */
   public void testConfigurationEquality() throws Exception
   {
      Configuration config1 = ConfigurationHelper.newConfiguration(TCP, "localhost", PORT);
      Configuration config2 = ConfigurationHelper.newConfiguration(TCP, "localhost", PORT);

      assertNotSame(config1, config2);

      NIOConnector connector1 = registry.getConnector(config1, dispatcher);
      assertEquals(1, registry.getConnectorCount(config1));

      NIOConnector connector2 = registry.getConnector(config2, dispatcher);
      assertEquals(2, registry.getConnectorCount(config2));

      assertSame(connector1, connector2);

      assertNull(registry.removeConnector(config1));
      assertNotNull(registry.removeConnector(config2));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
