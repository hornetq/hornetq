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
import org.jboss.messaging.core.remoting.impl.ConnectorRegistryImpl;
import org.jboss.messaging.core.remoting.impl.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.RemotingConfiguration;

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
      assertEquals(0, registry.getRegisteredRemotingConfigurations().length);
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, registry.getRegisteredRemotingConfigurations().length);
      registry = null;
   }
   
   public void testRemotingConfigurationRegistration() throws Exception
   {
      RemotingConfiguration remotingConfig = new RemotingConfiguration(TCP, "localhost", PORT);
      
      assertTrue(registry.register(remotingConfig, dispatcher));
      assertFalse(registry.register(remotingConfig, dispatcher));
      
      assertTrue(registry.unregister(remotingConfig));
      assertFalse(registry.unregister(remotingConfig));

      assertTrue(registry.register(remotingConfig, dispatcher));
      assertTrue(registry.unregister(remotingConfig));
   }
   
   public void testRegistrationForTwoRemotingConfigurations() throws Exception
   {
      RemotingConfiguration remotingConfig_1 = new RemotingConfiguration(TCP, "localhost", PORT);
      RemotingConfiguration remotingConfig_2 = new RemotingConfiguration(TCP, "localhost", PORT + 1);     
      PacketDispatcher dispatcher_1 = new PacketDispatcher();      
      PacketDispatcher dispatcher_2 = new PacketDispatcher();
      
      assertTrue(registry.register(remotingConfig_1, dispatcher_1));
      assertTrue(registry.register(remotingConfig_2, dispatcher_2));
      
      assertTrue(registry.unregister(remotingConfig_1));
      assertTrue(registry.unregister(remotingConfig_2));
   }
   
   public void testINVMConnectorFromTCPRemotingConfiguration() throws Exception
   {
      RemotingConfiguration remotingConfig = new RemotingConfiguration(TCP, "localhost", PORT);
      
      // remotingConfig is registered -> client and server are in the same vm
      assertTrue(registry.register(remotingConfig, dispatcher));
      
      NIOConnector connector = registry.getConnector(remotingConfig);
      
      assertTrue(connector.getServerURI().startsWith(INVM.toString()));
      
      assertTrue(registry.unregister(remotingConfig));
      
      assertNotNull(registry.removeConnector(remotingConfig));
   }
   
   
   public void testTCPConnectorFromTCPRemotingConfiguration() throws Exception
   {
      RemotingConfiguration remotingConfig = new RemotingConfiguration(TCP, "localhost", PORT);
      
      // remotingConfig is not registered -> client and server are not in the same vm
      
      NIOConnector connector = registry.getConnector(remotingConfig);
      
      assertNotNull(connector);
      assertEquals(remotingConfig.getURI(), connector.getServerURI());
      
      assertNotNull(registry.removeConnector(remotingConfig));
   }
   
   public void testConnectorCount() throws Exception
   {
      RemotingConfiguration remotingConfig = new RemotingConfiguration(TCP, "localhost", PORT);
      assertEquals(0, registry.getConnectorCount(remotingConfig));

      NIOConnector connector1 = registry.getConnector(remotingConfig);
      assertEquals(1, registry.getConnectorCount(remotingConfig));

      NIOConnector connector2 = registry.getConnector(remotingConfig);
      assertEquals(2, registry.getConnectorCount(remotingConfig));

      assertSame(connector1, connector2);
      
      assertNull(registry.removeConnector(remotingConfig));
      assertEquals(1, registry.getConnectorCount(remotingConfig));

      NIOConnector connector3 = registry.getConnector(remotingConfig);
      assertEquals(2, registry.getConnectorCount(remotingConfig));

      assertSame(connector1, connector3);
      
      assertNull(registry.removeConnector(remotingConfig));
      assertNotNull(registry.removeConnector(remotingConfig));
      assertEquals(0, registry.getConnectorCount(remotingConfig));
   }
   
   public void testConnectorCount_2() throws Exception
   {
      RemotingConfiguration remotingConfig1 = new RemotingConfiguration(TCP, "localhost", PORT);
      RemotingConfiguration remotingConfig2 = new RemotingConfiguration(TCP, "127.0.0.1", PORT);

      assertNotSame(remotingConfig1, remotingConfig2);
      
      assertEquals(0, registry.getConnectorCount(remotingConfig1));
      assertEquals(0, registry.getConnectorCount(remotingConfig2));

      NIOConnector connector1 = registry.getConnector(remotingConfig1);
      assertEquals(1, registry.getConnectorCount(remotingConfig1));

      NIOConnector connector2 = registry.getConnector(remotingConfig2);
      assertEquals(1, registry.getConnectorCount(remotingConfig2));
      
      assertNotSame(connector1, connector2);
      
      assertNotNull(registry.removeConnector(remotingConfig1));
      assertNotNull(registry.removeConnector(remotingConfig2));
   }
   
   /**
    * Check that 2 RemotingConfiguration which are equals (but not the same object) will
    * return the same NIOConnector
    */
   public void testRemotingConfigurationEquality() throws Exception
   {
      RemotingConfiguration remotingConfig1 = new RemotingConfiguration(TCP, "localhost", PORT);
      RemotingConfiguration remotingConfig2 = new RemotingConfiguration(TCP, "localhost", PORT);

      assertNotSame(remotingConfig1, remotingConfig2);
      assertEquals(remotingConfig1, remotingConfig2);

      NIOConnector connector1 = registry.getConnector(remotingConfig1);
      assertEquals(1, registry.getConnectorCount(remotingConfig1));

      NIOConnector connector2 = registry.getConnector(remotingConfig2);
      assertEquals(2, registry.getConnectorCount(remotingConfig2));

      assertSame(connector1, connector2);

      assertNull(registry.removeConnector(remotingConfig1));
      assertNotNull(registry.removeConnector(remotingConfig2));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
