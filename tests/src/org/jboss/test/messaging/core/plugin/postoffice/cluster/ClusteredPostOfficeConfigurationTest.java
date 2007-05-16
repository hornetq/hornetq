package org.jboss.test.messaging.core.plugin.postoffice.cluster;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Vector;

import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.test.messaging.core.plugin.base.PostOfficeTestBase;
import org.jgroups.JChannel;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UDP;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

// This test assumes that bind_addr is not set in the clustered-*-persistence.xml
// configuration file!
// TODO this test actually tests JGroups rather than Messaging
public class ClusteredPostOfficeConfigurationTest extends PostOfficeTestBase
{
   public ClusteredPostOfficeConfigurationTest(String name)
   {
      super(name);
   }
   
   private Properties savedProperties;
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      savedProperties = (Properties) System.getProperties().clone();
      
      Properties systemProperties = System.getProperties();
      
      // Remove JGroups properties if there are any
      systemProperties.remove(org.jgroups.Global.BIND_ADDR_OLD);
      systemProperties.remove(org.jgroups.Global.IGNORE_BIND_ADDRESS_PROPERTY_OLD);

      systemProperties.remove(org.jgroups.Global.BIND_ADDR);
      systemProperties.remove(org.jgroups.Global.IGNORE_BIND_ADDRESS_PROPERTY);
   }
   
   protected void tearDown() throws Exception
   {
      System.setProperties(savedProperties);
      super.tearDown();
   }
   
   protected ClusteredPostOffice createClusteredPostOfficeSimple() throws Exception {
      return createClusteredPostOffice(1, "testgroup", sc, ms, pm, tr);
   }
   
   // TODO these two methods are of course very ugly
   private static JChannel getPostOfficeSyncChannel(ClusteredPostOffice postOffice) throws Exception {
      Field field = DefaultClusteredPostOffice.class.getDeclaredField("syncChannel");
      field.setAccessible(true);
      return (JChannel) field.get(postOffice);
   }
   
   private static JChannel getPostOfficeAsyncChannel(ClusteredPostOffice postOffice) throws Exception {
      Field field = DefaultClusteredPostOffice.class.getDeclaredField("asyncChannel");
      field.setAccessible(true);
      return (JChannel) field.get(postOffice);
   }
   
   private static String getUDPBindAddress(JChannel channel) {
      Vector protocols = channel.getProtocolStack().getProtocols();
      for (int i = 0; i < protocols.size(); i++) {
         Protocol protocol = (Protocol) protocols.get(i);
         if (protocol instanceof UDP) {
            return ((UDP) protocol).getBindAddress();
         }
      }
      
      return null;
   }

   private void assertChannelsBoundTo(InetAddress bindAddress) throws Exception {
      String addressAsString = bindAddress.toString();
      ClusteredPostOffice postOffice = createClusteredPostOfficeSimple();

      JChannel syncChannel = getPostOfficeSyncChannel(postOffice);
      assertEquals(addressAsString, getUDPBindAddress(syncChannel));
      
      JChannel asyncChannel = getPostOfficeAsyncChannel(postOffice);
      assertEquals(addressAsString, getUDPBindAddress(asyncChannel));
   }

   public void testNoProperties() throws Exception {
      InetAddress address = org.jgroups.util.Util.getFirstNonLoopbackAddress();
      if (address == null) {
         fail("No address available for JGroups to bind to");
      }
      assertChannelsBoundTo(address);
   }
   
   public void testBindAddressPropertySet() throws Exception {
      String address = "127.0.0.1";
      System.setProperty(org.jgroups.Global.BIND_ADDR, address);
      assertChannelsBoundTo(InetAddress.getByName(address));
   }
}
