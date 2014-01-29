package org.hornetq.tests.util;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.remoting.impl.invm.TransportConstants;

public final class TransportConfigurationUtils
{

   private TransportConfigurationUtils()
   {
      // Utility
   }

   public static TransportConfiguration getInVMAcceptor(final boolean live)
   {
      return transportConfiguration(UnitTestCase.INVM_ACCEPTOR_FACTORY, live);
   }

   public static TransportConfiguration getInVMConnector(final boolean live)
   {
      return transportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY, live);
   }

   public static TransportConfiguration getInVMAcceptor(final boolean live, int server)
   {
      return transportConfiguration(UnitTestCase.INVM_ACCEPTOR_FACTORY, live, server);
   }

   public static TransportConfiguration getInVMConnector(final boolean live, int server)
   {
      return transportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY, live, server);
   }

   public static TransportConfiguration getNettyAcceptor(final boolean live, int server)
   {
      return transportConfiguration(UnitTestCase.NETTY_ACCEPTOR_FACTORY, live, server);
   }

   public static TransportConfiguration getNettyConnector(final boolean live, int server)
   {
      return transportConfiguration(UnitTestCase.NETTY_CONNECTOR_FACTORY, live, server);
   }

   public static TransportConfiguration getInVMAcceptor(final boolean live, int server, String name)
   {
      return transportConfiguration(UnitTestCase.INVM_ACCEPTOR_FACTORY, live, server, name);
   }

   public static TransportConfiguration getInVMConnector(final boolean live, int server, String name)
   {
      return transportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY, live, server, name);
   }

   public static TransportConfiguration getNettyAcceptor(final boolean live, int server, String name)
   {
      return transportConfiguration(UnitTestCase.NETTY_ACCEPTOR_FACTORY, live, server, name);
   }

   public static TransportConfiguration getNettyConnector(final boolean live, int server, String name)
   {
      return transportConfiguration(UnitTestCase.NETTY_CONNECTOR_FACTORY, live, server, name);
   }

   /**
    * @param classname
    * @param live
    * @return
    */
   private static TransportConfiguration transportConfiguration(String classname, boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(classname);
      }

      Map<String, Object> server1Params = new HashMap<String, Object>();
      server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      return new TransportConfiguration(classname, server1Params);
   }

   private static TransportConfiguration transportConfiguration(String classname, boolean live, int server)
   {
      if (classname.contains("netty"))
      {
         Map<String, Object> serverParams = new HashMap<String, Object>();
         Integer port = live ? 5445 : 5545;
         serverParams.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, port);
         return new TransportConfiguration(classname, serverParams);
      }

      Map<String, Object> serverParams = new HashMap<String, Object>();
      serverParams.put(TransportConstants.SERVER_ID_PROP_NAME, live ? server : server + 100);
      return new TransportConfiguration(classname, serverParams);
   }

   private static TransportConfiguration transportConfiguration(String classname, boolean live, int server, String name)
   {
      if (classname.contains("netty"))
      {
         Map<String, Object> serverParams = new HashMap<String, Object>();
         Integer port = live ? 5445 : 5545;
         serverParams.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, port);
         return new TransportConfiguration(classname, serverParams, name);
      }

      Map<String, Object> serverParams = new HashMap<String, Object>();
      serverParams.put(TransportConstants.SERVER_ID_PROP_NAME, live ? server : server + 100);
      return new TransportConfiguration(classname, serverParams, name);
   }
}
