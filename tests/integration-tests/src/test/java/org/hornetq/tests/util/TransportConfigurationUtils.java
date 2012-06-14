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
}
