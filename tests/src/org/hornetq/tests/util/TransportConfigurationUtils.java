package org.hornetq.tests.util;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;

public final class TransportConfigurationUtils
{

   public static TransportConfiguration getInVMAcceptor(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName());
      }

      Map<String, Object> server1Params = new HashMap<String, Object>();
      server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      return new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName(), server1Params);
   }

   public static TransportConfiguration getInVMConnector(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(InVMConnectorFactory.class.getCanonicalName());
      }

      Map<String, Object> server1Params = new HashMap<String, Object>();
      server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      return new TransportConfiguration(InVMConnectorFactory.class.getCanonicalName(), server1Params);
   }

}
