package org.hornetq.core.cluster;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.hornetq.core.config.BroadcastEndpointConfiguration;
import org.hornetq.utils.ClassloadingUtil;

public class BroadcastEndpointFactory
{
   public static BroadcastEndpoint createEndpoint(BroadcastEndpointConfiguration endpointConfig) throws Exception
   {
      final String clazz = endpointConfig.getClazz();
      
      BroadcastEndpoint endpoint = AccessController.doPrivileged(new PrivilegedAction<BroadcastEndpoint>()
      {
         public BroadcastEndpoint run()
         {
            return (BroadcastEndpoint)ClassloadingUtil.newInstanceFromClassLoader(clazz);
         }
      });
      
      endpoint.init(endpointConfig.getParams());
      
      return endpoint;
   }
}
