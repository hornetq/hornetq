package org.hornetq.core.cluster;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.hornetq.core.config.BroadcastEndpointConfiguration;

public class BroadcastEndpointFactory
{
   public static BroadcastEndpoint createEndpoint(BroadcastEndpointConfiguration endpointConfig) throws Exception
   {
      final String className = endpointConfig.getClazz();
      
      BroadcastEndpoint endpoint = AccessController.doPrivileged(new PrivilegedAction<BroadcastEndpoint>()
      {
         public BroadcastEndpoint run()
         {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try
            {
               Class<?> clazz = loader.loadClass(className);
               return (BroadcastEndpoint)clazz.newInstance();
            }
            catch (Exception e)
            {
               throw new IllegalArgumentException("Error instantiating endpoint \"" + className +
                                                           "\"",
                                                  e);
            }
         }
      });
      
      endpoint.init(endpointConfig.getParams());
      
      return endpoint;
   }
}
