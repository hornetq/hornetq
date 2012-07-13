package org.hornetq.core.cluster;

import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.hornetq.core.cluster.impl.JGroupsBroadcastEndpoint;
import org.hornetq.core.cluster.impl.UDPBroadcastEndpoint;
import org.hornetq.utils.ClassloadingUtil;

public class BroadcastEndpointFactory
{
   public static BroadcastEndpoint createUDPEndpoint(final String groupAddress,
                                                     final int groupPort,
                                                     final String localBindAddress,
                                                     final int localBindPort) throws Exception
   {
      return createUDPEndpoint(groupAddress != null ? InetAddress.getByName(groupAddress) : null, groupPort,
         localBindAddress != null ? InetAddress.getByName(localBindAddress) : null, localBindPort);
   }

   public static BroadcastEndpoint createUDPEndpoint(final InetAddress groupAddress,
                                                     final int groupPort,
                                                     final InetAddress localBindAddress,
                                                     final int localBindPort) throws Exception
   {
      return new UDPBroadcastEndpoint(groupAddress, groupPort, localBindAddress, localBindPort);
   }

   public static BroadcastEndpoint createJGropusEndpoint(final String fileName, final String channelName) throws Exception
   {
      //  I don't want any possible hard coded dependency on JGropus,
      //       for that reason we use reflection here, to avoid the compiler to bring any dependency here
      return AccessController.doPrivileged(new PrivilegedAction<BroadcastEndpoint>()
      {
         public BroadcastEndpoint run()
         {
            BroadcastEndpoint endpoint = (BroadcastEndpoint) ClassloadingUtil.
                    newInstanceFromClassLoader("org.hornetq.core.cluster.impl.JGroupsBroadcastEndpoint", fileName, channelName);
            return endpoint;
         }
      });
   }
}
