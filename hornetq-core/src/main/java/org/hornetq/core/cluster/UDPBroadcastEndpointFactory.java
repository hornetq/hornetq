/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.hornetq.core.cluster;

import org.hornetq.api.core.BroadcastEndpoint;
import org.hornetq.api.core.UDPBroadcastEndpoint;

import java.net.InetAddress;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         9/25/12
 */
public class UDPBroadcastEndpointFactory implements BroadcastEndpointFactory
{
    private final String groupAddress;
    private final int groupPort;
    private final String localBindAddress;
    private final int localBindPort;

    public UDPBroadcastEndpointFactory(String groupAddress, int groupPort, String localBindAddress, int localBindPort)
    {
        this.groupAddress = groupAddress;
        this.groupPort = groupPort;
        this.localBindAddress = localBindAddress;
        this.localBindPort = localBindPort;
    }

    public BroadcastEndpoint createBroadcastEndpoint() throws Exception
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

}
