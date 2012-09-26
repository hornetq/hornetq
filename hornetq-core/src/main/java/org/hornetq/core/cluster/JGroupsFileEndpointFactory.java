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
import org.hornetq.utils.ClassloadingUtil;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         9/25/12
 */
public class JGroupsFileEndpointFactory implements BroadcastEndpointFactory
{
    private final String fileName;

    private final String channelName;

    public JGroupsFileEndpointFactory(String fileName, String channelName)
    {
        this.fileName = fileName;
        this.channelName = channelName;
    }

    public BroadcastEndpoint createBroadcastEndpoint() throws Exception
    {
       //  I don't want any possible hard coded dependency on JGroups,
       //       for that reason we use reflection here, to avoid the compiler to bring any dependency here
       return AccessController.doPrivileged(new PrivilegedAction<BroadcastEndpoint>()
       {
            public BroadcastEndpoint run()
            {
                BroadcastEndpoint endpoint = (BroadcastEndpoint) ClassloadingUtil.
                       newInstanceFromClassLoader("org.hornetq.api.core.JGroupsBroadcastEndpointWithFile", fileName, channelName);
                return endpoint;
            }
        });
    }
}
