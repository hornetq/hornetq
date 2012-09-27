/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.api.core;

import java.util.List;

import org.hornetq.api.core.BroadcastEndpoint;
import org.hornetq.core.cluster.BroadcastEndpointFactory;
import org.hornetq.core.cluster.JGroupsFileEndpointFactory;


/**
 * The configuration used to determine how the server will broadcast members
 * This is analogous to {@link org.hornetq.api.core.DiscoveryGroupConfiguration}
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 08:44:30
 *
 */
public class JGroupsBroadcastGroupConfigurationWithFile implements BroadcastEndpointFactoryConfiguration
{
   private static final long serialVersionUID = 8952238567248461285L;

   private String jgroupsFile;

   private String jgroupsChannel;

   public JGroupsBroadcastGroupConfigurationWithFile(final String jgroupsFile, final String jgroupsChannel)
   {
      this.jgroupsFile = jgroupsFile;
      this.jgroupsChannel = jgroupsChannel;
   }

    @Override
    public BroadcastEndpointFactory createBroadcastEndpointFactory()
    {
        return new JGroupsFileEndpointFactory(jgroupsFile, jgroupsChannel);
    }
}
