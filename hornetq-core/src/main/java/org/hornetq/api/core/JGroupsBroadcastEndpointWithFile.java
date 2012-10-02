/*
 * Copyright 2012 Red Hat, Inc.
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

import java.net.URL;

import org.jgroups.JChannel;


/**
 * This class is the implementation of HornetQ members discovery that will use JGroups.
 * @author Tomohisa
 * @author Howard Gao
 * @author Clebert Suconic
 */
public class JGroupsBroadcastEndpointWithFile extends AbstractJGroupsBroadcastEndpoint
{
   public JGroupsBroadcastEndpointWithFile(final String fileName, final String channelName) throws Exception
   {
      super(channelName);
      URL configURL = Thread.currentThread().getContextClassLoader().getResource(fileName);
      if (configURL == null)
      {
         throw new RuntimeException("couldn't find JGroups configuration " + fileName);
      }
      channel = new JChannel(configURL);
   }
}
