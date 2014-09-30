/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.core.config.ha;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.core.config.HAPolicyConfiguration;

public class ReplicatedPolicyConfiguration implements HAPolicyConfiguration
{
   private boolean checkForLiveServer = HornetQDefaultConfiguration.isDefaultCheckForLiveServer();

   private boolean allowAutoFailBack = HornetQDefaultConfiguration.isDefaultAllowAutoFailback();

   private long failbackDelay = HornetQDefaultConfiguration.getDefaultFailbackDelay();

   private String groupName = null;

   private String clusterName;
   public ReplicatedPolicyConfiguration(boolean checkForLiveServer, boolean allowAutoFailBack, long failbackDelay, String groupName, String clusterName)
   {
      this.checkForLiveServer = checkForLiveServer;
      this.allowAutoFailBack = allowAutoFailBack;
      this.failbackDelay = failbackDelay;
      this.groupName = groupName;
      this.clusterName = clusterName;
   }

   public ReplicatedPolicyConfiguration()
   {
   }

   @Override
   public TYPE getType()
   {
      return TYPE.REPLICATED;
   }

   public boolean isCheckForLiveServer()
   {
      return checkForLiveServer;
   }

   public void setCheckForLiveServer(boolean checkForLiveServer)
   {
      this.checkForLiveServer = checkForLiveServer;
   }

   public boolean isAllowAutoFailBack()
   {
      return allowAutoFailBack;
   }

   public void setAllowAutoFailBack(boolean allowAutoFailBack)
   {
      this.allowAutoFailBack = allowAutoFailBack;
   }

   public long getFailbackDelay()
   {
      return failbackDelay;
   }

   public void setFailbackDelay(long failbackDelay)
   {
      this.failbackDelay = failbackDelay;
   }

   public String getGroupName()
   {
      return groupName;
   }

   public void setGroupName(String groupName)
   {
      this.groupName = groupName;
   }

   public String getClusterName()
   {
      return clusterName;
   }

   public void setClusterName(String clusterName)
   {
      this.clusterName = clusterName;
   }
}
