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
import org.hornetq.core.config.ScaleDownConfiguration;

public class ReplicaPolicyConfiguration implements HAPolicyConfiguration
{
   private String clusterName;

   private int maxSavedReplicatedJournalsSize = HornetQDefaultConfiguration.getDefaultMaxSavedReplicatedJournalsSize();

   private String groupName = null;

   private boolean restartBackup = HornetQDefaultConfiguration.isDefaultRestartBackup();

   private ScaleDownConfiguration scaleDownConfiguration;

   /*
   * used in the replicated policy after failover
   * */
   private boolean allowFailBack;

   private long failbackDelay = HornetQDefaultConfiguration.getDefaultFailbackDelay();

   public ReplicaPolicyConfiguration(String clusterName, int maxSavedReplicatedJournalsSize, String groupName, boolean restartBackup, boolean allowFailBack, long failbackDelay, ScaleDownConfiguration scaleDownConfiguration)
   {
      this.clusterName = clusterName;
      this.maxSavedReplicatedJournalsSize = maxSavedReplicatedJournalsSize;
      this.groupName = groupName;
      this.restartBackup = restartBackup;
      this.allowFailBack = allowFailBack;
      this.failbackDelay = failbackDelay;
      this.scaleDownConfiguration = scaleDownConfiguration;
   }

   public ReplicaPolicyConfiguration()
   {
   }

   @Override
   public TYPE getType()
   {
      return TYPE.REPLICA;
   }

   public ScaleDownConfiguration getScaleDownConfiguration()
   {
      return scaleDownConfiguration;
   }

   public void setScaleDownConfiguration(ScaleDownConfiguration scaleDownConfiguration)
   {
      this.scaleDownConfiguration = scaleDownConfiguration;
   }



   public String getClusterName()
   {
      return clusterName;
   }

   public void setClusterName(String clusterName)
   {
      this.clusterName = clusterName;
   }

   public int getMaxSavedReplicatedJournalsSize()
   {
      return maxSavedReplicatedJournalsSize;
   }

   public void setMaxSavedReplicatedJournalsSize(int maxSavedReplicatedJournalsSize)
   {
      this.maxSavedReplicatedJournalsSize = maxSavedReplicatedJournalsSize;
   }

   public String getGroupName()
   {
      return groupName;
   }

   public void setGroupName(String groupName)
   {
      this.groupName = groupName;
   }

   public boolean isRestartBackup()
   {
      return restartBackup;
   }

   public void setRestartBackup(boolean restartBackup)
   {
      this.restartBackup = restartBackup;
   }

   public boolean isAllowFailBack()
   {
      return allowFailBack;
   }

   public void setAllowFailBack(boolean allowFailBack)
   {
      this.allowFailBack = allowFailBack;
   }

   public void setFailbackDelay(long failbackDelay)
   {
      this.failbackDelay = failbackDelay;
   }

   public long getFailbackDelay()
   {
      return failbackDelay;
   }
}
