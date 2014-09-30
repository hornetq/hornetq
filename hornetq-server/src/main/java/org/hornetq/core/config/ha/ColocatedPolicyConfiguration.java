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

import java.util.ArrayList;
import java.util.List;

public class ColocatedPolicyConfiguration implements HAPolicyConfiguration
{
   private boolean requestBackup = HornetQDefaultConfiguration.isDefaultHapolicyRequestBackup();

   private int backupRequestRetries = HornetQDefaultConfiguration.getDefaultHapolicyBackupRequestRetries();

   private long backupRequestRetryInterval = HornetQDefaultConfiguration.getDefaultHapolicyBackupRequestRetryInterval();

   private int maxBackups = HornetQDefaultConfiguration.getDefaultHapolicyMaxBackups();

   private int backupPortOffset = HornetQDefaultConfiguration.getDefaultHapolicyBackupPortOffset();

   private List<String> remoteConnectors = new ArrayList<>();

   private int portOffset = HornetQDefaultConfiguration.getDefaultHapolicyBackupPortOffset();

   private HAPolicyConfiguration liveConfig;

   private HAPolicyConfiguration backupConfig;

   public ColocatedPolicyConfiguration(boolean requestBackup, int backupRequestRetries, long backupRequestRetryInterval, int maxBackups, int backupPortOffset, List<String> remoteConnectors, int portOffset, HAPolicyConfiguration liveConfig, HAPolicyConfiguration backupConfig)
   {
      this.requestBackup = requestBackup;
      this.backupRequestRetries = backupRequestRetries;
      this.backupRequestRetryInterval = backupRequestRetryInterval;
      this.maxBackups = maxBackups;
      this.backupPortOffset = backupPortOffset;
      this.remoteConnectors = remoteConnectors;
      this.portOffset = portOffset;
      this.liveConfig = liveConfig;
      this.backupConfig = backupConfig;
   }

   public ColocatedPolicyConfiguration()
   {
   }

   @Override
   public TYPE getType()
   {
      return TYPE.COLOCATED;
   }

   public boolean isRequestBackup()
   {
      return requestBackup;
   }

   public void setRequestBackup(boolean requestBackup)
   {
      this.requestBackup = requestBackup;
   }

   public int getBackupRequestRetries()
   {
      return backupRequestRetries;
   }

   public void setBackupRequestRetries(int backupRequestRetries)
   {
      this.backupRequestRetries = backupRequestRetries;
   }

   public long getBackupRequestRetryInterval()
   {
      return backupRequestRetryInterval;
   }

   public void setBackupRequestRetryInterval(long backupRequestRetryInterval)
   {
      this.backupRequestRetryInterval = backupRequestRetryInterval;
   }

   public int getMaxBackups()
   {
      return maxBackups;
   }

   public void setMaxBackups(int maxBackups)
   {
      this.maxBackups = maxBackups;
   }

   public int getBackupPortOffset()
   {
      return backupPortOffset;
   }

   public void setBackupPortOffset(int backupPortOffset)
   {
      this.backupPortOffset = backupPortOffset;
   }

   public List<String> getRemoteConnectors()
   {
      return remoteConnectors;
   }

   public void setRemoteConnectors(List<String> remoteConnectors)
   {
      this.remoteConnectors = remoteConnectors;
   }

   public int getPortOffset()
   {
      return portOffset;
   }

   public void setPortOffset(int portOffset)
   {
      this.portOffset = portOffset;
   }

   public HAPolicyConfiguration getLiveConfig()
   {
      return liveConfig;
   }

   public void setLiveConfig(HAPolicyConfiguration liveConfig)
   {
      this.liveConfig = liveConfig;
   }

   public HAPolicyConfiguration getBackupConfig()
   {
      return backupConfig;
   }

   public void setBackupConfig(HAPolicyConfiguration backupConfig)
   {
      this.backupConfig = backupConfig;
   }
}
