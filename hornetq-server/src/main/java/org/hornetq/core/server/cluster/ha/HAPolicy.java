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
package org.hornetq.core.server.cluster.ha;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.core.config.BackupStrategy;

/**
 * Every live server will have an HAPolicy that configures the type of server that it should be either live, backup or
 * colocated (both). It also configures how, if colocated, it should react to sending and receiving requests for backups.
 */
public class HAPolicy implements Serializable
{
   /**
    * the policy type for a server
    */
   public enum POLICY_TYPE
   {
      LIVE((byte) 0),
      BACKUP_REPLICATED((byte) 1),
      BACKUP_SHARED_STORE((byte) 2),
      COLOCATED_REPLICATED((byte) 3),
      COLOCATED_SHARED_STORE((byte) 4);

      private static final Set<POLICY_TYPE> all = EnumSet.allOf(POLICY_TYPE.class);
      private final byte type;

      POLICY_TYPE(byte type)
      {
         this.type = type;
      }

      public byte getType()
      {
         return type;
      }

      public static POLICY_TYPE toBackupType(byte b)
      {
         for (POLICY_TYPE backupType : all)
         {
            if (b == backupType.getType())
            {
               return backupType;
            }
         }
         return null;
      }

   }

   private POLICY_TYPE policyType = POLICY_TYPE.valueOf(HornetQDefaultConfiguration.getDefaultHapolicyType());

   private BackupStrategy backupStrategy = BackupStrategy.valueOf(HornetQDefaultConfiguration.getDefaultHapolicyBackupStrategy());

   protected boolean scaleDown = HornetQDefaultConfiguration.isDefaultScaleDown();

   private String scaleDownGroupName = null;

   private List<String> scaleDownConnectors = new ArrayList<>();

   private String scaleDownDiscoveryGroup;

   private boolean requestBackup = HornetQDefaultConfiguration.isDefaultHapolicyRequestBackup();

   private int backupRequestRetries = HornetQDefaultConfiguration.getDefaultHapolicyBackupRequestRetries();

   private long backupRequestRetryInterval = HornetQDefaultConfiguration.getDefaultHapolicyBackupRequestRetryInterval();

   private int maxBackups = HornetQDefaultConfiguration.getDefaultHapolicyMaxBackups();

   private int backupPortOffset = HornetQDefaultConfiguration.getDefaultHapolicyBackupPortOffset();

   private List<String> remoteConnectors = new ArrayList<>();

   public POLICY_TYPE getPolicyType()
   {
      return policyType;
   }

   public void setPolicyType(POLICY_TYPE policyType)
   {
      this.policyType = policyType;
   }

   public BackupStrategy getBackupStrategy()
   {
      return backupStrategy;
   }

   public void setBackupStrategy(BackupStrategy backupStrategy)
   {
      this.backupStrategy = backupStrategy;
   }

   /**
    * Should we scaleDown our messages when the server is shutdown cleanly.
    *
    * @return true if server should scaleDown its messages on clean shutdown
    * @see #setScaleDown(boolean)
    */
   public boolean isScaleDown()
   {
      return scaleDown;
   }

   /**
    * Sets whether to allow the server to scaleDown its messages on server shutdown.
    */
   public void setScaleDown(boolean scaleDown)
   {
      this.scaleDown = scaleDown;
   }

   /**
    * returns the name used to group
    *
    * @return the name of the group
    */
   public String getScaleDownGroupName()
   {
      return scaleDownGroupName;
   }

   /**
    * Used to configure groups of live/backup servers.
    *
    * @param nodeGroupName the node group name
    */
   public void setScaleDownGroupName(String nodeGroupName)
   {
      this.scaleDownGroupName = nodeGroupName;
   }

   public List<String> getScaleDownConnectors()
   {
      return scaleDownConnectors;
   }

   public void setScaleDownConnectors(List<String> scaleDownConnectors)
   {
      this.scaleDownConnectors = scaleDownConnectors;
   }

   public void setScaleDownDiscoveryGroup(String scaleDownDiscoveryGroup)
   {
      this.scaleDownDiscoveryGroup = scaleDownDiscoveryGroup;
   }

   public String getScaleDownDiscoveryGroup()
   {
      return scaleDownDiscoveryGroup;
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
}
