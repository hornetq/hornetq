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

public class SharedStoreSlavePolicyConfiguration implements HAPolicyConfiguration
{
   private long failbackDelay = HornetQDefaultConfiguration.getDefaultFailbackDelay();

   private boolean failoverOnServerShutdown = HornetQDefaultConfiguration.isDefaultFailoverOnServerShutdown();

   private boolean restartBackup = HornetQDefaultConfiguration.isDefaultRestartBackup();

   private ScaleDownConfiguration scaleDownConfiguration;

   public SharedStoreSlavePolicyConfiguration()
   {
   }

   public SharedStoreSlavePolicyConfiguration(long failbackDelay, boolean failoverOnServerShutdown, boolean restartBackup, ScaleDownConfiguration scaleDownConfiguration, boolean allowAutoFailBack)
   {
      this.failbackDelay = failbackDelay;
      this.failoverOnServerShutdown = failoverOnServerShutdown;
      this.restartBackup = restartBackup;
      this.scaleDownConfiguration = scaleDownConfiguration;
      this.allowFailBack = allowAutoFailBack;
   }

   @Override
   public TYPE getType()
   {
      return TYPE.SHARED_STORE_SLAVE;
   }

   public boolean isRestartBackup()
   {
      return restartBackup;
   }

   public void setRestartBackup(boolean restartBackup)
   {
      this.restartBackup = restartBackup;
   }

   public ScaleDownConfiguration getScaleDownConfiguration()
   {
      return scaleDownConfiguration;
   }

   public void setScaleDownConfiguration(ScaleDownConfiguration scaleDownConfiguration)
   {
      this.scaleDownConfiguration = scaleDownConfiguration;
   }

   public boolean isAllowFailBack()
   {
      return allowFailBack;
   }

   public void setAllowFailBack(boolean allowFailBack)
   {
      this.allowFailBack = allowFailBack;
   }

   public boolean isFailoverOnServerShutdown()
   {
      return failoverOnServerShutdown;
   }

   public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   public long getFailbackDelay()
   {
      return failbackDelay;
   }

   public void setFailbackDelay(long failbackDelay)
   {
      this.failbackDelay = failbackDelay;
   }

   private boolean allowFailBack = HornetQDefaultConfiguration.isDefaultAllowAutoFailback();

}
