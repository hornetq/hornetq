/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.jms.server.recovery;

import java.util.HashMap;

import org.hornetq.core.logging.Logger;
import org.jboss.tm.XAResourceRecoveryRegistry;

/**
 * This class is a base class for the integration layer where
 * we verify if a given connection factory already have a recovery registered
 *
 * @author Clebert
 *
 *
 */
public abstract class HornetQRegistryBase implements RecoveryRegistry
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(HornetQRegistryBase.class);

   // Attributes ----------------------------------------------------

   private static HashMap<XARecoveryConfig, HornetQResourceRecovery> configSet = new HashMap<XARecoveryConfig, HornetQResourceRecovery>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public abstract XAResourceRecoveryRegistry getTMRegistry();

   public HornetQResourceRecovery register(final HornetQResourceRecovery resourceRecovery)
   {
      synchronized (configSet)
      {
         HornetQResourceRecovery recovery = configSet.get(resourceRecovery.getConfig());
         
         if (recovery == null)
         {
            recovery = resourceRecovery;
            if (log.isDebugEnabled())
            {
               log.debug("Registering a new recovery for " + recovery.getConfig() + ", recovery = " + resourceRecovery);
            }
            configSet.put(resourceRecovery.getConfig(), resourceRecovery);
            getTMRegistry().addXAResourceRecovery(recovery);
         }
         else
         {
            if (log.isDebugEnabled())
            {
               log.info("Return pre-existent recovery=" + recovery + " for configuration = " + resourceRecovery.getConfig());
            }
         }
         recovery.incrementUsage();
         return recovery;
      }
   }



   public void unRegister(final HornetQResourceRecovery resourceRecovery)
   {
      synchronized (configSet)
      {
         HornetQResourceRecovery recFound = configSet.get(resourceRecovery.getConfig());
         
         if (recFound != null && recFound.decrementUsage() == 0)
         {
            if (log.isDebugEnabled())
            {
               log.debug("Removing recovery information for " + recFound + " as all the deployments were already removed");
            }
            getTMRegistry().removeXAResourceRecovery(recFound);
            configSet.remove(resourceRecovery);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
    // Inner classes -------------------------------------------------

}
