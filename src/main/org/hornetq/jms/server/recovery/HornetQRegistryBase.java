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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.jboss.tm.XAResourceRecoveryRegistry;

/**
 * This class is a base class for the integration layer where
 * we verify if a given connection factory already have a recovery registered
 *
 * @author Clebert
 * @author Andy Taylor
 *
 *
 */
public abstract class HornetQRegistryBase implements RecoveryRegistry
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(HornetQRegistryBase.class);

   // Attributes ----------------------------------------------------

   private static Set<HornetQResourceRecovery> configSet = new HashSet<HornetQResourceRecovery>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public abstract XAResourceRecoveryRegistry getTMRegistry();

   public HornetQResourceRecovery register(final HornetQResourceRecovery resourceRecovery)
   {
      synchronized (configSet)
      {
         HornetQResourceRecovery usedInstance = locateSimilarResource(resourceRecovery);
         if (usedInstance == null)
         {
            if (log.isDebugEnabled())
            {
               log.debug("Adding " + resourceRecovery.getConfig() + " resource = " + resourceRecovery);
            }
            usedInstance = resourceRecovery;
            configSet.add(usedInstance);
            getTMRegistry().addXAResourceRecovery(usedInstance);
         }
         usedInstance.incrementUsage();
         return usedInstance;
      }
   }



   public synchronized void unRegister(final HornetQResourceRecovery resourceRecovery)
   {
      synchronized (configSet)
      {
         // The same resource could have been reused by more than one resource manager or factory
         if (resourceRecovery.decrementUsage() == 0)
         {
            getTMRegistry().removeXAResourceRecovery(resourceRecovery);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   private static HornetQResourceRecovery locateSimilarResource(HornetQResourceRecovery resourceInput)
   {
      HornetQConnectionFactory factory = resourceInput.getConfig().getFactory();
      
      TransportConfiguration[] transportConfigurations = resourceInput.getConfig().getFactory().getServerLocator()
               .getStaticTransportConfigurations();

      
      if (log.isTraceEnabled())
      {
         log.trace("############################################## looking for a place on " + Arrays.toString(transportConfigurations));
      }
      
      for (HornetQResourceRecovery resourceScan : configSet)
      {
         XARecoveryConfig xaRecoveryConfig = resourceScan.getConfig();

         if (transportConfigurations != null)
         {
            TransportConfiguration[] xaConfigurations = xaRecoveryConfig.getHornetQConnectionFactory().getServerLocator()
                  .getStaticTransportConfigurations();
            
            if (log.isTraceEnabled())
            {
               log.trace("Checking " + Arrays.toString(transportConfigurations) + " against " + Arrays.toString(xaConfigurations));
            }

            if (xaConfigurations == null)
            {
               continue;
            }
            if (transportConfigurations.length != xaConfigurations.length)
            {
               if (log.isTraceEnabled())
               {
                  log.trace(Arrays.toString(transportConfigurations) + " != " + Arrays.toString(xaConfigurations) + " because of size");
               }
               continue;
            }
            boolean theSame = true;
            for (int i = 0; i < transportConfigurations.length; i++)
            {
               TransportConfiguration tc = transportConfigurations[i];
               TransportConfiguration xaTc = xaConfigurations[i];
               if (!tc.equals(xaTc))
               {
                  log.info(Arrays.toString(transportConfigurations) + " != " + Arrays.toString(xaConfigurations) + " because of " + tc + " != " + xaTc);
                  theSame = false;
                  break;
               }
            }
            if (theSame)
            {
               return resourceScan;
            }
         } else
         {
            DiscoveryGroupConfiguration discoveryGroupConfiguration = xaRecoveryConfig.getHornetQConnectionFactory()
                  .getServerLocator().getDiscoveryGroupConfiguration();
            if (discoveryGroupConfiguration != null && discoveryGroupConfiguration.equals(factory.getDiscoveryGroupConfiguration()))
            {
               return resourceScan;
            }
         }
      }

      return null;

   }

   // Inner classes -------------------------------------------------

}
