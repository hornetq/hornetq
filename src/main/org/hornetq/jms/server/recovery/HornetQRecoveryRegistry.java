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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.XAResource;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.jboss.tm.XAResourceRecovery;

/**
 * A HornetQRecoveryRegistry
 *
 * @author clebertsuconic
 *
 *
 */
public class HornetQRecoveryRegistry implements XAResourceRecovery
{

   private static final Logger log = Logger.getLogger(HornetQRecoveryRegistry.class);

   private final static HornetQRecoveryRegistry theInstance = new HornetQRecoveryRegistry();

   private ConcurrentHashMap<XARecoveryConfig, RecoveryDiscovery> configSet = new ConcurrentHashMap<XARecoveryConfig, RecoveryDiscovery>();

   private ConcurrentHashMap<String, HornetQXAResourceWrapper> recoveries = new ConcurrentHashMap<String, HornetQXAResourceWrapper>();

   private Set<RecoveryDiscovery> failedDiscoverySet = new HashSet<RecoveryDiscovery>();

   private HornetQRecoveryRegistry()
   {
   }

   /** This will be called periodically by the Transaction Manager*/
   public XAResource[] getXAResources()
   {
      try
      {
         checkFailures();

         HornetQXAResourceWrapper[] resourceArray = new HornetQXAResourceWrapper[recoveries.size()];
         resourceArray = recoveries.values().toArray(resourceArray);

         if (log.isDebugEnabled())
         {
            log.debug("\n=======================================================================================");
            log.debug("Returning the following list on getXAREsources:");
            for (Map.Entry<String, HornetQXAResourceWrapper> entry : recoveries.entrySet())
            {
               log.debug("server-id=" + entry.getKey() + ", value=" + entry.getValue());
            }
            log.debug("=======================================================================================\n");
         }

         return resourceArray;
      }
      catch (Throwable e)
      {
         log.warn(e.getMessage(), e);
         return new XAResource[] {};
      }
   }

   public static HornetQRecoveryRegistry getInstance()
   {
      return theInstance;
   }

   public void register(final XARecoveryConfig resourceConfig)
   {
      RecoveryDiscovery newInstance = new RecoveryDiscovery(resourceConfig);
      RecoveryDiscovery discoveryRecord = configSet.putIfAbsent(resourceConfig, newInstance);
      if (discoveryRecord == null)
      {
         discoveryRecord = newInstance;
         discoveryRecord.start();
      }
      discoveryRecord.incrementUsage();
   }

   public void unRegister(final XARecoveryConfig resourceConfig)
   {
      RecoveryDiscovery discoveryRecord = configSet.get(resourceConfig);
      if (discoveryRecord != null && discoveryRecord.decrementUsage() == 0)
      {
         discoveryRecord = configSet.remove(resourceConfig);
         if (discoveryRecord != null)
         {
            discoveryRecord.stop();
         }
      }
   }

   public void failedDiscovery(RecoveryDiscovery failedDiscovery)
   {
      synchronized (failedDiscoverySet)
      {
         failedDiscoverySet.add(failedDiscovery);
      }
   }

   public void checkFailures()
   {
      final HashSet<RecoveryDiscovery> failures = new HashSet<RecoveryDiscovery>();

      // it will transfer all the discoveries to a new collection
      synchronized (failedDiscoverySet)
      {
         failures.addAll(failedDiscoverySet);
         failedDiscoverySet.clear();
      }

      if (failures.size() > 0)
      {
         // This shouldn't happen on a regular scenario, however when this retry happens this needs
         // to be done on a new thread
         Thread t = new Thread("HornetQ Recovery Discovery Reinitialization")
         {
            public void run()
            {
               for (RecoveryDiscovery discovery : failures)
               {
                  discovery.start();
               }
            }
         };

         t.start();
      }
   }

   /**
    * @param nodeID
    * @param pair
    * @param username
    * @param password
    */
   public void nodeUp(String nodeID,
                      Pair<TransportConfiguration, TransportConfiguration> networkConfiguration,
                      String username,
                      String password)
   {

      if (recoveries.get(nodeID) == null)
      {
         if (log.isDebugEnabled())
         {
            log.debug(nodeID + " being registered towards " + networkConfiguration);
         }
         XARecoveryConfig config = new XARecoveryConfig(true,
                                                        extractTransportConfiguration(networkConfiguration),
                                                        username,
                                                        password);

         HornetQXAResourceWrapper wrapper = new HornetQXAResourceWrapper(config);
         recoveries.put(nodeID, wrapper);
      }
   }

   public void nodeDown(String nodeID)
   {
   }

   /**
    * @param networkConfiguration
    * @return
    */
   private TransportConfiguration[] extractTransportConfiguration(Pair<TransportConfiguration, TransportConfiguration> networkConfiguration)
   {
      if (networkConfiguration.getB() != null)
      {
         return new TransportConfiguration[] { networkConfiguration.getA(), networkConfiguration.getB() };
      }
      else
      {
         return new TransportConfiguration[] { networkConfiguration.getA() };
      }
   }

}
