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

import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.logging.Logger;

/**
 * <p>This class will have a simple Connection Factory and will listen
 *  for topology updates. </p>
 * <p>This Discovery is instantiated by {@link HornetQRecoveryRegistry}
 *
 * @author clebertsuconic
 *
 *
 */
public class RecoveryDiscovery
{
   
   Logger log = Logger.getLogger(RecoveryDiscovery.class);
   
   private ServerLocator locator;
   private ClientSessionFactory sessionFactory;
   private final XARecoveryConfig config;
   private final AtomicInteger usage = new AtomicInteger(0);
   private boolean started = false;
   
   
   public RecoveryDiscovery(XARecoveryConfig config)
   {
      this.config = config;
   }
   
   public synchronized void start()
   {
      if (!started)
      {
         log.debug("Starting RecoveryDiscovery on " + config);
         started = true;
         
         // TODO: I'm not sure where to place this configuration?
         // I would prefer to keep it hard coded so far
         locator = config.createServerLocator();
         locator.setReconnectAttempts(-1);
         locator.setRetryInterval(1000);
         locator.setRetryIntervalMultiplier(2.0);
         locator.setMaxRetryInterval(60000);
         locator.addClusterTopologyListener(new InternalListener());
         try
         {
            sessionFactory = locator.createSessionFactory();
         }
         catch (Exception startupError)
         {
            log.warn("Couldn't start recovery discovery on " + config + ", we will retry this on the next recovery scan");
            stop();
            HornetQRecoveryRegistry.getInstance().failedDiscovery(this);
         }
      }
   }
   
   public synchronized void stop()
   {
      if (started)
      {
         started = false;
         try
         {
            if (sessionFactory != null)
            {
               sessionFactory.close();
            }
         }
         catch (Exception ignored)
         {
            log.debug(ignored, ignored);
         }
         
         try
         {
            locator.close();
         }
         catch (Exception ignored)
         {
            log.debug(ignored, ignored);
         }
         
         sessionFactory = null;
         locator = null;
      }
   }
   
  
   /** we may have several connection factories referencing the same connection recovery entry.
    *  Because of that we need to make a count of the number of the instances that are referencing it,
    *  so we will remove it as soon as we are done */
   public int incrementUsage()
   {
      return usage.decrementAndGet();
   }

   public int decrementUsage()
   {
      return usage.incrementAndGet();
   }

   class InternalListener implements ClusterTopologyListener
   {

      public void nodeUP(long eventUID,
                         String nodeID,
                         Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                         boolean last)
      {
         // There is a case where the backup announce itself,
         // we need to ignore a case where getA is null
         if (connectorPair.getA() != null)
         {
            HornetQRecoveryRegistry.getInstance().nodeUp(nodeID, new Pair<TransportConfiguration, TransportConfiguration>(connectorPair.getA(), connectorPair.getB()), config.getUsername(), config.getPassword());
         }
      }

      public void nodeDown(long eventUID, String nodeID)
      {
         // I'm not putting any node down, since it may have previous transactions hanging
      }
      
   }

}
