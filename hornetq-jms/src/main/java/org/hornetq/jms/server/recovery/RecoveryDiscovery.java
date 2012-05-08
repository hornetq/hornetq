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

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.jms.HornetQJMSLogger;

/**
 * <p>This class will have a simple Connection Factory and will listen
 *  for topology updates. </p>
 * <p>This Discovery is instantiated by {@link HornetQRecoveryRegistry}
 *
 * @author clebertsuconic
 *
 *
 */
public class RecoveryDiscovery implements SessionFailureListener
{
   
   private ServerLocator locator;
   private ClientSessionFactoryInternal sessionFactory;
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
    	 HornetQJMSLogger.LOGGER.debug("Starting RecoveryDiscovery on " + config);
         started = true;
         
         locator = config.createServerLocator();
         locator.disableFinalizeCheck();
         locator.addClusterTopologyListener(new InternalListener());
         try
         {
            sessionFactory = (ClientSessionFactoryInternal)locator.createSessionFactory();
            // We are using the SessionFactoryInternal here directly as we don't have information to connect with an user and password
            // on the session as all we want here is to get the topology
            // in case of failure we will retry
            sessionFactory.addFailureListener(this);
            
            HornetQJMSLogger.LOGGER.debug("RecoveryDiscovery started fine on " + config);
         }
         catch (Exception startupError)
         {
        	HornetQJMSLogger.LOGGER.warn("Couldn't start recovery discovery on " + config + ", we will retry this on the next recovery scan");
            stop();
            HornetQRecoveryRegistry.getInstance().failedDiscovery(this);
         }
         
      }
   }
   
   public synchronized void stop()
   {
      internalStop();
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

   
   protected void finalize()
   {
      // I don't think it's a good thing to synchronize a method on a finalize,
      // hence the internalStop (no sync) call here
      internalStop();
   }

   protected void internalStop()
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
        	 HornetQJMSLogger.LOGGER.debug(ignored, ignored);
         }
         
         try
         {
            locator.close();
         }
         catch (Exception ignored)
         {
        	 HornetQJMSLogger.LOGGER.debug(ignored, ignored);
         }
         
         sessionFactory = null;
         locator = null;
      }
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


   /* (non-Javadoc)
    * @see org.hornetq.core.remoting.FailureListener#connectionFailed(org.hornetq.api.core.HornetQException, boolean)
    */
   public void connectionFailed(HornetQException exception, boolean failedOver)
   {
      if (exception.getType() == HornetQExceptionType.DISCONNECTED)
      {
    	  HornetQJMSLogger.LOGGER.warn("being disconnected for server shutdown", exception);
      }
      else
      {
    	  HornetQJMSLogger.LOGGER.warn("Notified of connection failure in xa discovery, we will retry on the next recovery",
                                           exception);
      }
      internalStop();
      HornetQRecoveryRegistry.getInstance().failedDiscovery(this);
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.client.SessionFailureListener#beforeReconnect(org.hornetq.api.core.HornetQException)
    */
   public void beforeReconnect(HornetQException exception)
   {
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "RecoveryDiscovery [config=" + config + ", started=" + started + "]";
   }

}
