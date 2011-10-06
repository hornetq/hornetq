/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.hornetq.ra.recovery;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.recovery.HornetQResourceRecovery;
import org.hornetq.jms.server.recovery.RecoveryRegistry;
import org.hornetq.jms.server.recovery.XARecoveryConfig;
import org.hornetq.ra.Util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         9/21/11
 */
public class RecoveryManager
{
   private static Logger log = Logger.getLogger(RecoveryManager.class);

   private RecoveryRegistry registry;

   private String resourceRecoveryClassNames = "org.jboss.as.integration.hornetq.recovery.AS5RecoveryRegistry";

   private Map<XARecoveryConfig, HornetQResourceRecovery> configMap = new HashMap<XARecoveryConfig, HornetQResourceRecovery>();

   public void start()
   {
      locateRecoveryRegistry();
   }

   public HornetQResourceRecovery register(HornetQConnectionFactory factory, String userName, String password)
   {
      if(!isRegistered(factory) && registry != null)
      {
         XARecoveryConfig xaRecoveryConfig = new XARecoveryConfig(factory, userName, password);
         HornetQResourceRecovery resourceRecovery = new HornetQResourceRecovery(xaRecoveryConfig);
         registry.register(resourceRecovery);
         configMap.put(xaRecoveryConfig, resourceRecovery);
         return resourceRecovery;
      }
      return null;
   }

   public void unRegister(HornetQResourceRecovery resourceRecovery)
   {
      registry.unRegister(resourceRecovery);
   }

   public void stop()
   {
      for (HornetQResourceRecovery hornetQResourceRecovery : configMap.values())
      {
         registry.unRegister(hornetQResourceRecovery);
      }
      configMap.clear();
   }

   private void locateRecoveryRegistry()
   {
      String locatorClasses[] = resourceRecoveryClassNames.split(";");

      for (int i = 0 ; i < locatorClasses.length; i++)
      {
         registry = Util.locateRecoveryRegistry(locatorClasses[i]);
         if (registry != null)
         {
            break;
         }
      }

      if (registry == null)
      {
         registry = new RecoveryRegistry()
         {
            public void register(HornetQResourceRecovery resourceRecovery)
            {
               //no op
            }

            public void unRegister(HornetQResourceRecovery xaRecoveryConfig)
            {
               //no op
            }
         };
      }
      else
      {
         log.debug("Recovery Registry located = " + registry);
      }
   }


   public boolean isRegistered(HornetQConnectionFactory factory)
   {
      for (XARecoveryConfig xaRecoveryConfig : configMap.keySet())
      {
         TransportConfiguration[] transportConfigurations = factory.getServerLocator().getStaticTransportConfigurations();

         if (transportConfigurations != null)
         {
            TransportConfiguration[] xaConfigurations = xaRecoveryConfig.getHornetQConnectionFactory().getServerLocator().getStaticTransportConfigurations();
            if(xaConfigurations == null)
            {
               break;
            }
            if(transportConfigurations.length != xaConfigurations.length)
            {
               break;
            }
            boolean theSame=true;
            for(int i = 0; i < transportConfigurations.length; i++)
            {
              TransportConfiguration tc = transportConfigurations[i];
              TransportConfiguration xaTc = xaConfigurations[i];
              if(!tc.equals(xaTc))
              {
                 theSame = false;
                 break;
              }
            }
            if(theSame)
            {
               return theSame;
            }
         }
         else
         {
            DiscoveryGroupConfiguration discoveryGroupConfiguration = xaRecoveryConfig.getHornetQConnectionFactory().getServerLocator().getDiscoveryGroupConfiguration();
            if(discoveryGroupConfiguration != null && discoveryGroupConfiguration.equals(factory.getDiscoveryGroupConfiguration()))
            {
               return true;
            }
         }
      }
      return false;
   }

}
