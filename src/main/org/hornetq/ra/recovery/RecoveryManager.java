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

import java.util.Set;

import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.recovery.HornetQResourceRecovery;
import org.hornetq.jms.server.recovery.RecoveryRegistry;
import org.hornetq.jms.server.recovery.XARecoveryConfig;
import org.hornetq.ra.Util;
import org.hornetq.utils.ConcurrentHashSet;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         9/21/11
 */
public class RecoveryManager
{
   private static Logger log = Logger.getLogger(RecoveryManager.class);

   private RecoveryRegistry registry;

   private String resourceRecoveryClassNames = "org.jboss.as.integration.hornetq.recovery.AS5RecoveryRegistry";
   
   private final Set<HornetQResourceRecovery> resources = new ConcurrentHashSet<HornetQResourceRecovery>(); 

   public void start()
   {
      locateRecoveryRegistry();
   }

   public HornetQResourceRecovery register(HornetQConnectionFactory factory, String userName, String password)
   {
      log.debug("registering recovery for factory : " + factory);
      
      HornetQResourceRecovery resourceRecovery = newResourceRecovery(factory, userName, password);
      
      if (registry != null)
      {
         resourceRecovery = registry.register(resourceRecovery);
         if (resourceRecovery != null)
         {
            resources.add(resourceRecovery);
         }
      }
      
      return resourceRecovery;
   }

   /**
    * @param factory
    * @param userName
    * @param password
    * @return
    */
   private HornetQResourceRecovery newResourceRecovery(HornetQConnectionFactory factory,
                                                       String userName,
                                                       String password)
   {
      XARecoveryConfig xaRecoveryConfig;

      if (factory.getServerLocator().getDiscoveryGroupConfiguration() != null)
      {
         xaRecoveryConfig = new XARecoveryConfig(factory.getServerLocator().isHA(), factory.getServerLocator().getDiscoveryGroupConfiguration(), userName, password);
      }
      else
      {
         xaRecoveryConfig = new XARecoveryConfig(factory.getServerLocator().isHA(), factory.getServerLocator().getStaticTransportConfigurations(), userName, password);
      }
      
      HornetQResourceRecovery resourceRecovery = new HornetQResourceRecovery(xaRecoveryConfig);
      return resourceRecovery;
   }

   public void unRegister(HornetQResourceRecovery resourceRecovery)
   {
      registry.unRegister(resourceRecovery);
   }

   public void stop()
   {
      for (HornetQResourceRecovery hornetQResourceRecovery : resources)
      {
         registry.unRegister(hornetQResourceRecovery);
      }
      resources.clear();
   }

   private void locateRecoveryRegistry()
   {
      String locatorClasses[] = resourceRecoveryClassNames.split(";");

      for (int i = 0 ; i < locatorClasses.length; i++)
      {
         try
         {
            registry = Util.locateRecoveryRegistry(locatorClasses[i]);
         }
         catch (Throwable e)
         {
            log.debug("unable to load  recovery registry " + locatorClasses[i], e);
         }
         if (registry != null)
         {
            break;
         }
      }

      if (registry == null)
      {
         registry = new RecoveryRegistry()
         {
            public HornetQResourceRecovery register(HornetQResourceRecovery resourceRecovery)
            {
               return null;
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


}
