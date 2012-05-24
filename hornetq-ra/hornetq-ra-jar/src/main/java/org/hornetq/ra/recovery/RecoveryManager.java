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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;

import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.recovery.HornetQRecoveryRegistry;
import org.hornetq.jms.server.recovery.HornetQRegistryBase;
import org.hornetq.jms.server.recovery.XARecoveryConfig;
import org.hornetq.ra.HornetQRALogger;
import org.hornetq.utils.ClassloadingUtil;
import org.hornetq.utils.ConcurrentHashSet;
import org.jboss.tm.XAResourceRecoveryRegistry;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         9/21/11
 */
public class RecoveryManager
{
   private HornetQRegistryBase registry;

   private String resourceRecoveryClassNames = "org.jboss.as.messaging.jms.AS7RecoveryRegistry;org.jboss.as.integration.hornetq.recovery.AS5RecoveryRegistry";
   
   private final Set<XARecoveryConfig> resources = new ConcurrentHashSet<XARecoveryConfig>(); 

   public void start(final boolean useAutoRecovery)
   {
      if (useAutoRecovery)
      {
         locateRecoveryRegistry();
      }
      else
      {
         registry = new EmptyRecoveryRegistry();
      }
   }

   public XARecoveryConfig register(HornetQConnectionFactory factory, String userName, String password)
   {
      HornetQRALogger.LOGGER.debug("registering recovery for factory : " + factory);
      
      XARecoveryConfig config = newResourceConfig(factory, userName, password);
      resources.add(config);
      if (registry != null)
      {
         registry.register(config);
      }
      return config;
   }

   /**
    * @param factory
    * @param userName
    * @param password
    * @return
    */
   private XARecoveryConfig newResourceConfig(HornetQConnectionFactory factory,
                                                       String userName,
                                                       String password)
   {
      if (factory.getServerLocator().getDiscoveryGroupConfiguration() != null)
      {
        return new XARecoveryConfig(factory.getServerLocator().isHA(), factory.getServerLocator().getDiscoveryGroupConfiguration(), userName, password);
      }
      else
      {
         return new XARecoveryConfig(factory.getServerLocator().isHA(), factory.getServerLocator().getStaticTransportConfigurations(), userName, password);
      }
   }

   public void unRegister(XARecoveryConfig resourceRecovery)
   {
      registry.unRegister(resourceRecovery);
   }

   public void stop()
   {
      for (XARecoveryConfig recovery : resources)
      {
         registry.unRegister(recovery);
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
            registry = (HornetQRegistryBase) safeInitNewInstance(locatorClasses[i]);
         }
         catch (Throwable e)
         {
            HornetQRALogger.LOGGER.debug("unable to load  recovery registry " + locatorClasses[i], e);
         }
         if (registry != null)
         {
            break;
         }
      }

      if (registry == null)
      {
         registry = new EmptyRecoveryRegistry();
      }
      else
      {
         HornetQRALogger.LOGGER.debug("Recovery Registry located = " + registry);
      }
   }

   /** This seems duplicate code all over the place, but for security reasons we can't let something like this to be open in a
    *  utility class, as it would be a door to load anything you like in a safe VM.
    *  For that reason any class trying to do a privileged block should do with the AccessController directly.
    */
   private static Object safeInitNewInstance(final String className)
   {
      return AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            return ClassloadingUtil.newInstanceFromClassLoader(className);
         }
      });
   }

   private static class EmptyRecoveryRegistry extends HornetQRegistryBase
   {

      @Override
      public XAResourceRecoveryRegistry getTMRegistry()
      {
         return null;
      }
      

      /** no need to register any discovery since we woulnd't do anything with it */
      public void register(final XARecoveryConfig resourceConfig)
      {
      }

      /** no need to register any discovery since we woulnd't do anything with it */
      public void unRegister(final XARecoveryConfig resourceConfig)
      {
      }

   }
}
