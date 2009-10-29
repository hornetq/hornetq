/*
 * Copyright 2009 Red Hat, Inc.
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
package org.hornetq.service;

import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.security.HornetQSecurityManager;

import javax.management.MBeanServer;
import javax.management.MBeanRegistration;
import javax.management.ObjectName;

/**
 * @author <a href="mailto:lucazamador@gmail.com">Lucaz Amador</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class HornetQStarterService implements HornetQStarterServiceMBean, MBeanRegistration
{
   JBossASSecurityManagerServiceMBean securityManagerService;

   HornetQFileConfigurationServiceMBean configurationService;

   private MBeanServer mBeanServer;

   private HornetQServer server;

   private boolean start = true;

   public void create() throws Exception
   {
      Configuration config;
      HornetQSecurityManager hornetQSecurityManager = null;
      if(securityManagerService != null)
      {
         hornetQSecurityManager = securityManagerService.getJBossASSecurityManager();
      }
      if(configurationService != null)
      {
         config = configurationService.getConfiguration();
      }
      else
      {
         config = new ConfigurationImpl();   
      }
      server = new HornetQServerImpl(config, mBeanServer, hornetQSecurityManager);
   }

   public void start() throws Exception
   {
      if(start)
      {
         server.start();
      }
   }

   public void stop() throws Exception
   {
      if(start)
      {
         server.stop();
      }
   }

   public HornetQServer getServer()
   {
      return server;
   }

   public void setStart(boolean start)
   {
      this.start = start;
   }

   public void setSecurityManagerService(JBossASSecurityManagerServiceMBean securityManagerService)
   {
      this.securityManagerService = securityManagerService;
   }

   public void setConfigurationService(HornetQFileConfigurationServiceMBean configurationService)
   {
      this.configurationService = configurationService;
   }

   public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception
   {
      this.mBeanServer = server;

      return name;
   }

   public void postRegister(Boolean registrationDone)
   {
      // NO - OP
   }

   public void preDeregister() throws Exception
   {
      // NO - OP
   }

   public void postDeregister()
   {
      server = null;
      mBeanServer = null;
   }
}
