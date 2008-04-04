/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
   * by the @authors tag. See the copyright.txt in the distribution for a
   * full listing of individual contributors.
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
package org.jboss.test.messaging.tools;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Set;

import org.jboss.kernel.spi.dependency.KernelControllerContext;
import org.jboss.kernel.spi.dependency.KernelControllerContextAware;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.test.messaging.tools.container.JBMPropertyKernelConfig;

/**
 * This is class is used in test environments. it will intercept the creation of the configuration and change certain
 * attributes, such as the server id
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ConfigurationHelper implements KernelControllerContextAware
{
   private Configuration configuration;
   
   private KernelControllerContext kernelControllerContext;
   
   private static HashMap<Integer, HashMap<String, Object>> configs;

   public void setKernelControllerContext(KernelControllerContext kernelControllerContext) throws Exception
   {
      this.kernelControllerContext = kernelControllerContext;
   }

   public void unsetKernelControllerContext(KernelControllerContext kernelControllerContext) throws Exception
   {
      this.kernelControllerContext = null;
   }

   public Configuration getConfiguration()
   {
      return configuration;
   }

   public void setConfiguration(Configuration configuration)
   {
      this.configuration = configuration;
   }

   public void start()
   {
      JBMPropertyKernelConfig config = (JBMPropertyKernelConfig) kernelControllerContext.getKernel().getConfig();
      HashMap<String, Object> configuration = configs.get(config.getServerID());
      ConfigurationImpl actualConfiguration = (ConfigurationImpl) kernelControllerContext.getKernel().getRegistry().getEntry("Configuration").getTarget();
      actualConfiguration.setMessagingServerID(config.getServerID());
      actualConfiguration.setPort(actualConfiguration.getPort() + config.getServerID());
      alterConfig(actualConfiguration, configuration);
   }

   public static void addServerConfig(int serverID, HashMap<String, Object> configuration)
   {
      configs = new HashMap<Integer, HashMap<String, Object>>();
      configs.put(serverID, configuration);
   }

   public Hashtable<String, Serializable> getEnvironment()
   {
      Hashtable<String, Serializable> env = new Hashtable<String, Serializable>();
      env.put("java.naming.factory.initial", "org.jboss.test.messaging.tools.container.InVMInitialContextFactory");
      env.put("jboss.messaging.test.server.index", "" + configuration.getMessagingServerID());
      return env;
   }

   private void alterConfig(Configuration origConf, HashMap<String, Object> newConf)
   {
      Set<String> keys = newConf.keySet();
      for (String key : keys)
      {
         try
         {
            Method m = null;

               m = origConf.getClass().getMethod(key, newConf.get(key).getClass());


            m.invoke(configuration, newConf.get(key));
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }
}
