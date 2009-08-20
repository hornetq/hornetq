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

package org.hornetq.jms.tests.tools;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Set;

import org.hornetq.core.config.Configuration;
import org.jboss.kernel.spi.dependency.KernelControllerContext;
import org.jboss.kernel.spi.dependency.KernelControllerContextAware;

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
   }

   public static void addServerConfig(int serverID, HashMap<String, Object> configuration)
   {
      configs = new HashMap<Integer, HashMap<String, Object>>();
      configs.put(serverID, configuration);
   }

   public Hashtable<String, Serializable> getEnvironment()
   {
      Hashtable<String, Serializable> env = new Hashtable<String, Serializable>();
      env.put("java.naming.factory.initial", "org.hornetq.jms.tests.tools.container.InVMInitialContextFactory");
      env.put("hornetq.test.server.index", "0");
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
