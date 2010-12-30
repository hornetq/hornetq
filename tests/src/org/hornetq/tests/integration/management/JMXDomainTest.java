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

package org.hornetq.tests.integration.management;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ObjectNameBuilder;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;

/**
 * A JMXDomainTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class JMXDomainTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void test2HornetQServersManagedFrom1MBeanServer() throws Exception
   {

      Configuration config_0 = createDefaultConfig();
      config_0.setJMXManagementEnabled(true);

      String jmxDomain_1 = ConfigurationImpl.DEFAULT_JMX_DOMAIN + ".1";

      Configuration config_1 = createBasicConfig();
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      config_1.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName(), params));
      config_1.setJMXDomain(jmxDomain_1);
      config_1.setJMXManagementEnabled(true);

      HornetQServer server_0 = HornetQServers.newHornetQServer(config_0, mbeanServer, false);
      HornetQServer server_1 = HornetQServers.newHornetQServer(config_1, mbeanServer, false);

      ObjectNameBuilder builder_0 = ObjectNameBuilder.DEFAULT;
      ObjectNameBuilder builder_1 = ObjectNameBuilder.create(jmxDomain_1);

      checkNoResource(builder_0.getHornetQServerObjectName());
      checkNoResource(builder_1.getHornetQServerObjectName());

      server_0.start();

      checkResource(builder_0.getHornetQServerObjectName());
      checkNoResource(builder_1.getHornetQServerObjectName());

      server_1.start();

      checkResource(builder_0.getHornetQServerObjectName());
      checkResource(builder_1.getHornetQServerObjectName());

      server_0.stop();

      checkNoResource(builder_0.getHornetQServerObjectName());
      checkResource(builder_1.getHornetQServerObjectName());

      server_1.stop();

      checkNoResource(builder_0.getHornetQServerObjectName());
      checkNoResource(builder_1.getHornetQServerObjectName());

   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
