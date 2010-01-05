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

import java.util.Set;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ConfigurationImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.security.Role;
import org.hornetq.core.security.impl.HornetQSecurityManagerImpl;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.HierarchicalRepository;

/**
 * A SecurityManagementTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class SecurityManagementWithConfiguredAdminUserTest extends SecurityManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String validAdminUser = "validAdminUser";

   private final String validAdminPassword = "validAdminPassword";

   private final String invalidAdminUser = "invalidAdminUser";

   private final String invalidAdminPassword = "invalidAdminPassword";

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    *  default CLUSTER_ADMIN_USER must work even when there are other
    *  configured admin users
    */
   public void testSendManagementMessageWithClusterAdminUser() throws Exception
   {
      doSendManagementMessage(ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_USER,
                              ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_PASSWORD,
                              true);
   }

   public void testSendManagementMessageWithAdminRole() throws Exception
   {
      doSendManagementMessage(validAdminUser, validAdminPassword, true);
   }

   public void testSendManagementMessageWithoutAdminRole() throws Exception
   {
      doSendManagementMessage(invalidAdminUser, invalidAdminPassword, false);
   }

   public void testSendManagementMessageWithoutUserCredentials() throws Exception
   {
      doSendManagementMessage(null, null, false);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected HornetQServer setupAndStartHornetQServer() throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      HornetQServer server = HornetQ.newHornetQServer(conf, false);
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      HornetQSecurityManagerImpl securityManager = (HornetQSecurityManagerImpl)server.getSecurityManager();
      securityManager.addUser(validAdminUser, validAdminPassword);
      securityManager.addUser(invalidAdminUser, invalidAdminPassword);

      securityManager.addRole(validAdminUser, "admin");
      securityManager.addRole(validAdminUser, "guest");
      securityManager.addRole(invalidAdminUser, "guest");

      Set<Role> adminRole = securityRepository.getMatch(ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS.toString());
      adminRole.add(new Role("admin", true, true, true, true, true, true, true));
      securityRepository.addMatch(ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS.toString(), adminRole);
      Set<Role> guestRole = securityRepository.getMatch("*");
      guestRole.add(new Role("guest", true, true, true, true, true, true, false));
      securityRepository.addMatch("*", guestRole);

      return server;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
