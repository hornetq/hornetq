/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.management;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.security.impl.JBMSecurityManagerImpl;
import org.jboss.messaging.core.security.impl.SecurityStoreImpl;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.settings.HierarchicalRepository;

import java.util.HashSet;
import java.util.Set;

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
      doSendManagementMessage(SecurityStoreImpl.CLUSTER_ADMIN_USER, 
                              ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_PASSWORD, true);
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

   protected MessagingServer setupAndStartMessagingServer() throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      MessagingServer server = Messaging.newNullStorageMessagingServer(conf);
      server.start();
      HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
      JBMSecurityManagerImpl securityManager = (JBMSecurityManagerImpl)server.getSecurityManager();
      securityManager.addUser(validAdminUser, validAdminPassword);
      securityManager.addUser(invalidAdminUser, invalidAdminPassword);
      securityManager.addRole(validAdminUser, "admin");
      securityManager.addRole(validAdminUser, "guest");
      securityManager.addRole(invalidAdminUser, "guest");

      Set<Role> adminRole = new HashSet<Role>();
      adminRole.add(new Role("admin", true, true, false, true, true, true, true));
      securityRepository.addMatch(DEFAULT_MANAGEMENT_ADDRESS.toString(), adminRole);
      Set<Role> guestRole = new HashSet<Role>();
      guestRole.add(new Role("guest", true, true, true, true, true, true, true));
      securityRepository.addMatch("*", guestRole);
      
      return server;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
