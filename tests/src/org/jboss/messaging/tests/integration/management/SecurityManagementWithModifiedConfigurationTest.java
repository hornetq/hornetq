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

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.security.impl.SecurityStoreImpl;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;

/**
 * A SecurityManagementTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class SecurityManagementWithModifiedConfigurationTest extends SecurityManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String configuredClusterPassword = "this is not the default password";
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSendManagementMessageWithModifiedClusterAdminUser() throws Exception
   {
      doSendManagementMessage(SecurityStoreImpl.CLUSTER_ADMIN_USER, 
                              configuredClusterPassword, true);
   }

   public void testSendManagementMessageWithDefaultClusterAdminUser() throws Exception
   {
      doSendManagementMessage(SecurityStoreImpl.CLUSTER_ADMIN_USER, 
                              ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_PASSWORD, false);
   }

   public void testSendManagementMessageWithGuest() throws Exception
   {
      doSendManagementMessage("guest", "guest", false);
   }

   public void testSendManagementMessageWithoutUserCredentials() throws Exception
   {
      doSendManagementMessage(null, null, false);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected MessagingServer setupAndStartMessagingServer() throws Exception
   {
      ConfigurationImpl conf = new ConfigurationImpl();
      conf.setSecurityEnabled(true);
      conf.setManagementClusterPassword(configuredClusterPassword);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      MessagingServer server = Messaging.newNullStorageMessagingServer(conf);
      server.start();
      
      return server;
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
