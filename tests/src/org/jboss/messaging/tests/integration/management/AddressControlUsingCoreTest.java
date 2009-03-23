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

import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.management.Operation;
import org.jboss.messaging.core.management.Parameter;
import org.jboss.messaging.utils.SimpleString;

import javax.management.openmbean.TabularData;

/**
 * A JMXQueueControlTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class AddressControlUsingCoreTest extends AddressControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // AddressControlTest overrides --------------------------------

   @Override
   protected AddressControlMBean createManagementControl(final SimpleString address) throws Exception
   {
      return new AddressControlMBean()
      {
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session,
                                                                         ObjectNames.getAddressObjectName(address));

         @Operation(desc = "Add a Role to this address")
         public void addRole(@Parameter(name = "name", desc = "Name of the role to add")String name, @Parameter(name = "send", desc = "Can the user send to an address?")boolean send, @Parameter(name = "consume", desc = "Can the user consume from this address?")boolean consume, @Parameter(name = "createDurableQueue", desc = "Can the user create a durable queue?")boolean createDurableQueue, @Parameter(name = "deleteDurableQueue", desc = "Can the user delete a durable queue?")boolean deleteDurableQueue, @Parameter(name = "createNonDurableQueue", desc = "Can the user create a temp queue?")boolean createNonDurableQueue, @Parameter(name = "deleteNonDurableQueue", desc = "Can the user delete a temp queue?")boolean deleteNonDurableQueue, @Parameter(name = "manage", desc = "Can the user send management messages?")boolean manage) throws Exception
         {
            proxy.invokeOperation("addRole", name, send, consume,  createDurableQueue, deleteDurableQueue, createNonDurableQueue, deleteNonDurableQueue, manage);
         }

         public String getAddress()
         {
            return (String)proxy.retrieveAttributeValue("Address");
         }

         public String[] getQueueNames() throws Exception
         {
            return (String[])proxy.retrieveAttributeValue("QueueNames");
         }

         public TabularData getRoles() throws Exception
         {
            return (TabularData)proxy.retrieveAttributeValue("Roles");
         }

         public void removeRole(String name) throws Exception
         {
            proxy.invokeOperation("removeRole", name);
         }
      };
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
