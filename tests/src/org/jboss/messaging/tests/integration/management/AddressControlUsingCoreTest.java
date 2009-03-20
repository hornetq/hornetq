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

import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.utils.SimpleString;

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

         public void addRole(String name, boolean create, boolean read, boolean write) throws Exception
         {
            proxy.invokeOperation("addRole", name, create, read, write);
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
