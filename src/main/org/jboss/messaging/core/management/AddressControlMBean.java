/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.management;

import javax.management.openmbean.TabularData;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface AddressControlMBean
{
   // Attributes ----------------------------------------------------

   String getAddress();

   TabularData getRoles() throws Exception;
   
   String[] getQueueNames() throws Exception;

   // Operations ----------------------------------------------------

   @Operation(desc = "Add a Role to this address")
   void addRole(
         @Parameter(name = "name", desc = "Name of the role to add") String name,
         @Parameter(name = "send", desc = "Can the user send to an address?") boolean send,
         @Parameter(name = "consume", desc = "Can the user consume from this address?") boolean consume,
         @Parameter(name = "createDurableQueue", desc = "Can the user create a durable queue?") boolean createDurableQueue,
         @Parameter(name = "deleteDurableQueue", desc = "Can the user delete a durable queue?") boolean deleteDurableQueue,
         @Parameter(name = "createNonDurableQueue", desc = "Can the user create a temp queue?") boolean createNonDurableQueue,
         @Parameter(name = "deleteNonDurableQueue", desc = "Can the user delete a temp queue?") boolean deleteNonDurableQueue,
         @Parameter(name = "manage", desc = "Can the user send management messages?") boolean manage)
         throws Exception;

   @Operation(desc = "Remove a Role from this address")
   void removeRole(
         @Parameter(name = "name", desc = "Name of the role to remove") String name)
         throws Exception;
}
