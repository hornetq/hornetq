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
         @Parameter(name = "create", desc = "Can the user create resource?") boolean create,
         @Parameter(name = "read", desc = "Can the user read from this address?") boolean read,
         @Parameter(name = "write", desc = "Can the user write from this address?") boolean write)
         throws Exception;

   @Operation(desc = "Remove a Role from this address")
   void removeRole(
         @Parameter(name = "name", desc = "Name of the role to remove") String name)
         throws Exception;
}
