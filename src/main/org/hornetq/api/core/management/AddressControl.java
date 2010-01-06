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

package org.hornetq.api.core.management;


/**
 * An AddressControl is used to manage an address.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public interface AddressControl
{
   // Attributes ----------------------------------------------------

   /**
    * Returns the managed address.
    */
   String getAddress();

   /**
    * Returns the roles (name and permissions) associated to this address.
    */
   Object[] getRoles() throws Exception;

   /**
    * Returns the roles  (name and permissions) associated to this address
    * using JSON serialization.
    * <br>
    * Java objects can be recreated from JSON serialization using {@link RoleInfo#from(String)}.
    */
   String getRolesAsJSON() throws Exception;

   /**
    * Returns the names of the queues bound to this address.
    */
   String[] getQueueNames() throws Exception;

   /**
    * Returns the number of pages used by this address.
    */
   int getNumberOfPages() throws Exception;

   /**
    * Returns the number of bytes used by each page for this address.
    */
   long getNumberOfBytesPerPage() throws Exception;

   // Operations ----------------------------------------------------

   /**
    * Adds a role to this address.
    * 
    * @param name name of the role
    * @param send can the user send to this address?
    * @param consume can the user consume from a queue bound to this address?
    * @param createDurableQueue can the user create a durable queue bound to this address?
    * @param deleteDurableQueue can the user delete a durable queue bound to this address?
    * @param createNonDurableQueue can the user create a non-durable queue bound to this address?
    * @param deleteNonDurableQueue can the user delete a non-durable queue bound to this address?
    * @param manage can the user send management messages to this address?
    * @throws Exception if an exception occurred while adding the role
    */
   @Operation(desc = "Add a Role to this address")
   void addRole(@Parameter(name = "name", desc = "Name of the role to add") String name,
                @Parameter(name = "send", desc = "Can the user send to this address?") boolean send,
                @Parameter(name = "consume", desc = "Can the user consume from this address?") boolean consume,
                @Parameter(name = "createDurableQueue", desc = "Can the user create a durable queue?") boolean createDurableQueue,
                @Parameter(name = "deleteDurableQueue", desc = "Can the user delete a durable queue?") boolean deleteDurableQueue,
                @Parameter(name = "createNonDurableQueue", desc = "Can the user create a temp queue?") boolean createNonDurableQueue,
                @Parameter(name = "deleteNonDurableQueue", desc = "Can the user delete a temp queue?") boolean deleteNonDurableQueue,
                @Parameter(name = "manage", desc = "Can the user send management messages?") boolean manage) throws Exception;

   /**
    * Removes the role corresponding to the specified name from this address.
    * 
    * @throws Exception if an exception occurred while removing the role
    */
   @Operation(desc = "Remove a Role from this address")
   void removeRole(@Parameter(name = "name", desc = "Name of the role to remove") String name) throws Exception;
}
