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

   boolean isPaging() throws Exception;

   /**
    * Returns the number of bytes used by each page for this address.
    */
   long getNumberOfBytesPerPage() throws Exception;

   /**
    * Returns the names of all bindings (both queues and diverts) bound to this address
    */
   String[] getBindingNames() throws Exception;

   // Operations ----------------------------------------------------

}
