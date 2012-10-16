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

package org.hornetq.spi.core.security;

import java.util.Set;

import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQComponent;

/**
 * Use to validate whether a user has is valid to connect to the server and perform certain
 * functions
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface HornetQSecurityManager extends HornetQComponent
{
   /**
    * is this a valid user.
    * @param user the user
    * @param password the users password
    * @return true if a valid user
    */
   boolean validateUser(String user, String password);

   /**
    * is this a valid user and do they have the correct role
    *
    * @param user the user
    * @param password the users password
    * @param roles the roles the user has
    * @param checkType the type of check to perform
    * @return true if the user is valid and they have the correct roles
    */
   boolean validateUserAndRole(String user, String password, Set<Role> roles, CheckType checkType);

   /**
    * adds a new user
    * @param user the user to add
    * @param password theusers password
    */
   void addUser(String user, String password);

   /**
    * removes a user and any roles they may have.
    * @param user the user to remove
    */
   void removeUser(String user);

   /**
    * adds a new role for a user.
    * @param user the user
    * @param role the role to add
    */
   void addRole(String user, String role);

   /**
    * removes a role from a user
    * @param user the user
    * @param role the role to remove
    */
   void removeRole(String user, String role);

   /*
   * set the default user for null users
   */
   void setDefaultUser(String username);
}
