/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.security;

import java.util.Set;

import org.jboss.messaging.core.server.MessagingComponent;

/**
 * USe to validate whether a user has is valid to connect to the server and perform certain functions
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JBMSecurityManager extends MessagingComponent
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
