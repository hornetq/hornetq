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

/**
 * extends JBMSecurityManager to allow the addition and removal of users and roles.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JBMUpdateableSecurityManager extends JBMSecurityManager
{
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
}
