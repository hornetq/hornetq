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

package org.jboss.messaging.core.security.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.JBMUpdateableSecurityManager;
import org.jboss.messaging.core.security.Role;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A basic implementation of the JBMUpdateableSecurityManager. This can be used within an appserver and be deployed by
 * BasicSecurityDeployer or used standalone or embedded.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JBMSecurityManagerImpl implements JBMUpdateableSecurityManager
{
   private static final Logger log = Logger.getLogger(JBMSecurityManagerImpl.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   /**
    * the current valid users
    */
   private Map<String, User> users = new HashMap<String, User>();

   private String defaultUser = null;

   /**
    * the roles for the users
    */
   private Map<String, List<String>> roles = new HashMap<String, List<String>>();


   public boolean validateUser(final String user, final String password)
   {
      if(user == null && defaultUser == null)
      {
         return false;
      }
      User theUser = users.get(user == null ? defaultUser : user);
      return theUser != null && theUser.isValid(user == null ? defaultUser : user, password == null ? defaultUser : password);

   }

   public boolean validateUserAndRole(final String user, final String password, final Set<Role> roles, final CheckType checkType)
   {
      if (validateUser(user, password))
      {
         List<String> availableRoles = this.roles.get(user == null ? defaultUser : user);
         if(availableRoles == null)
         {
            return false;
         }
         for (String availableRole : availableRoles)
         {
            if (roles != null)
            {
               for (Role role : roles)
               {
                  if (role.getName().equals(availableRole) && checkType.hasRole(role))
                  {
                     return true;
                  }
               }
            }
         }
      }
      return false;
   }

   public void addUser(final String user, final String password)
   {
      if (user == null)
      {
         throw new IllegalArgumentException("User cannot be null");
      }
      if (password == null)
      {
         throw new IllegalArgumentException("password cannot be null");
      }
      users.put(user, new User(user, password));
   }

   public void removeUser(final String user)
   {
      users.remove(user);
      roles.remove(user);
   }

   public void addRole(final String user, final String role)
   {
      if (roles.get(user) == null)
      {
         roles.put(user, new ArrayList<String>());
      }
      roles.get(user).add(role);
   }

   public void removeRole(final String user, final String role)
   {
      if (roles.get(user) == null)
      {
         return;
      }
      roles.get(user).remove(role);
   }

   /*
   * set the default user for null users
   */
   public void setDefaultUser(String username)
   {
      defaultUser = username;
   }

   static class User
   {
      final String user;
      final String password;

      User(final String user, final String password)
      {
         this.user = user;
         this.password = password;
      }

      public boolean equals(Object o)
      {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         User user1 = (User) o;

         if (!user.equals(user1.user)) return false;

         return true;
      }

      public int hashCode()
      {
         return user.hashCode();
      }

      public boolean isValid(final String user, final String password)
      {
         if (user == null)
         {
            return false;
         }
         return this.user.equals(user) && this.password.equals(password);
      }
   }
}
