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

package org.jboss.jms.example;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.jboss.messaging.core.security.impl.JAASSecurityManager;

/**
 * A ExampleLoginModule
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ExampleLoginModule implements LoginModule
{

   private Map<String, ?> options;

   private Subject subject;

   public ExampleLoginModule()
   {
   }

   public boolean abort() throws LoginException
   {
      return true;
   }

   public boolean commit() throws LoginException
   {
      return true;
   }

   public void initialize(Subject subject,
                          CallbackHandler callbackHandler,
                          Map<String, ?> sharedState,
                          Map<String, ?> options)
   {
      this.subject = subject;
      // the credentials are passed directly to the
      // login module through the options user, pass, role
      this.options = options;
   }

   public boolean login() throws LoginException
   {
      Iterator<char[]> iterator = subject.getPrivateCredentials(char[].class).iterator();
      char[] passwordChars = iterator.next();
      String password = new String(passwordChars);
      Iterator<Principal> iterator2 = subject.getPrincipals().iterator();
      String user = iterator2.next().getName();

      boolean authenticated = user.equals(options.get("user")) && password.equals(options.get("pass"));

      if (authenticated)
      {
         Group roles = new SimpleGroup("Roles");
         roles.addMember(new JAASSecurityManager.SimplePrincipal((String)options.get("role")));
         subject.getPrincipals().add(roles);
      }
      System.out.format("JAAS authentication >>> user=%s, password=%s\n", user, password);
      return authenticated;

   }

   public Subject getSubject()
   {
      return subject;
   }

   public boolean logout() throws LoginException
   {
      return true;
   }

   public class SimpleGroup implements Group
   {
      private final String name;

      private Set<Principal> members = new HashSet<Principal>();

      public SimpleGroup(String name)
      {
         this.name = name;
      }

      public boolean addMember(Principal principal)
      {
         return members.add(principal);
      }

      public boolean isMember(Principal principal)
      {
         return members.contains(principal);
      }

      public Enumeration<? extends Principal> members()
      {
         return Collections.enumeration(members);
      }

      public boolean removeMember(Principal principal)
      {
         return members.remove(principal);
      }

      public String getName()
      {
         return name;
      }
   }

}
