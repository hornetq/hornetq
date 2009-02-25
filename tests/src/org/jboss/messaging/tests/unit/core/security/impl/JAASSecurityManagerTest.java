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

package org.jboss.messaging.tests.unit.core.security.impl;

import java.io.IOException;
import java.security.Principal;
import java.security.acl.Group;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.spi.LoginModule;

import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.integration.security.JAASSecurityManager;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.security.SimpleGroup;

/**
 * tests the JAASSecurityManager
 *
 *@author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JAASSecurityManagerTest extends UnitTestCase
{
   private JAASSecurityManager securityManager;

   private static final String USER = "user";
   private static final String PASSWORD = "password";
   private static final String INVALID_PASSWORD = "invalidPassword";   
   private static final String ROLE = "role";
   private static final String INVALID_ROLE = "invalidRole";
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      securityManager = new JAASSecurityManager();
      
      final String domainName = SimpleLogingModule.class.getName();
      // pass the correct user/pass and a role as options to the login module
      final Map<String, String> options = new HashMap<String, String>();
      options.put("user", USER);
      options.put("pass", PASSWORD);
      options.put("role", ROLE);

      securityManager.setConfigurationName(domainName);
      securityManager.setCallbackHandler(new CallbackHandler()
      {
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
         {
            // empty callback, auth info are directly passed as options to the login module
         }
      });
      securityManager.setConfiguration(new SimpleConfiguration(domainName, options));
      
   }

   protected void tearDown() throws Exception
   {
      securityManager = null;
      
      super.tearDown();
   }

   public void testValidatingUser()
   {
      assertTrue(securityManager.validateUser(USER, PASSWORD));
      assertFalse(securityManager.validateUser(USER, INVALID_PASSWORD));
   }

   public void testValidatingUserAndRole()
   {
      Set<Role> roles = new HashSet<Role>();
      roles.add(new Role(ROLE, true, true, true));

      assertTrue(securityManager.validateUserAndRole(USER, PASSWORD, roles, CheckType.CREATE));

      roles.clear();
      roles.add(new Role(INVALID_ROLE, true, true, true));
      assertFalse(securityManager.validateUserAndRole(USER, PASSWORD, roles, CheckType.CREATE));
   }

   public static class SimpleLogingModule implements LoginModule
   {

      private Map<String, ?> options;

      private Subject subject;

      public SimpleLogingModule()
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
   }

   public static class SimpleConfiguration extends Configuration
   {
      private Map<String, ?> options;
      private String loginModuleName;

      public SimpleConfiguration(String loginModuleName, Map<String, ?> options)
      {
         this.loginModuleName = loginModuleName;
         this.options = options;
      }

      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name)
      {
         AppConfigurationEntry entry = new AppConfigurationEntry(loginModuleName,
                                                                 LoginModuleControlFlag.REQUIRED,
                                                                 options);
         return new AppConfigurationEntry[] { entry };
      }

      @Override
      public void refresh()
      {
      }
   }
}
