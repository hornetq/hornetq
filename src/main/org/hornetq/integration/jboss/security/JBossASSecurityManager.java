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

package org.hornetq.integration.jboss.security;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.naming.InitialContext;
import javax.security.auth.Subject;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.HornetQSecurityManager;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQComponent;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.RealmMapping;
import org.jboss.security.SimplePrincipal;

/**
 * This implementation delegates to the JBoss AS security interfaces (which in turn use JAAS)
 * It can be used when running HornetQ in JBoss AS
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class JBossASSecurityManager implements HornetQSecurityManager, HornetQComponent
{
   private static final Logger log = Logger.getLogger(JBossASSecurityManager.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   /**
    * the realmmapping
    */
   private RealmMapping realmMapping;

   /**
    * the JAAS Authentication Manager
    */
   private AuthenticationManager authenticationManager;

   /**
    * The JNDI name of the AuthenticationManager(and RealmMapping since they are the same object).
    */
   private String securityDomainName = "java:/jaas/hornetq";
   
   private boolean started;
   
   public boolean validateUser(final String user, final String password)
   {      
      SimplePrincipal principal = new SimplePrincipal(user);

      char[] passwordChars = null;

      if (password != null)
      {
         passwordChars = password.toCharArray();
      }

      Subject subject = new Subject();

      return authenticationManager.isValid(principal, passwordChars, subject);
   }

   public boolean validateUserAndRole(final String user, final String password, final Set<Role> roles, final CheckType checkType)
   {
      SimplePrincipal principal = user == null ? null : new SimplePrincipal(user);

      char[] passwordChars = null;

      if (password != null)
      {
         passwordChars = password.toCharArray();
      }

      Subject subject = new Subject();

      boolean authenticated = authenticationManager.isValid(principal, passwordChars, subject);
      // Authenticate. Successful authentication will place a new SubjectContext on thread local,
      // which will be used in the authorization process. However, we need to make sure we clean up
      // thread local immediately after we used the information, otherwise some other people
      // security my be screwed up, on account of thread local security stack being corrupted.
      if (authenticated)
      {
         SecurityActions.pushSubjectContext(principal, passwordChars, subject, securityDomainName);
         Set<Principal> rolePrincipals = getRolePrincipals(checkType, roles);

         authenticated = realmMapping.doesUserHaveRole(principal, rolePrincipals);

         if (trace)
         {
            log.trace("user " + user + (authenticated ? " is " : " is NOT ") + "authorized");
         }
         SecurityActions.popSubjectContext();
      }
      return authenticated;
   }
   
   public void addRole(String user, String role)
   {
      // NO-OP
   }

   public void addUser(String user, String password)
   {
      // NO-OP
   }

   public void removeRole(String user, String role)
   {
      // NO-OP
   }

   public void removeUser(String user)
   {
      // NO-OP
   }

   public void setDefaultUser(String username)
   {
      // NO-OP
   }

   private Set<Principal> getRolePrincipals(final CheckType checkType, final Set<Role> roles)
   {
      Set<Principal> principals = new HashSet<Principal>();
      for (Role role : roles)
      {
         if (checkType.hasRole(role))
         {
            principals.add(new SimplePrincipal(role.getName()));
         }
      }
      return principals;
   }

   public void setRealmMapping(final RealmMapping realmMapping)
   {
      this.realmMapping = realmMapping;
   }

   public void setAuthenticationManager(final AuthenticationManager authenticationManager)
   {
      this.authenticationManager = authenticationManager;
   }

   /**
    * lifecycle method, needs to be called
    *
    * @throws Exception
    */
   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }
      
      InitialContext ic = new InitialContext();
      authenticationManager = (AuthenticationManager) ic.lookup(securityDomainName);
      realmMapping = (RealmMapping) authenticationManager;
      
      started = true;
   }
   
   public synchronized void stop()
   {
      if (!started)
      {
         return;
      }
      started = false;
   }
   
   public synchronized boolean isStarted()
   {
      return started;
   }
   
   public void setSecurityDomainName(String securityDomainName)
   {
      this.securityDomainName = securityDomainName;
   }
}
