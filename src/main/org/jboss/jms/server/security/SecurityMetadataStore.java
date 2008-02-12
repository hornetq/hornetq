/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.server.security;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import org.jboss.jms.server.SecurityStore;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.util.HierarchicalRepository;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.RealmMapping;
import org.jboss.security.SimplePrincipal;

/**
 * A security metadate store for JMS. Stores security information for destinations and delegates
 * authentication and authorization to a JaasSecurityManager.
 *
 * @author Peter Antman
 * @author <a href="mailto:Scott.Stark@jboss.org">Scott Stark</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @version $Revision$
 *
 * $Id$
 */
public class SecurityMetadataStore implements SecurityStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SecurityMetadataStore.class);

   public static final String DEFAULT_SUCKER_USER_PASSWORD = "CHANGE ME!!";

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   HierarchicalRepository<HashSet<Role>> securityRepository;

   private AuthenticationManager authenticationManager;
   private RealmMapping realmMapping;


   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // SecurityManager implementation --------------------------------


   public Subject authenticate(String user, String password) throws MessagingException
   {
      if (trace) { log.trace("authenticating user " + user); }

      SimplePrincipal principal = new SimplePrincipal(user);
      char[] passwordChars = null;
      if (password != null)
      {
         passwordChars = password.toCharArray();
      }

      Subject subject = new Subject();

      boolean authenticated = authenticationManager.isValid(principal, passwordChars, subject);

      if (authenticated)
      {
         // Warning! This "taints" thread local. Make sure you pop it off the stack as soon as
         //          you're done with it.
         SecurityActions.pushSubjectContext(principal, passwordChars, subject);
         return subject;
      }
      else
      {
         throw new MessagingException(MessagingException.SECURITY_EXCEPTION, "User " + user + " is NOT authenticated");
      }
   }

   public boolean authorize(String user, String destination, CheckType checkType)
   {
      if (trace) { log.trace("authorizing user " + user + " for destination " + destination); }

      HashSet<Role> roles = securityRepository.getMatch(destination);

      Principal principal = user == null ? null : new SimplePrincipal(user);
      Set rolePrincipals = getRolePrincipals(checkType, roles);
      boolean hasRole = realmMapping.doesUserHaveRole(principal, rolePrincipals);

      if (trace) { log.trace("user " + user + (hasRole ? " is " : " is NOT ") + "authorized"); }

      return hasRole;
   }

   private Set getRolePrincipals(CheckType checkType, HashSet<Role> roles)
   {
      Set<SimplePrincipal> principals = new HashSet<SimplePrincipal>();
      for (Role role : roles)
      {
         if((checkType.equals(CheckType.CREATE) && role.isCreate()) ||
                 (checkType.equals(CheckType.WRITE) && role.isWrite()) ||
                 (checkType.equals(CheckType.READ) && role.isRead()))
         {
            principals.add(new SimplePrincipal(role.getName()));
         }
      }
      return principals;
   }

   // Public --------------------------------------------------------


   public void setSecurityRepository(HierarchicalRepository<HashSet<Role>> securityRepository)
   {
      this.securityRepository = securityRepository;
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------


   // Inner class ---------------------------------------------------      

   public void setAuthenticationManager(AuthenticationManager authenticationManager)
   {
      this.authenticationManager = authenticationManager;
      this.realmMapping = (RealmMapping) authenticationManager;
   }
}
