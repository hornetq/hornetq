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

package org.jboss.messaging.integration.security;

import static org.jboss.messaging.core.security.CheckType.CREATE;
import static org.jboss.messaging.core.security.CheckType.READ;
import static org.jboss.messaging.core.security.CheckType.WRITE;

import java.security.Principal;
import java.security.acl.Group;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessagingComponent;

/**
 * This implementation delegates to the JAAS security interfaces.
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="on@ibis.odessa.ua">Oleg Nitz</a>
 * @author Scott.Stark@jboss.org
 */
public class JAASSecurityManager implements JBMSecurityManager, MessagingComponent
{
   private static final Logger log = Logger.getLogger(JAASSecurityManager.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private String configurationName;

   private boolean started;

   private CallbackHandler callbackHandler;

   private Configuration config;

   // JBMSecurityManager implementation -----------------------------

   public boolean validateUser(final String user, final String password)
   {
      try
      {
         getAuthenticatedSubject(user, password);
         return true;
      }
      catch (LoginException e1)
      {
         return false;
      }
   }

   public boolean validateUserAndRole(final String user,
                                      final String password,
                                      final Set<Role> roles,
                                      final CheckType checkType)
   {
      Subject localSubject = null;
      try
      {
         localSubject = getAuthenticatedSubject(user, password);
      }
      catch (LoginException e1)
      {
         return false;
      }

      boolean authenticated = true;

      if (localSubject != null)
      {
         Set<Principal> rolePrincipals = getRolePrincipals(checkType, roles);

         // authenticated = realmMapping.doesUserHaveRole(principal, rolePrincipals);

         boolean hasRole = false;
         // check that the caller is authenticated to the current thread

         // Check the caller's roles
         Group subjectRoles = getSubjectRoles(localSubject);
         if (subjectRoles != null)
         {
            Iterator<Principal> iter = rolePrincipals.iterator();
            while (!hasRole && iter.hasNext())
            {
               Principal role = iter.next();
               hasRole = subjectRoles.isMember(role);
            }
         }

         authenticated = hasRole;

         if (trace)
         {
            log.trace("user " + user + (authenticated ? " is " : " is NOT ") + "authorized");
         }
      }
      return authenticated;
   }

   // MessagingComponent implementation -----------------------------

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

   private Subject getAuthenticatedSubject(final String user, final String password) throws LoginException
   {
      SimplePrincipal principal = user == null ? null : new SimplePrincipal(user);

      char[] passwordChars = null;

      if (password != null)
      {
         passwordChars = password.toCharArray();
      }

      Subject subject = new Subject();

      subject.getPrincipals().add(principal);
      subject.getPrivateCredentials().add(passwordChars);

      LoginContext lc = new LoginContext(configurationName, subject, callbackHandler, config);
      lc.login();
      return lc.getSubject();
   }

   private Group getSubjectRoles(Subject subject)
   {
      Set<Group> subjectGroups = subject.getPrincipals(Group.class);
      Iterator<Group> iter = subjectGroups.iterator();
      Group roles = null;
      while (iter.hasNext())
      {
         Group grp = iter.next();
         String name = grp.getName();
         if (name.equals("Roles"))
         {
            roles = grp;
         }
      }
      return roles;
   }

   private Set<Principal> getRolePrincipals(final CheckType checkType, final Set<Role> roles)
   {
      Set<Principal> principals = new HashSet<Principal>();
      for (Role role : roles)
      {
         if ((checkType.equals(CREATE) && role.isCheckType(CREATE))
          || (checkType.equals(WRITE) && role.isCheckType(WRITE))
          || (checkType.equals(READ) && role.isCheckType(READ)))
         {
            principals.add(new SimplePrincipal(role.getName()));
         }
      }
      return principals;
   }

   // Public --------------------------------------------------------

   public void setConfigurationName(String configurationName)
   {
      this.configurationName = configurationName;
   }

   public void setCallbackHandler(CallbackHandler handler)
   {
      this.callbackHandler = handler;
   }

   public void setConfiguration(Configuration config)
   {
      this.config = config;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   public static class SimplePrincipal implements Principal, java.io.Serializable
   {
      private static final long serialVersionUID = 1L;

      private String name;

      public SimplePrincipal(String name)
      {
         this.name = name;
      }

      /** Compare this SimplePrincipal's name against another Principal
      @return true if name equals another.getName();
       */
      public boolean equals(Object another)
      {
         if (!(another instanceof Principal))
            return false;
         String anotherName = ((Principal)another).getName();
         boolean equals = false;
         if (name == null)
            equals = anotherName == null;
         else
            equals = name.equals(anotherName);
         return equals;
      }

      public int hashCode()
      {
         return (name == null ? 0 : name.hashCode());
      }

      public String toString()
      {
         return name;
      }

      public String getName()
      {
         return name;
      }
   }

}
