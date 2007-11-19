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
package org.jboss.test.messaging.tools.container;

import java.security.Principal;
import java.security.acl.Group;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.security.auth.Subject;

import org.jboss.logging.Logger;
import org.jboss.security.AnybodyPrincipal;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.NobodyPrincipal;
import org.jboss.security.RealmMapping;
import org.jboss.security.SecurityAssociation;
import org.jboss.security.SimpleGroup;
import org.jboss.security.SimplePrincipal;


/**
 * Mock Security manager for testing JMS security.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * 
 */
public class MockJBossSecurityManager implements AuthenticationManager, RealmMapping
{
   public static final String TEST_SECURITY_DOMAIN = "messaging-securitydomain";
   
   private static final Logger log = Logger.getLogger(MockJBossSecurityManager.class);

   private boolean simulateJBossJaasSecurityManager;
   
   //Authentication Manager Implementation
   
   public String getSecurityDomain()
   {
      return TEST_SECURITY_DOMAIN;
   }

   public boolean isValid(Principal principal, Object credential)
   {
      throw new UnsupportedOperationException();
   }

   public boolean isValid(Principal principal, Object credential, Subject activeSubject)
   {
      if (log.isTraceEnabled()) { log.trace("principal:" + principal + " credential:" + credential); }

      boolean isValid = false;

      String username = principal == null ? null : principal.getName();
      char[] passwordChars = (char[])credential;
      String password = passwordChars == null ? null : new String(passwordChars);

      if (username == null)
      {
         isValid = true;

         if (isValid && simulateJBossJaasSecurityManager)
         {
            // modify the activeSubject, need to add to it its current roles
            // TODO: this is currently impmented in a messy way, this and doesUserHaveRole()
            //       implementation must be coalesced
            addRole(activeSubject, "guest");
         }
      }
      else if ("guest".equals(username))
      {
         isValid = "guest".equals(password);

         if (isValid && simulateJBossJaasSecurityManager)
         {
            // modify the activeSubject, need to add to it its current roles
            // TODO: this is currently impmented in a messy way, this and doesUserHaveRole()
            //       implementation must be coalesced
            addRole(activeSubject, "guest");
         }
      }
      else if ("john".equals(username))
      {
         isValid = "needle".equals(password);

         if (isValid && simulateJBossJaasSecurityManager)
         {
            // modify the activeSubject, need to add to it its current roles
            // TODO: this is currently impmented in a messy way, this and doesUserHaveRole()
            //       implementation must be coalesced
            addRole(activeSubject, "publisher");
            addRole(activeSubject, "durpublisher");
            addRole(activeSubject, "def");
         }
      }
      // We use this user with pre-configured clientIds
      else if ("dilbert".equals(username))
      {
         isValid = "dogbert".equals(password);

         if (isValid && simulateJBossJaasSecurityManager)
         {
            addRole(activeSubject, "publisher");
            addRole(activeSubject, "durpublisher");
            addRole(activeSubject, "def");
         }
      }
      else if ("nobody".equals(username))
      {
         isValid = "nobody".equals(password);

         if (isValid && simulateJBossJaasSecurityManager)
         {
            // modify the activeSubject, need to add to it its current roles
            // TODO: this is currently impmented in a messy way, this and doesUserHaveRole()
            //       implementation must be coalesced
            addRole(activeSubject, "noacc");
         }
      }
      else if ("dynsub".equals(username))
      {
         isValid = "dynsub".equals(password);

         if (isValid && simulateJBossJaasSecurityManager)
         {
            // modify the activeSubject, need to add to it its current roles
            // TODO: this is currently impmented in a messy way, this and doesUserHaveRole()
            //       implementation must be coalesced
            addRole(activeSubject, "publisher");
            addRole(activeSubject, "durpublisher");
         }
      }
      else
      {
         isValid = false;
      }

      return isValid;
   }

   public Subject getActiveSubject()
   {
      throw new UnsupportedOperationException();
   }

   //RealmMapping implementation
   
   public Principal getPrincipal(Principal principal)
   {
      throw new UnsupportedOperationException();
   }

   private boolean containsRole(String roleName, Set roles)
   {
      Iterator iter = roles.iterator();
      while (iter.hasNext())
      {
         Principal p = (Principal)iter.next();
         if (p.getName().equals(roleName))
         {
            return true;
         }
      }
      return false;
   }
   
   public boolean doesUserHaveRole(Principal principal, Set roles)
   {
      // introduced the possiblity to "simulate" JaasSecurityManager behavior, which is ingnoring
      // the principal passed as argument and looking at thread context for active subject; this
      // would allow us to catch some problems earlier at functional testsuite level, and not
      // wait for integration or smoke test. However, the "correct" place for this kind of test
      // is at integration testsuite level.

      if (simulateJBossJaasSecurityManager)
      {
         boolean hasRole = false;
         // check that the caller is authenticated to the current thread
         Subject subject = SecurityAssociation.getSubject();

         if (subject != null)
         {
            // Check the caller's roles
            Group subjectRoles = getSubjectRoles(subject);
            if (subjectRoles != null)
            {
               Iterator iter = roles.iterator();
               while (!hasRole && iter.hasNext())
               {
                  Principal role = (Principal)iter.next();
                  hasRole = doesRoleGroupHaveRole(role, subjectRoles);
               }
            }
         }
         return hasRole;
      }
      else
      {
         // "alternate" MockJBossSecurityManager behavior, we actually look at 'principal' passed as
         // parameter

         String username = principal == null ? "guest" : principal.getName();

         if (log.isTraceEnabled())
         {
            log.trace("doesUserHaveRole:" + username);
         }

         if ("guest".equals(username))
         {
            return containsRole("guest", roles);
         }
         else if ("john".equals(username))
         {
            return containsRole("publisher", roles) ||
               containsRole("durpublisher", roles) ||
               containsRole("def", roles);
         }
         else if ("dynsub".equals(username))
         {
            return containsRole("publisher", roles)||
               containsRole("durpublisher", roles);
         }
         else if ("nobody".equals(username))
         {
            return containsRole("noacc", roles);
         }
         else if ("dilbert".equals(username))
         {
            return containsRole("publisher", roles) ||
               containsRole("durpublisher", roles) ||
               containsRole("def", roles);
         }
         else
         {
            return false;
         }
      }
   }

   public Set getUserRoles(Principal principal)
   {
      throw new UnsupportedOperationException();
   }

   public void setSimulateJBossJaasSecurityManager(boolean b)
   {
      simulateJBossJaasSecurityManager = b;
   }

   public boolean isSimulateJBossJaasSecurityManager()
   {
      return simulateJBossJaasSecurityManager;
   }

   /**
    * Copied from JaasSecurityManager.
    */
   private Group getSubjectRoles(Subject subject)
   {
      Set subjectGroups = subject.getPrincipals(Group.class);
      Iterator iter = subjectGroups.iterator();
      Group roles = null;
      while (iter.hasNext())
      {
         Group grp = (Group)iter.next();
         String name = grp.getName();
         if (name.equals("Roles"))
         {
            roles = grp;
         }
      }
      return roles;
   }

   /**
    * Copied from JaasSecurityManager.
    */
   private boolean doesRoleGroupHaveRole(Principal role, Group userRoles)
   {
      // First check that role is not a NobodyPrincipal
      if (role instanceof NobodyPrincipal)
      {
         return false;
      }

      // Check for inclusion in the user's role set
      boolean isMember = userRoles.isMember(role);
      if (!isMember)
      {
         // Check the AnybodyPrincipal special cases
         isMember = (role instanceof AnybodyPrincipal);
      }

      return isMember;
   }

   private void addRole(Subject subject, String role)
   {
      Set groups = subject.getPrincipals(Group.class);

      if(groups == null || groups.isEmpty())
      {
         Group g = new SimpleGroup("Roles");
         subject.getPrincipals().add(g);
         groups = new HashSet();
         groups.add(g);
      }

      Group roles = null;

      for(Iterator i = groups.iterator(); i.hasNext(); )
      {
         Group g = (Group)i.next();
         if ("Roles".equals(g.getName()))
         {
            roles = g;
         }
      }

      if (roles == null)
      {
         roles =  new SimpleGroup("Roles");
         subject.getPrincipals().add(roles);
      }

      roles.addMember(new SimplePrincipal(role));

   }

}