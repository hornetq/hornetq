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

package org.hornetq.jms.tests.tools.container;

import java.security.Principal;
import java.security.acl.Group;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import javax.security.auth.message.MessageInfo;

import org.hornetq.core.logging.Logger;
import org.hornetq.utils.JNDIUtil;
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
   public static final String TEST_SECURITY_DOMAIN = "java:/jaas/messaging";

   private static final Logger log = Logger.getLogger(MockJBossSecurityManager.class);

   private boolean simulateJBossJaasSecurityManager;

   // Authentication Manager Implementation

   public String getSecurityDomain()
   {
      return MockJBossSecurityManager.TEST_SECURITY_DOMAIN;
   }

   public boolean isValid(final Principal principal, final Object credential)
   {
      throw new UnsupportedOperationException();
   }

   public boolean isValid(final Principal principal, final Object credential, final Subject activeSubject)
   {
      if (MockJBossSecurityManager.log.isTraceEnabled())
      {
         MockJBossSecurityManager.log.trace("principal:" + principal + " credential:" + credential);
      }

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
            // implementation must be coalesced
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
            // implementation must be coalesced
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
            // implementation must be coalesced
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
            // implementation must be coalesced
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
            // implementation must be coalesced
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

   // RealmMapping implementation

   public Principal getPrincipal(final Principal principal)
   {
      throw new UnsupportedOperationException();
   }

   private boolean containsRole(final String roleName, final Set roles)
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

   public boolean doesUserHaveRole(final Principal principal, final Set roles)
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

         if (MockJBossSecurityManager.log.isTraceEnabled())
         {
            MockJBossSecurityManager.log.trace("doesUserHaveRole:" + username);
         }

         if ("guest".equals(username))
         {
            return containsRole("guest", roles);
         }
         else if ("john".equals(username))
         {
            return containsRole("publisher", roles) || containsRole("durpublisher", roles) ||
                   containsRole("def", roles);
         }
         else if ("dynsub".equals(username))
         {
            return containsRole("publisher", roles) || containsRole("durpublisher", roles);
         }
         else if ("nobody".equals(username))
         {
            return containsRole("noacc", roles);
         }
         else if ("dilbert".equals(username))
         {
            return containsRole("publisher", roles) || containsRole("durpublisher", roles) ||
                   containsRole("def", roles);
         }
         else
         {
            return false;
         }
      }
   }

   public Set getUserRoles(final Principal principal)
   {
      throw new UnsupportedOperationException();
   }

   public boolean isValid(final MessageInfo messageInfo, final Subject subject, final String string)
   {
      return false; // To change body of implemented methods use File | Settings | File Templates.
   }

   public Principal getTargetPrincipal(final Principal principal, final Map<String, Object> map)
   {
      return null; // To change body of implemented methods use File | Settings | File Templates.
   }

   public void setSimulateJBossJaasSecurityManager(final boolean b)
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
   private Group getSubjectRoles(final Subject subject)
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
   private boolean doesRoleGroupHaveRole(final Principal role, final Group userRoles)
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
         isMember = role instanceof AnybodyPrincipal;
      }

      return isMember;
   }

   private void addRole(final Subject subject, final String role)
   {
      Set groups = subject.getPrincipals(Group.class);

      if (groups == null || groups.isEmpty())
      {
         Group g = new SimpleGroup("Roles");
         subject.getPrincipals().add(g);
         groups = new HashSet();
         groups.add(g);
      }

      Group roles = null;

      for (Iterator i = groups.iterator(); i.hasNext();)
      {
         Group g = (Group)i.next();
         if ("Roles".equals(g.getName()))
         {
            roles = g;
         }
      }

      if (roles == null)
      {
         roles = new SimpleGroup("Roles");
         subject.getPrincipals().add(roles);
      }

      roles.addMember(new SimplePrincipal(role));

   }

   public void start() throws Exception
   {
      bindToJndi("java:/jaas/messaging", this);
   }

   private boolean bindToJndi(final String jndiName, final Object objectToBind) throws NamingException
   {
      InitialContext initialContext = new InitialContext();
      String parentContext;
      String jndiNameInContext;
      int sepIndex = jndiName.lastIndexOf('/');
      if (sepIndex == -1)
      {
         parentContext = "";
      }
      else
      {
         parentContext = jndiName.substring(0, sepIndex);
      }
      jndiNameInContext = jndiName.substring(sepIndex + 1);
      try
      {
         initialContext.lookup(jndiName);

         MockJBossSecurityManager.log.warn("Binding for " + jndiName + " already exists");
         return false;
      }
      catch (Throwable e)
      {
         // OK
      }

      Context c = JNDIUtil.createContext(initialContext, parentContext);

      c.rebind(jndiNameInContext, objectToBind);
      return true;
   }
}