/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.jmx;

import java.security.Principal;
import java.util.Iterator;
import java.util.Set;

import javax.security.auth.Subject;

import org.jboss.logging.Logger;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.RealmMapping;
import org.jboss.security.SecurityAssociation;

/* Mock Security manager for testing JMS security.
 * 
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * 
 */
public class MockJBossSecurityManager implements AuthenticationManager, RealmMapping
{
   public static final String TEST_SECURITY_DOMAIN = "jbossmessaging-securitydomain";
   
   private static final Logger log = Logger.getLogger(MockJBossSecurityManager.class);
   
   //Authentication Manager Implementation
   
   public String getSecurityDomain()
   {
      return TEST_SECURITY_DOMAIN;
   }

   public boolean isValid(Principal principal, Object credential)
   {
      throw new UnsupportedOperationException();
   }

   public boolean isValid(Principal principal, Object credential,
      Subject activeSubject)
   {
      if (log.isTraceEnabled()) { log.trace("principal:" + principal + " credential:" + credential); }
      
      String username = principal == null ? null : principal.getName();
      char[] passwordChars = (char[])credential;
      String password = passwordChars == null ? null : new String(passwordChars);
      
      if (username == null)
      {
         return true;
      }      
      else if ("guest".equals(username))
      {
         return "guest".equals(password);
      }
      else if ("john".equals(username))
      {
         return "needle".equals(password);
      }
      else if ("nobody".equals(username))
      {
         return "nobody".equals(password);
      }
      else if ("dynsub".equals(username))
      {
         return "dynsub".equals(password);
      }
      else
      {
         return false;
      }
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
      else
      {
         return false;
      }
      
   }

  
   public Set getUserRoles(Principal principal)
   {
      throw new UnsupportedOperationException();
   }

}