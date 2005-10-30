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
package org.jboss.test.messaging.tools.jmx;

import java.security.Principal;
import java.util.Iterator;
import java.util.Set;

import javax.security.auth.Subject;

import org.jboss.logging.Logger;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.RealmMapping;

/* Mock Security manager for testing JMS security.
 * 
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * 
 */
public class MockJBossSecurityManager implements AuthenticationManager, RealmMapping
{
   public static final String TEST_SECURITY_DOMAIN = "messaging-securitydomain";
   
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