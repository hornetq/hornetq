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
package org.jboss.messaging.core.security;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

import org.jboss.messaging.core.MessagingException;
import org.jboss.messaging.core.ServerConnection;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.HierarchicalRepository;
import org.jboss.messaging.util.Logger;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.RealmMapping;
import org.jboss.security.SimplePrincipal;

/**
 * The JBM SecurityStore implementation
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * 
 * Parts based on old version by:
 * 
 * @author Peter Antman
 * @author <a href="mailto:Scott.Stark@jboss.org">Scott Stark</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * 
 * @version $Revision$
 *
 * $Id$
 */
public class SecurityStoreImpl implements SecurityStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SecurityStoreImpl.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   private HierarchicalRepository<HashSet<Role>> securityRepository;

   private AuthenticationManager authenticationManager;
   
   private RealmMapping realmMapping;
   
   private Set<String> readCache = new ConcurrentHashSet<String>();

   private Set<String> writeCache = new ConcurrentHashSet<String>();

   private Set<String> createCache = new ConcurrentHashSet<String>();

   private long invalidationInterval;

   private long lastCheck;

   // Constructors --------------------------------------------------
   
   public SecurityStoreImpl(long invalidationInterval)
   {
   	this.invalidationInterval = invalidationInterval;
   }
   
   // SecurityManager implementation --------------------------------

   public Subject authenticate(String user, String password) throws Exception
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
   
   public void check(String address, CheckType checkType, ServerConnection conn) throws Exception
   {
      if (trace) { log.trace("checking access permissions to " + address); }

      if (checkCached(address, checkType))
      {
         // OK
         return;
      }

      // Authenticate. Successful autentication will place a new SubjectContext on thread local,
      // which will be used in the authorization process. However, we need to make sure we clean up
      // thread local immediately after we used the information, otherwise some other people
      // security my be screwed up, on account of thread local security stack being corrupted.

      authenticate(conn.getUsername(), conn.getPassword());

      // Authorize
      try
      {
         if (!authorize(conn.getUsername(), address, checkType))
         {
            String msg = "User: " + conn.getUsername() +
               " is not authorized to " +
               (checkType == CheckType.READ ? "read from" :
                  checkType == CheckType.WRITE ? "write to" : "create durable sub on") +
               " destination " + address;

           throw new MessagingException(MessagingException.SECURITY_EXCEPTION, msg);
         }
      }
      finally
      {
         // pop the Messaging SecurityContext, it did its job
         SecurityActions.popSubjectContext();
      }

      // if we get here we're granted, add to the cache
      
      switch (checkType.type)
      {
         case CheckType.TYPE_READ:
         {
            readCache.add(address);
            break;
         }
         case CheckType.TYPE_WRITE:
         {
            writeCache.add(address);
            break;
         }
         case CheckType.TYPE_CREATE:
         {
            createCache.add(address);
            break;
         }
         default:
         {
            throw new IllegalArgumentException("Invalid checkType:" + checkType);
         }
      }      
   }
   
   public void invalidateCache()
   {
   	readCache.clear();

      writeCache.clear();

      createCache.clear();
   }
   
   // Public --------------------------------------------------------

   public void setSecurityRepository(HierarchicalRepository<HashSet<Role>> securityRepository)
   {
      this.securityRepository = securityRepository;
   }
   
   public void setAuthenticationManager(AuthenticationManager authenticationManager)
   {
      this.authenticationManager = authenticationManager;
      
      this.realmMapping = (RealmMapping) authenticationManager;
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private boolean checkCached(String dest, CheckType checkType)
   {
      long now = System.currentTimeMillis();

      boolean granted = false;

      if (now - lastCheck > invalidationInterval)
      {
      	invalidateCache();
      }
      else
      {
         switch (checkType.type)
         {
            case CheckType.TYPE_READ:
            {
               granted = readCache.contains(dest);
               break;
            }
            case CheckType.TYPE_WRITE:
            {
               granted = writeCache.contains(dest);
               break;
            }
            case CheckType.TYPE_CREATE:
            {
               granted = createCache.contains(dest);
               break;
            }
            default:
            {
               throw new IllegalArgumentException("Invalid checkType:" + checkType);
            }
         }
      }

      lastCheck = now;

      return granted;
   }
   
   private boolean authorize(String user, String destination, CheckType checkType)
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

   // Inner class ---------------------------------------------------         
}
