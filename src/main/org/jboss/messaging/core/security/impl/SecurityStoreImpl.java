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

package org.jboss.messaging.core.security.impl;

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_PASSWORD;

import java.util.Set;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.HierarchicalRepositoryChangeListener;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.SimpleString;

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
public class SecurityStoreImpl implements SecurityStore, HierarchicalRepositoryChangeListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SecurityStoreImpl.class);

   public static final String CLUSTER_ADMIN_USER = "JBM.MANAGEMENT.ADMIN.USER";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private HierarchicalRepository<Set<Role>> securityRepository;

   private JBMSecurityManager securityManager;

   private final Set<SimpleString> readCache = new ConcurrentHashSet<SimpleString>();

   private final Set<SimpleString> writeCache = new ConcurrentHashSet<SimpleString>();

   private final Set<SimpleString> createCache = new ConcurrentHashSet<SimpleString>();

   private final long invalidationInterval;

   private volatile long lastCheck;
   
   private final boolean securityEnabled;
   
   private String managementClusterPassword;
   
   // Constructors --------------------------------------------------

   public SecurityStoreImpl(final long invalidationInterval, final boolean securityEnabled)
   {
   	this.invalidationInterval = invalidationInterval;
   	
   	this.securityEnabled = securityEnabled;
   }

   // SecurityManager implementation --------------------------------

   public void authenticate(final String user, final String password) throws Exception
   {
      if (securityEnabled)
      {
         if (CLUSTER_ADMIN_USER.equals(user))
         {
            if (trace) { log.trace("Authenticating cluster admin user"); }
            
            checkDefaultManagementClusterPassword(password);
            
            // The special user CLUSTER_ADMIN_USER is used for creating sessions that replicate management operation between nodes
            if (!managementClusterPassword.equals(password))
            {
               throw new MessagingException(MessagingException.SECURITY_EXCEPTION, "Unable to validate user: " + user);                 
            }
         }
         else
         {
            if (!securityManager.validateUser(user, password))
            {
               throw new MessagingException(MessagingException.SECURITY_EXCEPTION, "Unable to validate user: " + user);  
            }
         }
      }
   }

   public void check(final SimpleString address, final CheckType checkType, final ServerSession session) throws Exception
   {
      if (securityEnabled)
      {
         if (trace) { log.trace("checking access permissions to " + address); }
   
         if (checkCached(address, checkType))
         {
            // OK
            return;
         }
   
         String saddress = address.toString();
         
         Set<Role> roles = securityRepository.getMatch(saddress);
         
         String user = session.getUsername();
         
         if (CLUSTER_ADMIN_USER.equals(user))
         {
            // The special user CLUSTER_ADMIN_USER is used for creating sessions that replicate management operation between nodes
            //It has automatic read/write access to all destinations
            return;
         } 
         else if (!securityManager.validateUserAndRole(user, session.getPassword(), roles, checkType))
         {
             throw new MessagingException(MessagingException.SECURITY_EXCEPTION, "Unable to validate user: " + session.getUsername());
         }
         // if we get here we're granted, add to the cache
   
         switch (checkType)
         {
            case READ:
            {
               readCache.add(address);
               break;
            }
            case WRITE:
            {
               writeCache.add(address);
               break;
            }
            case CREATE:
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
   }

   public void onChange()
   {
      invalidateCache();
   }

   // Public --------------------------------------------------------

   public void setSecurityRepository(HierarchicalRepository<Set<Role>> securityRepository)
   {
      this.securityRepository = securityRepository;
      securityRepository.registerListener(this);
   }

   public void setSecurityManager(JBMSecurityManager securityManager)
   {
      this.securityManager = securityManager;
   }

   public void setManagementClusterPassword(String password)
   {           
      this.managementClusterPassword = password;
      
      checkDefaultManagementClusterPassword(password);      
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   private void invalidateCache()
   {
      readCache.clear();

      writeCache.clear();

      createCache.clear();
   }

   private boolean checkCached(final SimpleString dest, final CheckType checkType)
   {
      long now = System.currentTimeMillis();

      boolean granted = false;

      if (now - lastCheck > invalidationInterval)
      {
      	invalidateCache();
      }
      else
      {
         switch (checkType)
         {
            case READ:
            {
               granted = readCache.contains(dest);
               break;
            }
            case WRITE:
            {
               granted = writeCache.contains(dest);
               break;
            }
            case CREATE:
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
   
   private void checkDefaultManagementClusterPassword(String password)
   {
      // Sanity check
      if (DEFAULT_MANAGEMENT_CLUSTER_PASSWORD.equals(password))
      {
         log.warn("It has been detected that the cluster admin password which is used to " +
                  "replicate management operation from one node to the other has not had its password changed from the installation default. " +
                  "Please see the JBoss Messaging user guide for instructions on how to do this.");
      }
   }

   // Inner class ---------------------------------------------------

}
