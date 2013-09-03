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

package org.hornetq.core.security.impl;

import static org.hornetq.api.core.management.NotificationType.SECURITY_AUTHENTICATION_VIOLATION;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.core.security.SecurityStore;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.HierarchicalRepositoryChangeListener;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.TypedProperties;

/**
 * The HornetQ SecurityStore implementation
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 *
 *
 * @version $Revision$
 *
 */
public class SecurityStoreImpl implements SecurityStore, HierarchicalRepositoryChangeListener
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private final boolean trace = HornetQServerLogger.LOGGER.isTraceEnabled();

   private final HierarchicalRepository<Set<Role>> securityRepository;

   private final HornetQSecurityManager securityManager;

   private final ConcurrentMap<String, ConcurrentHashSet<SimpleString>> cache = new ConcurrentHashMap<String, ConcurrentHashSet<SimpleString>>();

   private final long invalidationInterval;

   private volatile long lastCheck;

   private final boolean securityEnabled;

   private final String managementClusterUser;

   private final String managementClusterPassword;

   private final NotificationService notificationService;

   // Constructors --------------------------------------------------

   /**
    * @param notificationService can be <code>null</code>
    */
   public SecurityStoreImpl(final HierarchicalRepository<Set<Role>> securityRepository,
                            final HornetQSecurityManager securityManager,
                            final long invalidationInterval,
                            final boolean securityEnabled,
                            final String managementClusterUser,
                            final String managementClusterPassword,
                            final NotificationService notificationService)
   {
      this.securityRepository = securityRepository;
      this.securityManager = securityManager;
      this.invalidationInterval = invalidationInterval;
      this.securityEnabled = securityEnabled;
      this.managementClusterUser = managementClusterUser;
      this.managementClusterPassword = managementClusterPassword;
      this.notificationService = notificationService;
      this.securityRepository.registerListener(this);
   }

   // SecurityManager implementation --------------------------------

   public void stop()
   {
      securityRepository.unRegisterListener(this);
   }

   public void authenticate(final String user, final String password) throws Exception
   {
      if (securityEnabled)
      {

         if (managementClusterUser.equals(user))
         {
            if (trace)
            {
               HornetQServerLogger.LOGGER.trace("Authenticating cluster admin user");
            }

            /*
             * The special user cluster user is used for creating sessions that replicate management
             * operation between nodes
             */
            if (!managementClusterPassword.equals(password))
            {
               throw HornetQMessageBundle.BUNDLE.unableToValidateClusterUser(user);
            }
            else
            {
               return;
            }
         }

         if (!securityManager.validateUser(user, password))
         {
            if (notificationService != null)
            {
               TypedProperties props = new TypedProperties();

               props.putSimpleStringProperty(ManagementHelper.HDR_USER, SimpleString.toSimpleString(user));

               Notification notification = new Notification(null, SECURITY_AUTHENTICATION_VIOLATION, props);

               notificationService.sendNotification(notification);
            }

            throw HornetQMessageBundle.BUNDLE.unableToValidateUser(user);
         }
      }
   }

   public void check(final SimpleString address, final CheckType checkType, final ServerSession session) throws Exception
   {
      if (securityEnabled)
      {
         if (trace)
         {
            HornetQServerLogger.LOGGER.trace("checking access permissions to " + address);
         }

         String user = session.getUsername();
         if (checkCached(address, user, checkType))
         {
            // OK
            return;
         }

         String saddress = address.toString();

         Set<Role> roles = securityRepository.getMatch(saddress);

         // bypass permission checks for management cluster user
         if (managementClusterUser.equals(user) && session.getPassword().equals(managementClusterPassword))
         {
            return;
         }

         if (!securityManager.validateUserAndRole(user, session.getPassword(), roles, checkType))
         {
            if (notificationService != null)
            {
               TypedProperties props = new TypedProperties();

               props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, address);
               props.putSimpleStringProperty(ManagementHelper.HDR_CHECK_TYPE, new SimpleString(checkType.toString()));
               props.putSimpleStringProperty(ManagementHelper.HDR_USER, SimpleString.toSimpleString(user));

               Notification notification = new Notification(null, NotificationType.SECURITY_PERMISSION_VIOLATION, props);

               notificationService.sendNotification(notification);
            }

            throw HornetQMessageBundle.BUNDLE.userNoPermissions(session.getUsername(), checkType, saddress);
         }
         // if we get here we're granted, add to the cache
         ConcurrentHashSet<SimpleString> set = new ConcurrentHashSet<SimpleString>();
         ConcurrentHashSet<SimpleString> act = cache.putIfAbsent(user + "." + checkType.name(), set);
         if (act != null)
         {
            set = act;
         }
         set.add(address);

      }
   }

   public void onChange()
   {
      invalidateCache();
   }

   // Public --------------------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   private void invalidateCache()
   {
      cache.clear();
   }

   private boolean checkCached(final SimpleString dest, final String user, final CheckType checkType)
   {
      long now = System.currentTimeMillis();

      boolean granted = false;

      if (now - lastCheck > invalidationInterval)
      {
         invalidateCache();

         lastCheck = now;
      }
      else
      {
         ConcurrentHashSet<SimpleString> act = cache.get(user + "." + checkType.name());
         if (act != null)
         {
            granted = act.contains(dest);
         }
      }

      return granted;
   }

   // Inner class ---------------------------------------------------

}
