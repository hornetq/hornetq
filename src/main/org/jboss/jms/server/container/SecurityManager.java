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
package org.jboss.jms.server.container;

import java.util.HashSet;
import java.util.Set;

import org.jboss.jms.server.SecurityStore;
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.jms.server.security.CheckType;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

/**
 * This aspect enforces the JBossMessaging JMS security policy.
 *
 * This aspect is PER_INSTANCE
 *
 * For performance reasons we cache access rights in the interceptor for a maximum of
 * INVALIDATION_INTERVAL milliseconds.
 * This is because we don't want to do a full authentication and authorization on every send,
 * for example, since this will drastically reduce performance.
 * This means any changes to security data won't be reflected until INVALIDATION_INTERVAL
 * milliseconds later.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision 1.1 $</tt>
 *
 * $Id$
 */
public class SecurityManager
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(java.lang.SecurityManager.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private Set<String> readCache;

   private Set<String> writeCache;

   private Set createCache;

   //TODO Make this configurable
   private static final long INVALIDATION_INTERVAL = 15000;

   private long lastCheck;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   public SecurityManager()
   {
      readCache = new HashSet<String>();

      writeCache = new HashSet<String>();

      createCache = new HashSet<String>();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   public boolean checkCached(String dest, CheckType checkType)
   {
      long now = System.currentTimeMillis();

      boolean granted = false;

      if (now - lastCheck > INVALIDATION_INTERVAL)
      {
         readCache.clear();

         writeCache.clear();

         createCache.clear();
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

   public void check(String address, CheckType checkType, ServerConnectionEndpoint conn)
      throws MessagingException
   {
      if (trace) { log.trace("checking access permissions to " + address); }

      if (checkCached(address, checkType))
      {
         // OK
         return;
      }

      SecurityStore sm = conn.getSecurityManager();

      // Authenticate. Successful autentication will place a new SubjectContext on thread local,
      // which will be used in the authorization process. However, we need to make sure we clean up
      // thread local immediately after we used the information, otherwise some other people
      // security my be screwed up, on account of thread local security stack being corrupted.

      sm.authenticate(conn.getUsername(), conn.getPassword());

      // Authorize
      try
      {
         if (!sm.authorize(conn.getUsername(), address, checkType))
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
   
   // Inner classes -------------------------------------------------
  
}




