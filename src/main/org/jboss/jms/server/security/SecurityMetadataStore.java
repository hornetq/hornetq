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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSSecurityException;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.Subject;

import org.jboss.jms.server.SecurityManager;
import org.jboss.logging.Logger;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.RealmMapping;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.SubjectSecurityManager;
import org.w3c.dom.Element;

/**
 * A security metadate store for JMS. Stores security information for destinations and delegates
 * authentication and authorization to a JaasSecurityManager.
 *
 * @author Peter Antman
 * @author <a href="mailto:Scott.Stark@jboss.org">Scott Stark</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class SecurityMetadataStore implements SecurityManager
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SecurityMetadataStore.class);
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   private Map queueSecurityConf;
   private Map topicSecurityConf;

   private AuthenticationManager authenticationManager;
   private RealmMapping realmMapping;
   
   private Element defaultSecurityConfig;
   private String securityDomain;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   public SecurityMetadataStore()
   {
      queueSecurityConf = new HashMap();
      topicSecurityConf = new HashMap();
   }

   // SecurityManager implementation --------------------------------

   public SecurityMetadata getSecurityMetadata(boolean isQueue, String destName)
   {
      SecurityMetadata m = (SecurityMetadata)
         (isQueue ? queueSecurityConf.get(destName) : topicSecurityConf.get(destName));

      if (m == null)
      {
         // No SecurityMetadata was configured for the destination, apply the default
         if (defaultSecurityConfig != null)
         {
            log.debug("No SecurityMetadadata was available for " + destName + ", using default security config");
            try
            {
               m = new SecurityMetadata(defaultSecurityConfig);
            }
            catch (Exception e)
            {
               log.warn("Unable to apply default security for destName, using guest " + destName, e);
               m = new SecurityMetadata();
            }
         }
         else
         {
            // default to guest
            log.warn("No SecurityMetadadata was available for " + destName + ", adding guest");
            m = new SecurityMetadata();
         }

         // don't cache it! this way the callers will be able to take advantage of default security
         // configuration updates
         // securityConf.put(destName, m);
      }
      return m;
   }

   public void setSecurityConfig(boolean isQueue, String destName, Element conf) throws Exception
   {
      if (trace) { log.trace("adding security configuration for " + (isQueue ? "queue " : "topic ") + destName); }

      SecurityMetadata m = new SecurityMetadata(conf);

      if (isQueue)
      {
         queueSecurityConf.put(destName, m);
      }
      else
      {
         topicSecurityConf.put(destName, m);
      }
   }

   public void clearSecurityConfig(boolean isQueue, String name) throws Exception
   {
      if (trace) { log.trace("clearing security configuration for " + (isQueue ? "queue " : "topic ") + name); }

      if (isQueue)
      {
         queueSecurityConf.remove(name);
      }
      else
      {
         topicSecurityConf.remove(name);
      }
   }

   public Subject authenticate(String user, String password) throws JMSSecurityException
   {
      if (trace) { log.trace("authenticating user " + user); }

      SimplePrincipal principal = new SimplePrincipal(user);
      char[] passwordChars = null;
      if (password != null)
      {
         passwordChars = password.toCharArray();
      }

      Subject subject = new Subject();

      if (authenticationManager.isValid(principal, passwordChars, subject))
      {
         return subject;
      }
      else
      {
         throw new JMSSecurityException("User " + user + " is NOT authenticated");
      }
   }

   public boolean authorize(String user, Set rolePrincipals)
   {
      if (trace) { log.trace("authorizing user " + user + " for role(s) " + rolePrincipals.toString()); }

      Principal principal = user == null ? null : new SimplePrincipal(user);

      boolean hasRole = realmMapping.doesUserHaveRole(principal, rolePrincipals);

      if (trace) { log.trace("user " + user + (hasRole ? " is " : " is NOT ") + "authorized"); }

      return hasRole;
   }

   // Public --------------------------------------------------------
   
   public void start() throws NamingException
   {
      if (trace) { log.trace("initializing SecurityMetadataStore"); }

      // Get the JBoss security manager from JNDI
      InitialContext ic = new InitialContext();

      try
      {
         Object mgr = ic.lookup(securityDomain);

         log.debug("JaasSecurityManager is " + mgr);

         authenticationManager = (AuthenticationManager)mgr;
         realmMapping = (RealmMapping)mgr;

         log.trace("SecurityMetadataStore initialized");
      }
      catch (NamingException e)
      {
         // Apparently there is no security context, try adding java:/jaas
         log.warn("Failed to lookup securityDomain " + securityDomain, e);

         if (!securityDomain.startsWith("java:/jaas/"))
         {
            authenticationManager =
               (SubjectSecurityManager)ic.lookup("java:/jaas/" + securityDomain);
         }
         else
         {
            throw e;
         }
      }
      finally
      {
         ic.close();
      }
   }

   public void stop() throws Exception
   {
   }

   public String getSecurityDomain()
   {
      return this.securityDomain;
   }

   public void setSecurityDomain(String securityDomain)
   {
      this.securityDomain = securityDomain;
   }

   public Element getDefaultSecurityConfig()
   {
      return this.defaultSecurityConfig;
   }

   public void setDefaultSecurityConfig(Element conf) throws Exception
   {
      // Force a parse
      new SecurityMetadata(conf);
      defaultSecurityConfig = conf;
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner class ---------------------------------------------------

}
