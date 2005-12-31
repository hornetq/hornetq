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

import org.jboss.logging.Logger;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.RealmMapping;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.SubjectSecurityManager;
import org.w3c.dom.Element;

/**
 * A security manager for JMS. Mainly delegates to the JaasSecurityManager but also stores security
 * information for destinations.
 *
 * @author Peter Antman
 * @author Scott.Stark@jboss.org
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 * $Id$
 */
public class SecurityManager
{
   
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SecurityManager.class);
   
   // Attributes ----------------------------------------------------
   
   private Map securityConf = new HashMap();
   
   private AuthenticationManager authMgr;
   
   private RealmMapping realmMapping;
   
   private Element defaultSecurityConfig;
   
   private String securityDomain;
   

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void init() throws NamingException
   {
      if (log.isTraceEnabled()) { log.trace("Creating SecurityManager"); }

      // Get the JBoss security manager from JNDI
      InitialContext ic = new InitialContext();

      try
      {
         Object mgr = ic.lookup(securityDomain);

         if (log.isTraceEnabled()) { log.trace("JaasSecurityManager is: " + mgr); }

         authMgr = (AuthenticationManager)mgr;
         realmMapping = (RealmMapping)mgr;

         log.trace("JMS SecurityManager initialized");
      }
      catch (NamingException e)
      {
         // Apparently there is no security context, try adding java:/jaas
         log.warn("Failed to lookup securityDomain=" + securityDomain, e);

         if (securityDomain.startsWith("java:/jaas/") == false)
         {
            authMgr = (SubjectSecurityManager)ic.lookup("java:/jaas/" + securityDomain);
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
      
   /**
    * Get the security meta-data for the given destination.
    * 
    */
   public SecurityMetadata getSecurityMetadata(String destName)
   {
      if (log.isTraceEnabled()) { log.trace("Getting security metadata for " + destName); }
      
      SecurityMetadata m = (SecurityMetadata) securityConf.get(destName);
      if (m == null)
      {
         // No SecurityMetadata was configured for the dest,
         // Apply the default
         if (defaultSecurityConfig != null)
         {
            log.debug("No SecurityMetadadata was available for " + destName + " using default security config");
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
            log.warn("No SecurityMetadadata was available for " + destName + " adding guest");
            m = new SecurityMetadata();
         }
         securityConf.put(destName, m);
      }
      return m;
   }
   

   /**
    *  Authenticate the specified user with the given password
    *  Delegates to the JBoss AuthenticationManager
    * 
    * @param user
    * @param password
    * @return Subject 
    * @throws JMSSecurityException if the user is not authenticated
    */
   public Subject authenticate(String user, String password) throws JMSSecurityException
   {
      boolean trace = log.isTraceEnabled();
      
      if (trace)
      {
         log.trace("Authenticating, username=" + user);
      }
      
      SimplePrincipal principal = new SimplePrincipal(user);
      char[] passwordChars = null;
      if (password != null)
         passwordChars = password.toCharArray();
      Subject subject = new Subject();
      
      if (authMgr.isValid(principal, passwordChars, subject))
      {
         SecurityActions.pushSubjectContext(principal, passwordChars, subject);
         if (trace)
            log.trace("Username: " + user + " is authenticated");                  
         return subject;
      }
      else
      {
         if (trace) { log.trace("User " + user + " is NOT authenticated"); }
         throw new JMSSecurityException("User " + user + " is NOT authenticated");
      }
   }
   
   /**
    * Authorize that the subject has at least one of the specified roles
    * 
    * @param rolePrincipals - The set of roles allowed to read/write/create the destination
    * @return true if the subject is authorized, or false if not
    */
   public boolean authorize(String user, Set rolePrincipals)
   {   
      if (log.isTraceEnabled()) { log.trace("Checking authorize on user " + user + " for rolePrincipals " + rolePrincipals.toString()); }
      
      Principal principal = user == null ? null : new SimplePrincipal(user);
      
      boolean hasRole = realmMapping.doesUserHaveRole(principal, rolePrincipals);
           
      if (log.isTraceEnabled())
      {
         log.trace("User is authorized? " + hasRole);
      }
    
      return hasRole;
   }
   
   // SecurityManagerMBean implementation ----------------------------------
   
   public void setSecurityConfig(String destName, Element conf) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Adding security config. for destination:" + destName); }
      SecurityMetadata m = new SecurityMetadata(conf);
      securityConf.put(destName, m);
   }

   public void removeSecurityConf(String destName)
   {
      if (log.isTraceEnabled()) { log.trace("Removing security config. for destination:" + destName); }
      securityConf.remove(destName);
   }
   
   public Element getDefaultSecurityConfig()
   {
      return this.defaultSecurityConfig;
   }
   
   public void setDefaultSecurityConfig(Element conf)
      throws Exception
   {
      defaultSecurityConfig = conf;
      // Force a parse
      new SecurityMetadata(conf);
   }

   public String getSecurityDomain()
   {
      return this.securityDomain;
   }
   
   public void setSecurityDomain(String securityDomain)
   {
      this.securityDomain = securityDomain;
   }
   
   public SecurityManager getSecurityManager()
   {
      return this;
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
} // SecurityManager
