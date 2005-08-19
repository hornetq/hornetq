/*
 * JBoss, the OpenSource EJB server
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.security;

import java.security.Principal;
import java.security.acl.Group;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSSecurityException;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.Subject;

import org.jboss.logging.Logger;
import org.jboss.security.AnybodyPrincipal;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.NobodyPrincipal;
import org.jboss.security.RealmMapping;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.SubjectSecurityManager;
import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;

/**
 * A security manager for JMS.
 * Mainly delegates to the JaasSecurityManager but also stores security information for
 * destinations
 *
 * @author Peter Antman
 * @author Scott.Stark@jboss.org
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 */
public class SecurityManager extends ServiceMBeanSupport implements SecurityManagerMBean
{
   
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SecurityManager.class);
   
   // Attributes ----------------------------------------------------
   
   Map securityConf = new HashMap();
   
   AuthenticationManager authMgr;
   
   RealmMapping realmMapping;
   
   Element defaultSecurityConfig;
   
   String securityDomain;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
      
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
         if (trace)
            log.trace("Username: " + user + " is authenticated");                  
         return subject;
      }
      else
      {
         if (trace)
            log.trace("User: " + user + " is NOT authenticated");
         throw new JMSSecurityException("User: " + user + " is NOT authenticated");
      }
   }
   
   /**
    * Authorize that the subject has at least one of the specified roles
    * 
    * Note!
    * We check the roles manually from the subject since the JaasSecurityManager implementation
    * of doesUserHaveRole relies on the active subject being set which it doesn't appear
    * to be even after a successful authentication on the same thread.
    * So in order to get this to work we've borrowed some code from JaasSecurityManager    
    * 
    * @param subject
    * @param rolePrincipals - The set of roles allowed to read/write/create the destination
    * @return true if the subject is authorized, or false if not
    */
   public boolean authorize(Subject subject, Set rolePrincipals)
   {   
      if (log.isTraceEnabled())
         log.trace(
               "Checking authorize on subject: "
               + subject
               + " for rolePrincipals "
               + rolePrincipals.toString());
     
      //Hmmmm... Commented out since it doesn't appear to work
      //The active subject doesn't seem to be set even though
      //a preceeding successful call to autheticate had been completed on the
      //same thread
      //boolean hasRole = realmMapping.doesUserHaveRole(principal, rolePrincipals);
      
      /* From JaasSecurityManager */
      boolean hasRole = false;
      Group roles = getSubjectRoles(subject);
      if( log.isTraceEnabled() ) { log.trace("roles="+roles); }
      if( roles != null )
      {
         Iterator iter = rolePrincipals.iterator();
         while( hasRole == false && iter.hasNext() )
         {
            Principal role = (Principal) iter.next();
            hasRole = doesRoleGroupHaveRole(role, roles);
            if( log.isTraceEnabled() ) { log.trace("hasRole("+role+")="+hasRole); }
         }
      }
                  
      if (log.isTraceEnabled())
      {
         log.trace("User is authorized? " + hasRole);
      }
    
      return hasRole;
   }
   
   public void startService() throws Exception
   {
      if (log.isTraceEnabled()) { log.trace("Starting SecurityManager service"); }
      // Get the JBoss security manager from JNDI
      InitialContext iniCtx = new InitialContext();
      try
      {
         Object mgr = iniCtx.lookup(securityDomain);
         
         if (log.isTraceEnabled()) { log.trace("JaasSecurityManager is: " + mgr); }
         
         authMgr = (SubjectSecurityManager)mgr;
         realmMapping = (RealmMapping)mgr;
      }
      catch (NamingException e)
      {
         // Apparently there is no security context, try adding java:/jaas
         log.warn("Failed to lookup securityDomain=" + securityDomain, e);
         if (securityDomain.startsWith("java:/jaas/") == false)
            authMgr = (SubjectSecurityManager) iniCtx.lookup("java:/jaas/" + securityDomain);
         else
            throw e;
      }      
   }
   
   public void stopService() throws Exception
   {
      // Anything to do here?
   }
   
   // SecurityManagerMBean implementation ----------------------------------
   
   public void setSecurityConf(String destName, Element conf) throws Exception
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
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   /* From JaasSecurityManager */
   private Group getSubjectRoles(Subject theSubject)
   {
       Set subjectGroups = theSubject.getPrincipals(Group.class);
       Iterator iter = subjectGroups.iterator();
       Group roles = null;
       while( iter.hasNext() )
       {
          Group grp = (Group) iter.next();
          String name = grp.getName();
          if( name.equals("Roles") )
             roles = grp;
       }
      return roles;
   }
   
   /* From JaasSecurityManager */
   protected boolean doesRoleGroupHaveRole(Principal role, Group userRoles)
   {
      // First check that role is not a NobodyPrincipal
      if (role instanceof NobodyPrincipal)
         return false;

      // Check for inclusion in the user's role set
      boolean isMember = userRoles.isMember(role);
      if (isMember == false)
      {   // Check the AnybodyPrincipal special cases
         isMember = (role instanceof AnybodyPrincipal);
      }

      return isMember;
   }
    
} // SecurityManager
