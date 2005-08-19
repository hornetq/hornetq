/*
* JBoss, the OpenSource EJB server
*
* Distributable under LGPL license.
* See terms of license at gnu.org.
*/
package org.jboss.jms.security;

import org.jboss.system.ServiceMBean;
import org.w3c.dom.Element;

/**
 * MBean interface for security manager.
 *
 * @author <a href="pra@tim.se">Peter Antman</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 */

public interface SecurityManagerMBean extends ServiceMBean
{

   Element getDefaultSecurityConfig();
   
   void setDefaultSecurityConfig(Element conf) throws Exception;

   String getSecurityDomain();
   
   void setSecurityDomain(String securityDomain);
   
   SecurityManager getSecurityManager();
   
   void setSecurityConf(String destName, Element conf) throws Exception;
   
   void removeSecurityConf(String destName);

}
