package org.hornetq.service;

import org.hornetq.integration.jboss.security.JBossASSecurityManager;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Oct 20, 2009
 */
public interface JBossASSecurityManagerServiceMBean
{
   void create();

   JBossASSecurityManager getJBossASSecurityManager();
}
