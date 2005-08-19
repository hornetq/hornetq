/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.jmx;

import javax.jms.IllegalStateException;
import javax.management.ObjectName;

import org.jboss.jms.security.SecurityManager;
import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;

/**
 * Implementation of DestinationManager MBean
 *
 * @author <a href="pra@tim.se">Peter Antman</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 */
public class DestinationMBeanSupport extends ServiceMBeanSupport implements DestinationMBean
{

   protected ObjectName serverPeer;
   
   protected ObjectName securityManager;
   
   protected String jndiName;
   
   protected Element securityConf;
   
   protected String destinationName;
   
   
   public ObjectName getServerPeer()
   {
      return serverPeer;
   }
   
   public void setServerPeer(ObjectName on)
   {
      this.serverPeer = on;
   }

   public void setJNDIName(String name)
   {
      this.jndiName = name;
   }

   public String getJNDIName()
   {
      return jndiName;
   }
   
   public void setSecurityConf(Element securityConf)
   {
      this.securityConf = securityConf;
   }
   

   public void setSecurityManager(ObjectName securityManager)
   {
      this.securityManager = securityManager;
   }
   
   public void startService() throws Exception
   {
      if (serviceName != null)
      {
         destinationName = serviceName.getKeyProperty("name");
      }  
      
      if (destinationName == null || destinationName.length() == 0)
      {
         throw new IllegalStateException("QueueName was not set");
      }
            
      if (log.isTraceEnabled()) { log.trace("starting service, securityManager=" + securityManager); }
      
      //securityManager can be null
      
      if (securityManager != null)
      {
         
         //Get ref to the SecurityManager
         SecurityManager sm = (SecurityManager)
            server.getAttribute(securityManager, "SecurityManager");
         
         //Inform the sm about the security conf for the destination
         sm.setSecurityConf(destinationName, securityConf);
      }
   }
}
