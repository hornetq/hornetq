/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.jmx;

import javax.jms.IllegalStateException;
import javax.management.ObjectName;

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
   
   protected String jndiName;
   
   protected Element securityConfig;
   
   protected String destinationName;
   
   protected ObjectName destinationManager;
   
   
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
   
   public void setSecurityConfig(Element securityConf)
   {
      this.securityConfig= securityConf;
   }
   
   
   public void startService() throws Exception     
   {
      destinationManager = new ObjectName("jboss.messaging:service=DestinationManager");
      
      if (serviceName != null)
      {
         destinationName = serviceName.getKeyProperty("name");
      }  
      
      if (destinationName == null || destinationName.length() == 0)
      {
         throw new IllegalStateException("QueueName was not set");
      }
            
      if (securityConfig != null)
      {
         server.invoke(serverPeer, "setSecurityConfig",
               new Object[] {destinationName, securityConfig},
               new String[] {"java.lang.String", "org.w3c.dom.Element"}); 
      
      }
   }
}
