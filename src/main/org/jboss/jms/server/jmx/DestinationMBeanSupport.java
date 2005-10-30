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
