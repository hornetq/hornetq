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
package org.jboss.jms.server.connectormanager;

import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.ConnectorManager;
import org.jboss.logging.Logger;

/**
 * 
 * A SimpleConnectorManager.
 * 
 * The only function of these class is to add ConnectionListeners to Connectors
 * as connection factories are deployed.
 * Multiple connection factories can use the same connector and we don't want to install the connection
 * listener more than once, so we need to reference count.
 * If we can find out somehow if a connector already has a connection listener registered we can
 * get rid of this class, but currently remoting does not provide this functionality
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * SimpleConnectorManager.java,v 1.1 2006/04/13 19:43:05 timfox Exp
 */
public class SimpleConnectorManager implements ConnectorManager
{
   private static final Logger log = Logger.getLogger(SimpleConnectorManager.class);

   protected Map connectors;
   
   protected ConnectionManager cm;
   
   protected MBeanServer server;
   
   public SimpleConnectorManager(ConnectionManager cm)
   {
      connectors = new HashMap();
      
      this.cm = cm;
   }
   
   public void setServer(MBeanServer server)
   {
      this.server = server;
   }
   
   public synchronized void unregisterConnector(ObjectName connectorName) throws Exception
   { 
      String name = connectorName.getCanonicalName();
      
      Integer refCount = (Integer)connectors.get(name);
      
      if (refCount == null)
      {
         throw new IllegalArgumentException("Cannot find connector " + name + " to remove");
      }
      
      if (refCount.intValue() == 1)
      {
         connectors.remove(name);
      }
      else
      {
         connectors.put(name, new Integer(refCount.intValue() - 1));
      }         
   }
   
   public synchronized void registerConnector(ObjectName connectorName, boolean enablePing) throws Exception
   {
      String name = connectorName.getCanonicalName();
      
      Integer refCount = (Integer)connectors.get(name);
      
      if (refCount != null)
      {
         //This connector has already been registered by another connection factory, so no need to do anything
         //apart from increment the reference count
         connectors.put(name, new Integer(refCount.intValue() + 1));
      }
      else
      {                       
         if (enablePing)
         {        
            // install the connection listener that listens for failed connections            
            server.invoke(connectorName, "addConnectionListener",
                  new Object[] {cm},
                  new String[] {"org.jboss.remoting.ConnectionListener"});            
         }
               
         connectors.put(name, new Integer(1));
      }
   }
}
