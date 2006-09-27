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
package org.jboss.jms.tx;

import java.util.HashMap;
import java.util.Map;

/**
 * This class manages instances of ResourceManager. It ensures there is one instance per instance
 * of JMS server as specified by the server id.
 * 
 * This allows different JMS connections to the same JMS server (the underlying resource is the JMS server)
 * to use the same resource manager.
 * 
 * This means isSameRM() on XAResource returns true, allowing the Transaction manager to join work in one
 * tx to another thus allowing 1PC optimization which should help performance.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ResourceManagerFactory
{      
   public static ResourceManagerFactory instance = new ResourceManagerFactory();
   
   private Map holders;
   
   private ResourceManagerFactory()
   {      
      holders = new HashMap();
   }
      
   public synchronized boolean containsResourceManager(String serverID)
   {
      return holders.containsKey(serverID);
   }
   
   /**
    * @param serverID - server peer ID.
    */
   public synchronized ResourceManager checkOutResourceManager(String serverID)
   {
      Holder h = (Holder)holders.get(serverID);
      
      if (h == null)
      {
         h = new Holder();
         
         holders.put(serverID, h);
      }
      else
      {
         h.refCount++;
      }
      
      return h.rm;
   }
   
   public synchronized void checkInResourceManager(String serverID)
   {
      Holder h = (Holder)holders.get(serverID);
      
      if (h == null)
      {
         throw new IllegalArgumentException("Cannot find resource manager for server: " + serverID);
      }
      
      h.refCount--;
      
      if (h.refCount == 0)
      {
         holders.remove(serverID);
      }      
   }
   
   private class Holder
   {
      ResourceManager rm = new ResourceManager();
      
      int refCount = 1;
   }
  
}
