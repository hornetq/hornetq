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
import java.util.Iterator;
import java.util.Map;

import org.jboss.logging.Logger;

/**
 * This class manages ResourceManager instances. It ensures there is one instance per instance of
 * JMS server as specified by the server ID.
 * 
 * This allows different JMS connections to the same JMS server (the underlying resource is the JMS
 * server) to use the same resource manager.
 * 
 * This means isSameRM() on XAResource returns true, allowing the Transaction manager to join work
 * in one tx to another thus allowing 1PC optimization which should help performance.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision: 1329 $
 *
 * $Id: ResourceManagerFactory.java 1329 2006-09-20 21:29:56Z ovidiu.feodorov@jboss.com $
 */
public class ResourceManagerFactory
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ResourceManagerFactory.class);

	
   // Static ---------------------------------------------------------------------------------------

   public static ResourceManagerFactory instance = new ResourceManagerFactory();
   
   // Attributes -----------------------------------------------------------------------------------

   private Map holders;

   // Constructors ---------------------------------------------------------------------------------

   private ResourceManagerFactory()
   {
      holders = new HashMap();
   }

   // Public ---------------------------------------------------------------------------------------

   public synchronized void clear()
   {
      holders.clear();
   }
      
   public synchronized int size()
   {
      return holders.size();
   }

   public synchronized boolean containsResourceManager(int serverID)
   {
      return holders.containsKey(new Integer(serverID));
   }

   /**
    * @param serverID - server peer ID.
    */
   public synchronized ResourceManager checkOutResourceManager(int serverID)
   {
      Integer i = new Integer(serverID);

      Holder h = (Holder)holders.get(i);

      if (h == null)
      {
         h = new Holder(serverID);
         holders.put(i, h);
      }
      else
      {
         h.refCount++;
      }
      
      return h.rm;
   }

   public synchronized void checkInResourceManager(int serverID)
   {
      Integer i = new Integer(serverID);
      Holder h = (Holder)holders.get(i);

      if (h == null)
      {
         throw new IllegalArgumentException("Cannot find resource manager for server: " + serverID);
      }

      h.refCount--;

      if (h.refCount == 0)
      {
         holders.remove(i);
      }
   }

   /**
    * Need to failover rm from old server id to new server id, merging resource managers if it
    * already exists.
    */
   public synchronized void handleFailover(int oldServerID, int newServerID)
   {
      Holder hOld = (Holder)holders.remove(new Integer(oldServerID));

      if (hOld == null)
      {
         // This is ok - this would happen if there are more than one connections for the old server
         // failing in which only the first one to fail would failover the resource manager factory
         // - since they share the same rm.
         return;
      }

      ResourceManager oldRM = hOld.rm;
      ResourceManager newRM = null;
      Holder hNew = (Holder)holders.get(new Integer(newServerID));

      if (hNew != null)
      {
         // need to merge into the new

         newRM = hNew.rm;
         newRM.merge(oldRM);
      }
      else
      {
         // re-register the old rm with the new id

         Holder h = new Holder(oldRM);
         holders.put(new Integer(newServerID), h);
      }
   }
   
   public void dump()
   {
   	log.debug("Dumping " + this);
   	Iterator iter = holders.entrySet().iterator();
   	while (iter.hasNext())
   	{
   		Map.Entry entry = (Map.Entry)iter.next();
   		
   		log.debug(entry.getKey() + "--->" + entry.getValue());
   	}
   	log.debug("End dump");
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private static class Holder
   {
      ResourceManager rm;
      
      Holder(int serverID)
      {
         rm = new ResourceManager(serverID);
      }
      
      Holder(ResourceManager rm)
      {
         this.rm = rm;
      }
      
      int refCount = 1;
   }
}
