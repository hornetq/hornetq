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
package org.jboss.jms.client.delegate;

import java.util.Map;

import org.jboss.jms.server.Version;

/**
 * A ClusteredClientConnectionFactoryDelegate
 * 
 * This ConnectionFactoryDelegate handles connection factory operations
 * (getIdBlock, getClientAOPConfig like any other connection factory)
 * 
 * The actual failover logic is in the FailoverAspect
 * 
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 */
public class ClusteredClientConnectionFactoryDelegate extends ClientConnectionFactoryDelegate
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 8286850860206289277L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ClientConnectionFactoryDelegate[] delegates;

   //Map <node Id, failover node id>
   private Map failoverMap;

   // Constructors --------------------------------------------------

   public ClusteredClientConnectionFactoryDelegate(int objectID, int serverId,
                                                   String serverLocatorURI,
                                                   Version serverVersion, boolean clientPing,
                                                   ClientConnectionFactoryDelegate[] delegates,
                                                   Map failoverMap)
   {
      super(objectID, serverId, serverLocatorURI, serverVersion, clientPing);
      this.delegates = delegates;
      this.failoverMap = failoverMap;
   }

   // Some of the properties of ClientConnectionFactoryDelegate are not exposed.
   // I didn't want to expose then while I needed another delegate's properties to perform a copy.
   // So, I created this Constructor so I could have access into protected members inside an
   // extension class.

   public ClusteredClientConnectionFactoryDelegate(ClientConnectionFactoryDelegate mainDelegate,
                                                   ClientConnectionFactoryDelegate[] delegates,
                                                   Map failoverMap)
   {
      this(mainDelegate.getID(), mainDelegate.getServerID(), mainDelegate.getServerLocatorURI(),
           mainDelegate.getServerVersion(), mainDelegate.getClientPing(), delegates, failoverMap);
   }

   // DelegateSupport overrides -------------------------------------

   public void init()
   {
      super.init();

      for (int i = 0; i < delegates.length; i++)
      {
         if (this!=delegates[i])
         {
            delegates[i].init();
         }
      }      
   }

   // Public --------------------------------------------------------

   public ClientConnectionFactoryDelegate[] getDelegates()
   {
      return delegates;
   }

   public Map getFailoverMap()
   {
      return failoverMap;
   }
   
   public void setFailoverMap(Map failoverMap)
   {
      this.failoverMap = failoverMap;
   }
   
   public void setDelegates(ClientConnectionFactoryDelegate[] dels)
   {
      this.delegates = dels;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("ClusteredConnFactoryDelegate[");
      sb.append(id).append("][");
      if (delegates == null)
      {
         sb.append("-]");
      }
      else
      {
         for(int i = 0; i < delegates.length; i++)
         {
            sb.append(delegates[i].getServerID());
            if (i < delegates.length - 1)
            {
               sb.append(',');
            }
         }
         sb.append("]");
      }
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
