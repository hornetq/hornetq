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
package org.jboss.jms.server;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;

import org.jboss.jms.server.endpoint.ServerConnectionDelegate;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.DurableSubscription;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * Implementation of ClientManager
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientManagerImpl implements ClientManager
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ClientManagerImpl.class);


   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;
   protected Map connections;
   
   
   // Constructors --------------------------------------------------

   public ClientManagerImpl(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
      connections = new ConcurrentReaderHashMap();
      

   }

   // Public --------------------------------------------------------
   

   public ServerConnectionDelegate putConnectionDelegate(Serializable connectionID,
                                                         ServerConnectionDelegate d)
   {
      return (ServerConnectionDelegate)connections.put(connectionID, d);
   }
   
   public void removeConnectionDelegate(Serializable connectionID)
   {
      connections.remove(connectionID);
   }

   public ServerConnectionDelegate getConnectionDelegate(Serializable connectionID)
   {
      return (ServerConnectionDelegate)connections.get(connectionID);
   }
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
