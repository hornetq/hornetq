/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.server;

import java.util.List;



/**
 * An interface that allows management of ConnectionEnpoints and their association with remoting
 * clients.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface ConnectionManager
{
   void registerConnection(long remotingClientSessionID, ServerConnection endpoint);

   /**
    * @param ServerConnection 
    * @return null if there is no such connection.
    */
   ServerConnection unregisterConnection(long remotingClientSessionID, ServerConnection ServerConnection);
   
   /**
    * Returns a list of active connection endpoints currently maintained by an instance of this
    * manager. The implementation should make a copy of the list to avoid
    * ConcurrentModificationException. The list could be empty, but never null.
    *
    * @return List<ServerConnection>
    */
   List<ServerConnection> getActiveConnections();
}
