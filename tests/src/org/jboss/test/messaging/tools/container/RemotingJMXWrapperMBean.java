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
package org.jboss.test.messaging.tools.container;

import java.util.Set;

import org.jboss.remoting.ConnectionListener;
import org.jboss.remoting.ServerInvocationHandler;

/**
 * Note: This wrapper MUST NOT allow direct access to the Connector instance. This is necessary
 *       because the wrapper is maintaining the mapping between the Connector's
 *       ServerInvocationHandler instances and their subsystem names. Remoting should do that (it
 *       should have a Connector.getSubsystemNames() or similar), but it doesn't, so I have to do
 *       this by myself.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface RemotingJMXWrapperMBean
{
   void start() throws Exception;
   void stop() throws Exception;

   RemotingJMXWrapper getInstance();

   String getInvokerLocator() throws Exception;

   /**
    * This method's signature must be identical with Connector's
    * public ServerInvocationHandler addInvocationHandler(String subsystem,
    *                                ServerInvocationHandler handler) throws Exception;
    */
   ServerInvocationHandler addInvocationHandler(String subsystem, ServerInvocationHandler h)
         throws Exception;

   /**
    * This method's signature must be identical with Connector's
    * public void removeInvocationHandler(String subsystem) throws Exception;
    */
   void removeInvocationHandler(String subsystem) throws Exception;

   /**
    * @return Set<String> containing the subsystem names various ServerInvocationHandler instances
    *         have been added under to the Connector instance.
    */
   Set getConnectorSubsystems();

   void addConnectionListener(ConnectionListener listener);

   void removeConnectionListener(ConnectionListener listener);

   void setLeasePeriod(long leasePeriod);
   
   long getLeasePeriod();
}
