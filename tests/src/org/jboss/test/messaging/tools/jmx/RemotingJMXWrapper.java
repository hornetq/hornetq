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
package org.jboss.test.messaging.tools.jmx;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.remoting.ConnectionListener;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.transport.Connector;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RemotingJMXWrapper implements RemotingJMXWrapperMBean
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InvokerLocator locator;

   // DO NOT expose the Connector instance externally bacause consistency of subsytemToHandler map
   //        may be compromised!
   private Connector connector;

   private Map subsystemToHandler;

   // Constructors --------------------------------------------------

   public RemotingJMXWrapper(InvokerLocator locator)
   {
      this.locator = locator;
      this.subsystemToHandler = new HashMap();
   }

   // RemotingJMXWrapper implementation -----------------------------

   public void start() throws Exception
   {
      if (connector != null)
      {
         return;
      }

      connector = new Connector();
      connector.setInvokerLocator(locator.getLocatorURI());
      connector.start();
   }

   public void stop() throws Exception
   {
      if (connector == null)
      {
         return;
      }

      connector.stop();
      connector = null;
      subsystemToHandler.clear();
   }

   public RemotingJMXWrapper getInstance()
   {
      return this;
   }

   public String getInvokerLocator() throws Exception
   {
      if (connector != null)
      {
         return connector.getInvokerLocator();
      }
      return null;
   }

   public Set getConnectorSubsystems()
   {
      // create a serializable Set instance
      return new HashSet(subsystemToHandler.keySet());
   }

   public ServerInvocationHandler addInvocationHandler(String subsystem, ServerInvocationHandler h)
      throws Exception
   {
      if (connector != null)
      {
         subsystemToHandler.put(subsystem, h);
         return connector.addInvocationHandler(subsystem, h);
      }
      return null;
   }

   public void removeInvocationHandler(String subsystem) throws Exception
   {
      subsystemToHandler.remove(subsystem);
      connector.removeInvocationHandler(subsystem);
   }

   public void addConnectionListener(ConnectionListener listener)
   {
      connector.addConnectionListener(listener);
   }

   public void removeConnectionListener(ConnectionListener listener)
   {
      connector.removeConnectionListener(listener);
   }

   public void setLeasePeriod(long leasePeriod)
   {
      connector.setLeasePeriod(leasePeriod);
   }
   
   public long getLeasePeriod()
   {
      return connector.getLeasePeriod();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
