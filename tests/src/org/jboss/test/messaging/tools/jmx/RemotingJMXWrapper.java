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
   private Connector connector;

   // Constructors --------------------------------------------------

   public RemotingJMXWrapper(InvokerLocator locator)
   {
      this.locator = locator;
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
   }

   public RemotingJMXWrapper getInstance()
   {
      return this;
   }

   public Connector getConnector() throws Exception
   {
      return connector;
   }

   public String getInvokerLocator() throws Exception
   {
      if (connector != null)
      {
         return connector.getInvokerLocator();
      }
      return null;
   }

   public ServerInvocationHandler addInvocationHandler(String s, ServerInvocationHandler h)
      throws Exception
   {
      if (connector != null)
      {
         return connector.addInvocationHandler(s, h);
      }
      return null;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
