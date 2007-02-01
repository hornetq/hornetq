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

package org.jboss.jms.client.remoting;

import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.wireformat.ConnectionFactoryUpdate;
import org.jboss.logging.Logger;

/**
 * This class will manage ConnectionFactory messages updates
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactoryCallbackHandler
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ConnectionFactoryCallbackHandler.class);

   // Attributes -----------------------------------------------------------------------------------

   private ClientConnectionDelegate connectionDelegate;
   private ConnectionState state;

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Constructors ---------------------------------------------------------------------------------

   public ConnectionFactoryCallbackHandler(ClientConnectionDelegate connectionDelegate)
   {
      this.connectionDelegate = connectionDelegate;
   }

   // Public ---------------------------------------------------------------------------------------

   public void handleMessage(Object message)
   {
      if (trace) { log.trace(this + " handling " + message); }

      ConnectionFactoryUpdate viewChange = (ConnectionFactoryUpdate)message;

      Object d = getState().getClusteredConnectionFactoryDelegate();

      if (d instanceof ClientClusteredConnectionFactoryDelegate)
      {
         ClientClusteredConnectionFactoryDelegate clusteredDelegate =
            (ClientClusteredConnectionFactoryDelegate)d;

         clusteredDelegate.updateFailoverInfo(viewChange.getDelegates(),
                                              viewChange.getFailoverMap());
      }
   }

   public String toString()
   {
      return "ConnectionFactoryCallbackHandler[" + connectionDelegate + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // When ConnectionFactoryCallbackHandler is created, is not guaranteed that state is set
   // as this could be later initialized by the aop stack
   protected ConnectionState getState()
   {
      if (state==null)
      {
         this.state = (ConnectionState)connectionDelegate.getState();
      }
      return this.state;
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
