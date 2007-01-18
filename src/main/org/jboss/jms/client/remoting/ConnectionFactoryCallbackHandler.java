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
import org.jboss.jms.server.endpoint.ConnectionFactoryUpdateMessage;
import org.jboss.logging.Logger;

/**
 * This class will manage ConnectionFactory messages updates
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
 */
public class ConnectionFactoryCallbackHandler implements CallbackHandler
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   ClientConnectionDelegate connectionDelegate;
   ConnectionState state;

   // Static ---------------------------------------------------------------------------------------

   protected static final Logger log = Logger.getLogger(ConnectionFactoryCallbackHandler.class);
   private static boolean trace = log.isTraceEnabled();

   // Constructors ---------------------------------------------------------------------------------

   public ConnectionFactoryCallbackHandler(ClientConnectionDelegate connectionDelegate)
   {
      this.connectionDelegate = connectionDelegate;
      this.state = (ConnectionState)connectionDelegate.getState();
   }


   // Implementation of CallbackHandler ------------------------------------------------------------
   public void handleMessage(Object message)
   {
      ConnectionFactoryUpdateMessage updateMessage = (ConnectionFactoryUpdateMessage) message;


      if (getState().getClusteredConnectionFactoryDelegate() != null &&
          getState().getClusteredConnectionFactoryDelegate()
             instanceof ClientClusteredConnectionFactoryDelegate)
      {
         ClientClusteredConnectionFactoryDelegate delegate =
            (ClientClusteredConnectionFactoryDelegate) getState().
               getClusteredConnectionFactoryDelegate();

         delegate.updateFailoverInfo(updateMessage.getDelegates(), updateMessage.getFailoverMap());
      }

   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

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
