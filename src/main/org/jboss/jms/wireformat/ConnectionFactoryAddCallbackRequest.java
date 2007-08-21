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

package org.jboss.jms.wireformat;

import org.jboss.jms.server.endpoint.advised.ConnectionFactoryAdvised;
import org.jboss.jms.delegate.CreateConnectionResult;
import org.jboss.logging.Logger;
import java.io.DataOutputStream;
import java.io.DataInputStream;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class ConnectionFactoryAddCallbackRequest extends CallbackRequestSupport
{

   private static final Logger log = Logger.getLogger(ConnectionFactoryAddCallbackRequest.class);


   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ConnectionFactoryAddCallbackRequest()
   {
   }

   public ConnectionFactoryAddCallbackRequest(String jvmSessionId, String remotingSessionId, String objectId, byte version)
   {
      super(jvmSessionId, remotingSessionId, objectId, PacketSupport.REQ_CONNECTIONFACTORY_ADDCALLBACK, version);
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   public ResponseSupport serverInvoke() throws Exception
   {
      log.debug("serverInvoke callbackHandler=" + this.getCallbackHandler());
      ConnectionFactoryAdvised advised =
         (ConnectionFactoryAdvised)Dispatcher.instance.getTarget(objectId);

      if (advised == null)
      {
         throw new IllegalStateException("Cannot find object in dispatcher with id " + objectId);
      }

      advised.addCallback(getClientVMID(), getRemotingSessionID(), this.getCallbackHandler());



      return null;
   }


   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      os.flush();
   }

   public void read(DataInputStream is) throws Exception
   {
      super.read(is);
   }
}
