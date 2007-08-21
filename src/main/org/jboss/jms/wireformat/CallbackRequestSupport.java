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

import org.jboss.remoting.callback.ServerInvokerCallbackHandler;
import java.io.DataOutputStream;
import java.io.DataInputStream;

/**
 * Support of establishing server2client callback mechanism.
 *
 * (JMSServerInvocationHandler looks up for the callbackHandler based on the remoteSessionId.
 *  That routine used to be dependent on ConnectionFactoryCreateConnectionDelegateRequest
 *  but we also needed the same thing to establish callback on ConnectionFactory updates,
 *  so we created another level for RequestSupport having the callback information)
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public abstract class CallbackRequestSupport extends RequestSupport
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private String remotingSessionId;

   private transient ServerInvokerCallbackHandler callbackHandler;

   private String clientVMId;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   protected CallbackRequestSupport()
   {
   }

   protected CallbackRequestSupport(String clientVMId, String remotingSessionId, String objectId, int methodId, byte version)
   {
      super(objectId, methodId, version);
      this.remotingSessionId = remotingSessionId;
      this.clientVMId = clientVMId;
   }

   // Public ---------------------------------------------------------------------------------------

   public String getRemotingSessionID()
   {
      return remotingSessionId;
   }


   public String getClientVMID()
   {
      return clientVMId;
   }

   public void setRemotingSessionId(String remotingSessionId)
   {
      this.remotingSessionId = remotingSessionId;
   }

   public ServerInvokerCallbackHandler getCallbackHandler()
   {
      return callbackHandler;
   }

   public void setCallbackHandler(ServerInvokerCallbackHandler callbackHandler)
   {
      this.callbackHandler = callbackHandler;
   }

   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);

      os.writeUTF(remotingSessionId);

      os.writeUTF(clientVMId);
   }

   public void read(DataInputStream is) throws Exception
   {
      super.read(is);

      remotingSessionId = is.readUTF();

      clientVMId = is.readUTF();
      

   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
