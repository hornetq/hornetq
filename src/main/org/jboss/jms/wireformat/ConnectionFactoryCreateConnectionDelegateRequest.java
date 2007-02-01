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

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.jms.server.endpoint.CreateConnectionResult;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryEndpoint;
import org.jboss.jms.server.endpoint.advised.ConnectionFactoryAdvised;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;

/**
 * A ConnectionFactoryCreateConnectionDelegateRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ConnectionFactoryCreateConnectionDelegateRequest extends RequestSupport
{
   private String username;
   
   private String password;
   
   private int failedNodeId;
   
   private String remotingSessionId;
   
   private String clientVMId;
   
   private transient ServerInvokerCallbackHandler callbackHandler;
   
   public ConnectionFactoryCreateConnectionDelegateRequest()
   {      
   }

   public ConnectionFactoryCreateConnectionDelegateRequest(int objectId,
                                                           byte version,
                                                           String remotingSessionId,
                                                           String clientVMId,
                                                           String username, String password,
                                                           int failedNodeId)
   {
      super(objectId, PacketSupport.REQ_CONNECTIONFACTORY_CREATECONNECTIONDELEGATE, version);
      
      this.remotingSessionId = remotingSessionId;
      
      this.clientVMId = clientVMId;
      
      this.username = username;
      
      this.password = password;
      
      this.failedNodeId = failedNodeId;
   }

   public void read(DataInputStream is) throws Exception
   {
      super.read(is);
      
      remotingSessionId = is.readUTF();
      
      clientVMId = is.readUTF();
      
      username = readNullableString(is);
      
      password = readNullableString(is);
      
      failedNodeId = is.readInt();
   }

   public ResponseSupport serverInvoke() throws Exception
   {         
      ConnectionFactoryAdvised advised =
         (ConnectionFactoryAdvised)Dispatcher.instance.getTarget(objectId);
      
      if (advised == null)
      {
         throw new IllegalStateException("Cannot find object in dispatcher with id " + objectId);
      }
      
      ServerConnectionFactoryEndpoint endpoint =
         (ServerConnectionFactoryEndpoint)advised.getEndpoint();
                     
      CreateConnectionResult del = 
         endpoint.createConnectionDelegate(username, password, failedNodeId,
                                           remotingSessionId, clientVMId, version,
                                           callbackHandler);
      
      return new ConnectionFactoryCreateConnectionDelegateResponse(del);
   }

   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      
      os.writeUTF(remotingSessionId);
      
      os.writeUTF(clientVMId);
      
      //Write the args
           
      writeNullableString(username, os);
      
      writeNullableString(password, os);
      
      os.writeInt(failedNodeId); 
      
      os.flush();
   }
   
   public String getRemotingSessionID()
   {
      return remotingSessionId;
   }
   
   public String getClientVMID()
   {
      return clientVMId;
   }
   
   public ServerInvokerCallbackHandler getCallbackHandler()
   {
      return this.callbackHandler;
   }
   
   public void setCallbackHandler(ServerInvokerCallbackHandler handler)
   {
      this.callbackHandler = handler;
   }

}
