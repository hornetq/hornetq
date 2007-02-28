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
package org.jboss.jms.client.delegate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;

import org.jboss.jms.client.container.JMSClientVMIdentifier;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.Version;
import org.jboss.jms.server.endpoint.CreateConnectionResult;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.util.MessagingNetworkFailureException;
import org.jboss.jms.wireformat.ConnectionFactoryCreateConnectionDelegateRequest;
import org.jboss.jms.wireformat.ConnectionFactoryGetClientAOPStackRequest;
import org.jboss.jms.wireformat.ResponseSupport;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;

/**
 * The client-side ConnectionFactory delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientConnectionFactoryDelegate
   extends DelegateSupport implements ConnectionFactoryDelegate, Serializable
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   // Attributes -----------------------------------------------------------------------------------

   //This data is needed in order to create a connection
   private String serverLocatorURI;

   private Version serverVersion;
 
   private int serverID;
   
   private boolean clientPing;
   
   // Static ---------------------------------------------------------------------------------------
   
   /*
    * Calculate what version to use.
    * The client itself has a version, but we also support other versions of servers lower if the
    * connection version is lower (backwards compatibility)
    */
   public static Version getVersionToUse(Version connectionVersion)
   {
      Version clientVersion = Version.instance();

      Version versionToUse;

      if (connectionVersion.getProviderIncrementingVersion() <=
          clientVersion.getProviderIncrementingVersion())
      {
         versionToUse = connectionVersion;
      }
      else
      {
         versionToUse = clientVersion;
      }

      return versionToUse;
   }

   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionFactoryDelegate(int objectID, int serverID, String serverLocatorURI,
                                          Version serverVersion, boolean clientPing)
   {
      super(objectID);
      
      this.serverID = serverID;
      this.serverLocatorURI = serverLocatorURI;
      this.serverVersion = serverVersion;
      this.clientPing = clientPing;
   }
   
   public ClientConnectionFactoryDelegate()
   {      
   }

   // ConnectionFactoryDelegate implementation -----------------------------------------------------
 
   public CreateConnectionResult createConnectionDelegate(String username,
                                                          String password,
                                                          int failedNodeID)
      throws JMSException
   {
      // If the method being invoked is createConnectionDelegate() then we must invoke it on the
      // same remoting client subsequently used by the connection. This is because we need to pass
      // in the remoting session id in the call to createConnection. All other invocations can be
      // invoked on an arbitrary client, which can be created for each invocation.
      //
      // If we disable pinging on the client then it is a reasonably light weight operation to
      // create the client since it will use the already existing invoker. This prevents us from
      // having to maintain a Client instance per connection factory, which gives difficulties in
      // knowing when to close it.
      
      Version version = getVersionToUse(serverVersion);
      
      byte v = version.getProviderIncrementingVersion();
                       
      JMSRemotingConnection remotingConnection = null;
      
      CreateConnectionResult res;
      
      try
      {         
         remotingConnection = new JMSRemotingConnection(serverLocatorURI, clientPing);
         
         remotingConnection.start();
   
         Client client = remotingConnection.getRemotingClient();
         
         String remotingSessionId = client.getSessionId();
         
         String clientVMId = JMSClientVMIdentifier.instance;
            
         ConnectionFactoryCreateConnectionDelegateRequest req = 
            new ConnectionFactoryCreateConnectionDelegateRequest(id, v,
                                                                 remotingSessionId, clientVMId,
                                                                 username, password, failedNodeID);
           
         ResponseSupport rs = (ResponseSupport)client.invoke(req, null);
         
         res = (CreateConnectionResult)rs.getResponse();
      }
      catch (Throwable t)
      {
         //If we were invoking createConnectionDelegate and failure occurs then we need to clear
         // up the JMSRemotingConnection

         if (remotingConnection != null)
         {
            try
            {
               remotingConnection.stop();
            }
            catch (Throwable ignore)
            {
            }
         }
         
         throw handleThrowable(t);
      }
         
      ClientConnectionDelegate connectionDelegate = (ClientConnectionDelegate)res.getDelegate();
      
      if (connectionDelegate != null)
      {
         connectionDelegate.setRemotingConnection(remotingConnection);
         
         connectionDelegate.setVersionToUse(version);

         remotingConnection.getCallbackManager().setConnectionDelegate(connectionDelegate);
      }
      else
      {
         //Wrong server redirect on failure
         //close the remoting connection
         try
         {
            remotingConnection.stop();
         }
         catch (Throwable ignore)
         {
         }
      }

      return res;
   }
   
   public byte[] getClientAOPStack() throws JMSException
   {
      Version version = getVersionToUse(serverVersion);
      
      byte v = version.getProviderIncrementingVersion();
      
      // Create a client - make sure pinging is off

      Map configuration = new HashMap();

      configuration.put(Client.ENABLE_LEASE, String.valueOf(false));

      //We execute this on its own client
      
      Client theClient = createClient();
      
      ConnectionFactoryGetClientAOPStackRequest req =
         new ConnectionFactoryGetClientAOPStackRequest(id, v);
      
      return (byte[])doInvoke(theClient, req); 
   }
      
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ConnectionFactoryDelegate[" + id + ", SID=" + serverID + "]";
   }
   
   public String getServerLocatorURI()
   {
      return serverLocatorURI;
   }

   public int getServerID()
   {
      return serverID;
   }
   
   public boolean getClientPing()
   {
      return clientPing;
   }
   
   public Version getServerVersion()
   {
      return serverVersion;
   }

   public void synchronizeWith(DelegateSupport newDelegate) throws Exception
   {
      super.synchronizeWith(newDelegate);
   }
   

   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private Client createClient() throws JMSException
   {
      // Create a client - make sure pinging is off

      Map configuration = new HashMap();

      configuration.put(Client.ENABLE_LEASE, String.valueOf(false));

      //We execute this on it's own client
      Client client;
      
      try
      {
         client = new Client(new InvokerLocator(serverLocatorURI), configuration);
         client.setSubsystem(ServerPeer.REMOTING_JMS_SUBSYSTEM);
         client.connect();
      }
      catch (Exception e)
      {
         throw new MessagingNetworkFailureException("Failed to connect client", e);
      }

      client.setMarshaller(new JMSWireFormat());
      client.setUnMarshaller(new JMSWireFormat());
      
      return client;
   }
   
   // Streamable implementation --------------------------------------------

   public void read(DataInputStream in) throws Exception
   {      
      super.read(in);
      
      serverLocatorURI = in.readUTF();
      
      serverVersion = new Version();
      
      serverVersion.read(in);
      
      serverID = in.readInt();
      
      clientPing = in.readBoolean();
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
      
      out.writeUTF(serverLocatorURI);
      
      serverVersion.write(out);
      
      out.writeInt(serverID);
      
      out.writeBoolean(clientPing);
   }

   // Inner Classes --------------------------------------------------------------------------------

}
