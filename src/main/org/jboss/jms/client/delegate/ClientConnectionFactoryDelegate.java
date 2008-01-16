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

import static org.jboss.messaging.core.remoting.ConnectorRegistrySingleton.REGISTRY;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

import javax.jms.JMSException;

import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.container.JMSClientVMIdentifier;
import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.CreateConnectionResult;
import org.jboss.jms.delegate.TopologyResult;
import org.jboss.jms.exception.MessagingNetworkFailureException;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.core.remoting.impl.ClientImpl;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionResponse;
import org.jboss.messaging.util.Version;

/**
 * The client-side ConnectionFactory delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientConnectionFactoryDelegate
   extends CommunicationSupport<ClientConnectionFactoryDelegate> implements ConnectionFactoryDelegate, Serializable
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   // Attributes -----------------------------------------------------------------------------------

   //This data is needed in order to create a connection

   private String uniqueName;

   private String serverLocatorURI;

   private Version serverVersion;
 
   private int serverID;
   
   private boolean clientPing;

   private boolean strictTck;
   
   // Static ---------------------------------------------------------------------------------------
   
   /*
    * Calculate what version to use.
    * The client itself has a version, but we also support other versions of servers lower if the
    * connection version is lower (backwards compatibility)
    */
   private static Version getVersionToUse(Version connectionVersion)
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

   public ClientConnectionFactoryDelegate(String uniqueName, String objectID, int serverID, 
         String serverLocatorURI, Version serverVersion, boolean clientPing, boolean strictTck)
   {
      super(objectID);

      this.uniqueName = uniqueName;
      this.serverID = serverID;
      this.serverLocatorURI = serverLocatorURI;
      this.serverVersion = serverVersion;
      this.clientPing = clientPing;
      this.strictTck = strictTck;
   }
   
   public ClientConnectionFactoryDelegate()
   {
   }
   
   protected Client getClient()
   {
      return null;
   }

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
                       
      CreateConnectionResult res;
      
      JMSRemotingConnection remotingConnection = null;
      try
      {
         remotingConnection = new JMSRemotingConnection(serverLocatorURI);
       
         remotingConnection.start();
         Client client = remotingConnection.getRemotingClient(); 
         String sessionID = client.getSessionID();
         
         CreateConnectionRequest request = new CreateConnectionRequest(v, sessionID, JMSClientVMIdentifier.instance, failedNodeID, username, password);
         CreateConnectionResponse response = (CreateConnectionResponse) sendBlocking(client, this.getID(), this.getVersion(), request);
         ClientConnectionDelegate connectionDelegate = new ClientConnectionDelegate(response.getConnectionID(), response.getServerID());
         connectionDelegate.setStrictTck(strictTck);

         connectionDelegate.setVersionToUse(version);
         connectionDelegate.setResourceManager(ResourceManagerFactory.instance.checkOutResourceManager(connectionDelegate.getServerID()));

         ConsolidatedRemotingConnectionListener listener =
            new ConsolidatedRemotingConnectionListener(connectionDelegate);

         if (remotingConnection!=null)remotingConnection.addConnectionListener(listener);
         
         res = new CreateConnectionResult(connectionDelegate);
      } catch (Throwable t)
      {
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
         
      ClientConnection connectionDelegate = res.getInternalDelegate();
      
      if (connectionDelegate != null)
      {
         connectionDelegate.setRemotingConnection(remotingConnection);
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
   
   public TopologyResult getTopology() throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
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
   
   public String getName()
   {
      return uniqueName;
   }


   public boolean getStrictTck()
   {
       return strictTck;
   }

    public void synchronizeWith(ClientConnectionFactoryDelegate newDelegate) throws Exception
   {
      super.synchronizeWith(newDelegate);
   }

   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private Client createClient() throws JMSException
   {
      //We execute this on it's own client
      Client client;
      
      try
      {
         ServerLocator locator = new ServerLocator(serverLocatorURI);
         NIOConnector connector = REGISTRY.getConnector(locator);
         client = new ClientImpl(connector, locator);
         client.connect();
      }
      catch (Exception e)
      {
         throw new MessagingNetworkFailureException("Failed to connect client", e);
      }
      
      return client;
   }
   
   // Streamable implementation --------------------------------------------

   public void read(DataInputStream in) throws Exception
   {      
      super.read(in);
      
      uniqueName = in.readUTF();
      
      serverLocatorURI = in.readUTF();
      
      serverVersion = new Version();
      
      serverVersion.read(in);
      
      serverID = in.readInt();
      
      clientPing = in.readBoolean();

      strictTck = in.readBoolean();
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
      
      out.writeUTF(uniqueName);
      
      out.writeUTF(serverLocatorURI);
      
      serverVersion.write(out);
      
      out.writeInt(serverID);
      
      out.writeBoolean(clientPing);

      out.writeBoolean(strictTck);
   }

   // Inner Classes --------------------------------------------------------------------------------

}
