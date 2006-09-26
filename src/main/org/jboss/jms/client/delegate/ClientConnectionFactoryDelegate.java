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

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;

import org.jboss.aop.Dispatcher;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.client.container.JMSClientVMIdentifier;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.Version;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.server.remoting.MessagingMarshallable;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.IdBlock;
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
   extends DelegateSupport implements ConnectionFactoryDelegate
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   private static final Logger log = Logger.getLogger(ClientConnectionFactoryDelegate.class);

   // Attributes ----------------------------------------------------

   protected String serverLocatorURI;
   protected Version serverVersion;
   protected String serverID;
   protected boolean clientPing;

   private boolean trace;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClientConnectionFactoryDelegate(int objectID, String serverLocatorURI,
                                          Version serverVersion, String serverID,
                                          boolean clientPing)
   {
      super(objectID);

      this.serverLocatorURI = serverLocatorURI;
      this.serverVersion = serverVersion;
      this.serverID = serverID;
      this.clientPing = clientPing;
      trace = log.isTraceEnabled();
   }

   // ConnectionFactoryDelegateImplementation -----------------------

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ConnectionDelegate createConnectionDelegate(String username, String password)
      throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public byte[] getClientAOPConfig()
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public IdBlock getIdBlock(int size)
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   // Public --------------------------------------------------------

   public synchronized Object invoke(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;
      String methodName = mi.getMethod().getName();

      if (trace) { log.trace("invoking " + methodName + " on server"); }

      SimpleMetaData md = mi.getMetaData();

      md.addMetaData(Dispatcher.DISPATCHER,
                     Dispatcher.OID,
                     new Integer(id),
                     PayloadKey.AS_IS);
      
      /*
       * If the method being invoked is createConnectionDelegate then we must invoke it on the same
       * remoting client subsequently used by the connection.
       * This is because we need to pass in the remoting session id in the call to createConnection
       * All other invocations can be invoked on an arbitrary client, which can be created for each invocation.
       * If we disable pinging on the client then it is a reasonably light weight operation to create the client
       * since it will use the already existing invoker.
       * This prevents us from having to maintain a Client instance per connection factory, which gives
       * difficulties in knowing when to close it.
       */

      Client client; 
      
      JMSRemotingConnection remotingConnection = null;

      if ("createConnectionDelegate".equals(methodName))
      {
         // Create a new connection
         
         remotingConnection = new JMSRemotingConnection(serverLocatorURI, clientPing);
         remotingConnection.start();
         
         client = remotingConnection.getInvokingClient();
         
         md.addMetaData(MetaDataConstants.JMS,
                        MetaDataConstants.REMOTING_SESSION_ID,
                        client.getSessionId(),
                        PayloadKey.AS_IS);

         md.addMetaData(MetaDataConstants.JMS,
                        MetaDataConstants.JMS_CLIENT_VM_ID,
                        JMSClientVMIdentifier.instance,
                        PayloadKey.AS_IS);
      }
      else
      {
         // Create a client - make sure pinging is off
         
         Map configuration = new HashMap();

         configuration.put(Client.ENABLE_LEASE, String.valueOf(false));
         
         log.info("*********** SERVERLOCATOR URI:" + serverLocatorURI);

         client = new Client(new InvokerLocator(serverLocatorURI), configuration);     
         
         client.setSubsystem(ServerPeer.REMOTING_JMS_SUBSYSTEM);
         
         client.connect();         
         
         client.setMarshaller(new JMSWireFormat());
         client.setUnMarshaller(new JMSWireFormat());         
      }

      byte v = getVersionToUse().getProviderIncrementingVersion();
      
      MessagingMarshallable request = new MessagingMarshallable(v, mi);
      
      MessagingMarshallable response;

      try
      {
         response = (MessagingMarshallable)client.invoke(request, null);
         
         if (trace) { log.trace("got server response for " + methodName); }
      }
      catch (Throwable t)
      {
         //If we were invoking createConnectionDelegate and failure occurs then we need to clear
         //up the JMSRemotingConnection
         
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
         
         throw t;
      }
      finally
      {
         if (remotingConnection == null)
         {
            //Not a call to createConnectionDelegate - disconnect the client
            
            // client.disconnect();
         }
      }

      Object ret = response.getLoad();
      
      if (remotingConnection != null)
      {
         // It was a call to createConnectionDelegate - set the remoting connection to use
         ClientConnectionDelegate connectionDelegate = (ClientConnectionDelegate)ret;
         
         connectionDelegate.setRemotingConnection(remotingConnection);
      }

      return ret;
   }

   public Version getServerVersion()
   {
      return serverVersion;
   }

   public String getServerID()
   {
      return serverID;
   }

   public Version getVersionToUse()
   {
      Version clientVersion = Version.instance();

      Version versionToUse;

      if (serverVersion.getProviderIncrementingVersion() <= clientVersion.getProviderIncrementingVersion())
      {
         versionToUse = serverVersion;
      }
      else
      {
         versionToUse = clientVersion;
      }

      return versionToUse;
   }

   public String getServerLocatorURI()
   {
      return serverLocatorURI;
   }

   public String toString()
   {
      return "ConnectionFactoryDelegate[" + id + "]";
   }

   // Protected -----------------------------------------------------

   protected Client getClient()
   {
      return null;
   }

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------

}
