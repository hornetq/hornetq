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

import java.util.HashMap;
import java.util.Map;

import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.CallbackPoller;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.transport.bisocket.Bisocket;
import org.jboss.remoting.transport.socket.MicroSocketClientInvoker;
import org.jboss.remoting.transport.socket.SocketServerInvoker;
import org.jboss.util.id.GUID;


/**
 * Encapsulates the state and behaviour from jboss remoting needed for a JMS connection.
 * 
 * Each JMS connection maintains a single Client instance for invoking on the server.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class JMSRemotingConnection
{
   // Constants ------------------------------------------------------------------------------------

   public static final String CALLBACK_POLL_PERIOD_DEFAULT = "100";

   private static final Logger log = Logger.getLogger(JMSRemotingConnection.class);
   
   // Static ---------------------------------------------------------------------------------------

   private static String getPropertySafely(String propName)
   {
      String prop = null;

      try
      {
         prop = System.getProperty(propName);
      }
      catch (Exception ignore)
      {
         // May get a security exception depending on security permissions, in which case we
         // return null
      }

      return prop;
   }

   /**
    * Build the configuration we need to use to make sure a callback server works the way we want.
    *
    * @param doPushCallbacks - whether the callback should be push or pull. For socket transport
    *        allow true push callbacks, with callback Connector. For http transport, simulate push
    *        callbacks.
    * @param metadata - metadata that should be added to the metadata map being created. Can be
    *        null.
    *
    * @return a Map to be used when adding a listener to a client, thus enabling a callback server.
    */
   public static Map createCallbackMetadata(boolean doPushCallbacks, Map metadata,
                                            InvokerLocator serverLocator)
   {
      if (metadata == null)
      {
         metadata = new HashMap();
      }

      // Use our own direct thread pool that basically does nothing.
      // Note! This needs to be done irrespective of the transport and is used even for INVM
      //       invocations.
      metadata.put(ServerInvoker.ONEWAY_THREAD_POOL_CLASS_KEY,
                   "org.jboss.jms.server.remoting.DirectThreadPool");

      if (doPushCallbacks)
      {
         metadata.put(MicroSocketClientInvoker.CLIENT_SOCKET_CLASS_FLAG,
                      "org.jboss.jms.client.remoting.ClientSocketWrapper");
         metadata.put(SocketServerInvoker.SERVER_SOCKET_CLASS_FLAG,
                      "org.jboss.jms.server.remoting.ServerSocketWrapper");

         String bindAddress = getPropertySafely("jboss.messaging.callback.bind.address");
                  
         if (bindAddress != null)
         {
            metadata.put(Client.CALLBACK_SERVER_HOST, bindAddress);
         }

         String propertyPort = getPropertySafely("jboss.messaging.callback.bind.port");
                  
         if (propertyPort != null)
         {
            metadata.put(Client.CALLBACK_SERVER_PORT, propertyPort);
         }
         
         String protocol = serverLocator.getProtocol();
         if ("bisocket".equals(protocol) || "sslbisocket".equals(protocol))
         {
            metadata.put(Bisocket.IS_CALLBACK_SERVER, "true");

            // Setting the port prevents the Remoting Client from using PortUtil.findPort(), which
            // creates ServerSockets. The actual value of the port shouldn't matter. To "guarantee"
            // that each InvokerLocator is unique, a GUID is appended to the InvokerLocator.
            if (propertyPort == null)
            {
               String guid = new GUID().toString();
               int hash = guid.hashCode();
               
               // Make sure the hash code is > 0.
               // See http://jira.jboss.org/jira/browse/JBMESSAGING-863.
               while(hash <= 0)
               {
                  if (hash == 0)
                  {
                     guid = new GUID().toString();
                     hash = guid.hashCode();
                  }
                  if (hash < 0)
                  {
                     if (hash == Integer.MIN_VALUE)
                     {
                        hash = Integer.MAX_VALUE;
                     }
                     else
                     {
                        hash = -hash;
                     }
                  }
               }
               
               metadata.put(Client.CALLBACK_SERVER_PORT, Integer.toString(hash));
               metadata.put("guid", guid);
            }
         }
      }
      else
      {
         // "jboss.messaging.callback.pollPeriod" system property, if set, has the
         // highest priority ...
         String callbackPollPeriod = getPropertySafely("jboss.messaging.callback.pollPeriod");

         if (callbackPollPeriod == null)
         {
            // followed by the value configured on the HTTP connector ("callbackPollPeriod") ...
            callbackPollPeriod = (String)serverLocator.getParameters().get("callbackPollPeriod");
            if (callbackPollPeriod == null)
            {
               // followed by the hardcoded value.
               callbackPollPeriod = CALLBACK_POLL_PERIOD_DEFAULT;
            }
         }

         metadata.put(CallbackPoller.CALLBACK_POLL_PERIOD, callbackPollPeriod);

         String reportPollingStatistics =
            getPropertySafely("jboss.messaging.callback.reportPollingStatistics");

         if (reportPollingStatistics != null)
         {
            metadata.put(CallbackPoller.REPORT_STATISTICS, reportPollingStatistics);
         }
      }

      return metadata;
   }

   /**
    * Configures and add the invokerCallbackHandler the right way (push or pull).
    *
    * @param configurer - passed for logging purposes only.
    * @param initialMetadata - some initial metadata that we might want to pass along when
    *        registering invoker callback handler.
    */
   public static void addInvokerCallbackHandler(Object configurer,
                                                Client client,
                                                Map initialMetadata,
                                                InvokerLocator serverLocator,
                                                InvokerCallbackHandler invokerCallbackHandler)
      throws Throwable
   {

      // For transports derived from the socket transport, allow true push callbacks,
      // with callback Connector. For http transport, simulate push callbacks.
      String protocol = serverLocator.getProtocol();
      boolean isBisocket = "bisocket".equals(protocol) || "sslbisocket".equals(protocol);
      boolean isSocket   = "socket".equals(protocol)   || "sslsocket".equals(protocol);
      boolean doPushCallbacks = isBisocket || isSocket;
      Map metadata = createCallbackMetadata(doPushCallbacks, initialMetadata, serverLocator);

      if (doPushCallbacks)
      {
         log.debug(configurer + " is doing push callbacks");
         client.addListener(invokerCallbackHandler, metadata, null, true);
      }
      else
      {
         log.debug(configurer + " is simulating push callbacks");
         client.addListener(invokerCallbackHandler, metadata);
      }
   }

   // Attributes -----------------------------------------------------------------------------------

   private Client client;
   private boolean clientPing;
   private InvokerLocator serverLocator;
   private CallbackManager callbackManager;

   // When a failover is performed, this flag is set to true
   protected boolean failed = false;

   // Maintaining a reference to the remoting connection listener for cases when we need to
   // explicitly remove it from the remoting client
   private ConsolidatedRemotingConnectionListener remotingConnectionListener;

   // Constructors ---------------------------------------------------------------------------------

   public JMSRemotingConnection(String serverLocatorURI, boolean clientPing) throws Throwable
   {
      serverLocator = new InvokerLocator(serverLocatorURI);
      this.clientPing = clientPing;

      log.debug(this + " created");
   }

   // Public ---------------------------------------------------------------------------------------

   public void start() throws Throwable
   {
      // Enable client pinging. Server leasing is enabled separately on the server side.

      Map config = new HashMap();
      
      config.put(Client.ENABLE_LEASE, String.valueOf(clientPing));

      client = new Client(serverLocator, config);

      client.setSubsystem(ServerPeer.REMOTING_JMS_SUBSYSTEM);

      if (log.isTraceEnabled()) { log.trace(this + " created client"); }

      callbackManager = new CallbackManager();
      client.connect();

      // We explicitly set the Marshaller since otherwise remoting tries to resolve the marshaller
      // every time which is very slow - see org.jboss.remoting.transport.socket.ProcessInvocation
      // This can make a massive difference on performance. We also do this in
      // ServerConnectionEndpoint.setCallbackClient.

      client.setMarshaller(new JMSWireFormat());
      client.setUnMarshaller(new JMSWireFormat());

      Map metadata = new HashMap();

      metadata.put(InvokerLocator.DATATYPE, "jms");
      // Not actually used at present - but it does no harm
      metadata.put(InvokerLocator.SERIALIZATIONTYPE, "jms");

      addInvokerCallbackHandler(this, client, metadata, serverLocator, callbackManager);
      
      log.debug(this + " started");
   }

   public void stop() throws Throwable
   {
      log.debug(this + " closing");

      // explicitly remove the callback listener, to avoid race conditions on server
      // (http://jira.jboss.org/jira/browse/JBMESSAGING-535)

      client.removeListener(callbackManager);
      client.disconnect();

      client = null;
      log.debug(this + " closed");
   }

   public Client getRemotingClient()
   {
      return client;
   }

   public CallbackManager getCallbackManager()
   {
      return callbackManager;
   }

   public synchronized boolean isFailed()
   {
      return failed;
   }

   /**
    * Used by the FailoverCommandCenter to mark this remoting connection as "condemned", following
    * a failure detected by either a failed invocation, or the ConnectionListener.
    */
   public synchronized void setFailed()
   {
      failed = true;

      // Remoting has the bad habit of letting the job of cleaning after a failed connection up to
      // the application. Here, we take care of that, by disconnecting the remoting client, and
      // thus silencing both the connection validator and the lease pinger, and also locally
      // cleaning up the callback listener

      client.setDisconnectTimeout(0);
      
      client.disconnect();

      try
      {
         client.removeListener(callbackManager);
      }
      catch(Throwable t)
      {
         // very unlikely to get an exception on a local remove (I suspect badly designed API),
         // but we're failed anyway, so we don't care too much
         
         // Actually an exception will always be thrown here if the failure was detected by the connection
         // validator since the validator will disconnect the client before calling the connection
         // listener.

         log.debug(this + " failed to cleanly remove callback manager from the client", t);
      }

      client.disconnect();
   }

   /**
    * @return true if the listener was correctly installed, or false if the add attepmt was ignored
    *         because there is already another listener installed.
    */
   public synchronized boolean addConnectionListener(ConsolidatedRemotingConnectionListener listener)
   {
      if (remotingConnectionListener != null)
      {
         return false;
      }

      client.addConnectionListener(listener);
      remotingConnectionListener = listener;

      return true;
   }

   public synchronized ConsolidatedRemotingConnectionListener getConnectionListener()
   {
      return remotingConnectionListener;
   }

   /**
    * May return null, if no connection listener was previously installed.
    */
   public synchronized ConsolidatedRemotingConnectionListener removeConnectionListener()
   {
      if (remotingConnectionListener == null)
      {
         return null;
      }

      client.removeConnectionListener(remotingConnectionListener);

      log.debug(this + " removed consolidated connection listener from " + client);
      ConsolidatedRemotingConnectionListener toReturn = remotingConnectionListener;
      remotingConnectionListener = null;
      return toReturn;
   }

   public String toString()
   {
      return "JMSRemotingConnection[" + serverLocator.getLocatorURI() + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------

}
