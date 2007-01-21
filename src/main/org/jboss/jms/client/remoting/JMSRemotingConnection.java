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
import org.jboss.remoting.callback.CallbackPoller;
import org.jboss.remoting.transport.socket.MicroSocketClientInvoker;
import org.jboss.remoting.transport.socket.SocketServerInvoker;


/**
 * Encapsulates the state and behaviour from jboss remoting needed for a JMS connection.
 * 
 * Each JMS connection maintains a single Client instance for invoking on the server.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSRemotingConnection
{
   // Constants ------------------------------------------------------------------------------------

   public static final String CALLBACK_POLL_PERIOD_DEFAULT = "100";

   private static final Logger log = Logger.getLogger(JMSRemotingConnection.class);

   // Static ---------------------------------------------------------------------------------------

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
      // Enable client pinging. Server leasing is enabled separately on the server side

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

      // For socket transport allow true push callbacks, with callback Connector.
      // For http transport, simulate push callbacks.
      boolean doPushCallbacks = "socket".equals(serverLocator.getProtocol());

      if (doPushCallbacks)
      {
         if (log.isTraceEnabled()) log.trace("doing push callbacks");
         HashMap metadata = new HashMap();
         metadata.put(InvokerLocator.DATATYPE, "jms");
         // Not actually used at present - but it does no harm
         metadata.put(InvokerLocator.SERIALIZATIONTYPE, "jms");
         metadata.put(MicroSocketClientInvoker.CLIENT_SOCKET_CLASS_FLAG,
                      "org.jboss.jms.client.remoting.ClientSocketWrapper");
         metadata.put(SocketServerInvoker.SERVER_SOCKET_CLASS_FLAG,
                      "org.jboss.jms.server.remoting.ServerSocketWrapper");
         
         String bindAddress = System.getProperty("jboss.messaging.callback.bind.address");
         if (bindAddress != null)
         {
            metadata.put(Client.CALLBACK_SERVER_HOST, bindAddress);
         }

         String propertyPort = System.getProperty("jboss.messaging.callback.bind.port");
         if (propertyPort != null)
         {
            metadata.put(Client.CALLBACK_SERVER_PORT, propertyPort);
         }

         client.addListener(callbackManager, metadata, null, true);
      }
      else
      {
         if (log.isTraceEnabled()) log.trace("simulating push callbacks");

         HashMap metadata = new HashMap();

         // "jboss.messaging.callback.pollPeriod" system property, if set, has the highest priority ...
         String callbackPollPeriod = System.getProperty("jboss.messaging.callback.pollPeriod");
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
            System.getProperty("jboss.messaging.callback.reportPollingStatistics");

         if (reportPollingStatistics != null)
         {
            metadata.put(CallbackPoller.REPORT_STATISTICS, reportPollingStatistics);
         }

         client.addListener(callbackManager, metadata);
      }

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

   public boolean isFailed()
   {
      return failed;
   }

   /**
    * Used by the FailoverCommandCenter to mark this remoting connection as "condemned", following
    * a failure detected by either a failed invocation, or the ConnectionListener.
    */
   public void setFailed()
   {
      failed = true;

      // Remoting has the bad habit of letting the job of cleaning after a failed connection up to
      // the application. Here, we take care of that, by disconnecting the remoting client, and
      // thus silencing both the connection validator and the lease pinger, and also locally
      // cleaning up the callback listener

      try
      {
         client.removeListenerLocal(callbackManager);
      }
      catch(Throwable t)
      {
         // very unlikely to get an exception on a local remove (I suspect badly designed API),
         // but we're failed anyway, so we don't care too much
         log.debug(this + " failed to cleanly remove callback manager from the client", t);
      }

      client.disconnectLocal();

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
