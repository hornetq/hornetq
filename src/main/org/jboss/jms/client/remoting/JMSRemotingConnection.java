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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.transport.Connector;
import org.jboss.remoting.transport.PortUtil;
import org.jboss.remoting.transport.multiplex.MultiplexServerInvoker;
import org.jboss.util.id.GUID;


/**
 * Encapsulates the state and behaviour from jboss remoting needed for a JMS connection.
 * 
 * Each JMS connection maintains a single Client instance for invoking on the server, and a
 * Connector instance that represents the callback server used to receive push callbacks from the
 * server.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version 1.1
 *
 * JMSRemotingConnection.java,v 1.1 2006/01/23 11:05:19 timfox Exp
 */
public class JMSRemotingConnection
{
   // Constants -----------------------------------------------------

   public static final String JMS_CALLBACK_SUBSYSTEM = "CALLBACK";

   private static final Logger log = Logger.getLogger(JMSRemotingConnection.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Client client;
   protected Connector callbackServer;
   protected InvokerLocator serverLocator;
   protected String id;
   protected String thisAddress;
   protected int bindPort;
   protected boolean isMultiplex;
   protected CallbackManager callbackManager;
   protected InvokerCallbackHandler dummy;

   // Constructors --------------------------------------------------

   public JMSRemotingConnection(String serverLocatorURI) throws Throwable
   {
      callbackManager = new CallbackManager();

      id = new GUID().toString();

      thisAddress = InetAddress.getLocalHost().getHostAddress();

      serverLocator = new InvokerLocator(serverLocatorURI);

      isMultiplex = serverLocator.getProtocol().equals("multiplex");

      final int MAX_RETRIES = 50;

      boolean completed = false;

      int count = 0;

      while (!completed && count < MAX_RETRIES)
      {
         try
         {
            setUpConnection();

            completed = true;
         }
         catch (Exception e)
         {
            log.warn("Failed to start connection", e);

            // Intermittently we can fail to open a socket on the address since it's already in use
            // This is despite remoting having checked the port is free. This is either because the
            // remoting implementation is buggy or because of the small window between getting the
            // port number and actually opening the connection during which some one else can use
            // that port. Therefore we catch this and retry.

            count++;

            if (client != null)
            {
               client.disconnect();

               log.trace("Disconnected client");
            }
            if (callbackServer != null)
            {
               //Probably not necessary and may fail since it didn't get properly started
               try
               {
                  //callbackServer.removeInvocationHandler("Callback");

                  //log.trace("Removed invocation handler");

                  callbackServer.stop();

                  log.trace("Stopped callback server");

                  callbackServer.destroy();

                  log.trace("Destroyed callback server");

                  callbackServer = null;
               }
               catch (Exception ignore)
               {
                  //Ignore - it may well fail - this is to be expected
                  log.warn("Failed to shutdown callback server", ignore);
               }
            }
            if (count == MAX_RETRIES)
            {
               final String msg = "Cannot start callbackserver after " + MAX_RETRIES + " retries";
               log.error(msg, e);
               throw new JBossJMSException(msg, e);
            }
         }
      }
   }

   // Public --------------------------------------------------------

   public void close() throws Throwable
   {
      callbackServer.stop();

      callbackServer.destroy();

      client.disconnect();
   }

   public Client getInvokingClient()
   {
      return client;
   }

   public CallbackManager getCallbackManager()
   {
      return callbackManager;
   }

   public String getId()
   {
      return id;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected Map getConfig()
   {
      Map configuration = new HashMap();

      configuration.put(Client.ENABLE_LEASE, "true");

      if (isMultiplex)
      {
         configuration.put(MultiplexServerInvoker.CLIENT_MULTIPLEX_ID_KEY, id);
         configuration.put(MultiplexServerInvoker.MULTIPLEX_BIND_HOST_KEY, thisAddress);
         configuration.put(MultiplexServerInvoker.MULTIPLEX_BIND_PORT_KEY, String.valueOf(bindPort));
      }

      return configuration;
   }

   protected void setUpConnection() throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Connecting with server URI:" + serverLocator); }

      String params = "/?marshaller=org.jboss.jms.server.remoting.JMSWireFormat&" +
                      "unmarshaller=org.jboss.jms.server.remoting.JMSWireFormat&" +
                      "serializationtype=jboss&" +
                      "dataType=jms&" +
                      "socketTimeout=0&" +
                      "socket.check_connection=false";

      client = new Client(serverLocator, getConfig());

      //We explictly set the Marshaller since otherwise remoting tries to resolve the marshaller every time
      //which is very slow - see org.jboss.remoting.transport.socket.ProcessInvocation
      //This can make a massive difference on performance
      //We also do this in ServerConnectionEndpoint.setCallbackClient.
      client.setMarshaller(new JMSWireFormat());
      client.setUnMarshaller(new JMSWireFormat());
      client.setSubsystem(ServerPeer.REMOTING_JMS_SUBSYSTEM);

      if (log.isTraceEnabled()) { log.trace("Created client"); }

      bindPort = PortUtil.findFreePort("localhost");

      //Create callback server

      String callbackServerURI;

      if (isMultiplex)
      {
         callbackServerURI = "multiplex://" + thisAddress +
                              ":" + bindPort + params + "&serverMultiplexId=" +
                              id;
      }
      else
      {
         callbackServerURI = serverLocator.getProtocol() + "://" + thisAddress +
                             ":" + bindPort + params;
      }

      InvokerLocator callbackServerLocator = new InvokerLocator(callbackServerURI);

      if (log.isTraceEnabled()) { log.trace("Starting callback server with uri:"
            + callbackServerLocator.getLocatorURI()); }

      callbackServer = new Connector();

      callbackServer.setInvokerLocator(callbackServerLocator.getLocatorURI());

      callbackServer.create();

      callbackServer.addInvocationHandler(JMS_CALLBACK_SUBSYSTEM, callbackManager);

      callbackServer.start();

      if (log.isTraceEnabled()) { log.trace("Created callback server"); }

      client.connect();

      dummy = new DummyCallbackHandler();
      
      client.addListener(dummy, callbackServerLocator);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
