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
import org.jboss.remoting.transport.Connector;


/**
 * Encapsulates the state and behaviour from jboss remoting needed for a JMS connection.
 * 
 * Each JMS connection maintains a single Client instance for invoking on the server, and a
 * Connector instance that represents the callback server used to receive push callbacks from the
 * server.
 * Only Connector is maintained per protocol
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

   private static final Logger log = Logger.getLogger(JMSRemotingConnection.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Client client;
   protected Connector callbackServer;
   protected InvokerLocator serverLocator;
   protected CallbackManager callbackManager;

   // Constructors --------------------------------------------------

   public JMSRemotingConnection(String serverLocatorURI, boolean clientPing) throws Throwable
   { 
      serverLocator = new InvokerLocator(serverLocatorURI);
            
      // Enable client pinging
      // Server leasing is enabled separately on the server side
      Map config = new HashMap();

      config.put(Client.ENABLE_LEASE, String.valueOf(clientPing));

      client = new Client(serverLocator, config);
      
      client.setSubsystem(ServerPeer.REMOTING_JMS_SUBSYSTEM);

      if (log.isTraceEnabled()) { log.trace("created client"); }
            
      // Get the callback server
      
      callbackServer = CallbackServerFactory.instance.getCallbackServer(serverLocator);
      
      callbackManager = (CallbackManager)callbackServer.getInvocationHandlers()[0];
               
      client.connect();
      
      // We explictly set the Marshaller since otherwise remoting tries to resolve the marshaller
      // every time which is very slow - see org.jboss.remoting.transport.socket.ProcessInvocation
      // This can make a massive difference on performance. We also do this in
      // ServerConnectionEndpoint.setCallbackClient.

      client.setMarshaller(new JMSWireFormat());
      client.setUnMarshaller(new JMSWireFormat());
      
      client.addListener(new DummyCallbackHandler(), callbackServer.getLocator());

      log.debug(this + " created");
   }

   // Public --------------------------------------------------------

   public void close() throws Throwable
   {
      log.debug(this + " closing");
      
      CallbackServerFactory.instance.returnCallbackServer(serverLocator.getProtocol());
      
      client.disconnect();
      
      log.debug(this + " closed");      
   }

   public Client getInvokingClient()
   {
      return client;
   }

   public CallbackManager getCallbackManager()
   {
      return callbackManager;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
