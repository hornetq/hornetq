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

import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.transport.Connector;
import org.jboss.remoting.transport.PortUtil;
import org.jboss.remoting.transport.multiplex.MultiplexServerInvoker;
import org.jboss.util.id.GUID;


/**
 * 
 * Encapsulates the state and behaviour from jboss remoting needed for a 
 * JMS connection.
 * 
 * Each JMS connection maintains a single Client instance for invoking on the
 * server, and a Connector instance that represents the callback server used
 * to receive push callbacks from the server.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * JMSRemotingConnection.java,v 1.1 2006/01/23 11:05:19 timfox Exp
 */
public class JMSRemotingConnection
{
   private static final Logger log = Logger.getLogger(JMSRemotingConnection.class);
      
   protected Client client;
   
   protected Connector callbackServer;
   
   protected InvokerLocator serverLocator;
   
   protected String id;
   
   protected String thisAddress;
   
   protected int bindPort;
   
   protected boolean isMultiplex;
   
   public JMSRemotingConnection(String serverLocatorURI) throws Exception
   {
      id = new GUID().toString();
      
      thisAddress = InetAddress.getLocalHost().getHostAddress();
      
      bindPort = PortUtil.findFreePort("localhost");
      
      serverLocator = new InvokerLocator(serverLocatorURI);
      
      isMultiplex = serverLocator.getProtocol().equals("multiplex");
            
      if (log.isTraceEnabled()) { log.trace("Connecting with server URI:" + serverLocatorURI); }
            
      String callbackServerURI;
      
      if (isMultiplex)
      {
         callbackServerURI = "multiplex://" + thisAddress +
                              ":" + bindPort + "/?serverMultiplexId=" +
                              id;        
      }
      else
      {
         callbackServerURI = serverLocator.getProtocol() + "://" + thisAddress +
                             ":" + bindPort;
      }
      
      client = new Client(serverLocator, getConfig());
            
      if (log.isTraceEnabled()) { log.trace("Created client"); }
      
      InvokerLocator callbackServerLocator = new InvokerLocator(callbackServerURI);
        
      if (log.isTraceEnabled()) { log.trace("Starting callback server with uri:" 
            + callbackServerLocator.getLocatorURI()); }
            
      callbackServer = new Connector();
      
      callbackServer.setInvokerLocator(callbackServerLocator.getLocatorURI());
      
      callbackServer.create();
      
      callbackServer.start();
      
      if (log.isTraceEnabled()) { log.trace("Created callback server"); }
      
      client.connect();           
   }
   
   public void close()
   {
      callbackServer.stop();
      
      client.disconnect();
   }
   
   public Connector getCallbackServer()
   {
      return callbackServer;
   }
      
   public Client getInvokingClient()
   {
      return client;
   }
   
   /*
    * Register a callback handler to receive messages for a consumer
    */
   public Client registerCallbackHandler(InvokerCallbackHandler handler) throws Throwable
   {
      // We need to create new Client instance per consumer to handle the callbacks.
      // This is because we use the client session id on the server to associate the callback server
      // to the consumer.
      // Ideally remoting would allow us to pass arbitrary meta data to do this
      // more cleanly.
      
      Client client;
              
      client = new Client(serverLocator, getConfig());

      
      client.addListener(handler, callbackServer.getLocator());

      return client;
      
   }
   
   public String getId()
   {
      return id;
   }
   
   protected Map getConfig()
   {
      Map configuration = new HashMap();
      
      configuration.put(MetaDataConstants.CLIENT_CONNECTION_ID, id);
      
      if (isMultiplex)
      {     
         configuration.put(MultiplexServerInvoker.CLIENT_MULTIPLEX_ID_KEY, id);
         configuration.put(MultiplexServerInvoker.MULTIPLEX_BIND_HOST_KEY, thisAddress);
         configuration.put(MultiplexServerInvoker.MULTIPLEX_BIND_PORT_KEY, String.valueOf(bindPort));
      }
      
      return configuration;      
   }
  

}
