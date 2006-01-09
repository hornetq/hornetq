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
package org.jboss.jms.client.state;

import java.net.Socket;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;
import org.jboss.remoting.transport.PortUtil;
import org.jboss.util.id.GUID;

import EDU.oswego.cs.dl.util.concurrent.Executor;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import EDU.oswego.cs.dl.util.concurrent.SyncSet;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

/**
 * 
 * State corresponding to a connection. This state is acessible inside aspects/interceptors.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionState extends HierarchicalStateBase
{
   private static final Logger log = Logger.getLogger(ConnectionState.class);
   
   private Client client;
   
   private ResourceManager resourceManager;
   
   private Connector callbackServer;
   
   private String serverURI;
   
   //Thread pool used for making asynch calls to server - e.g. activateConsumer
   private PooledExecutor pooledExecutor;
   
   public ConnectionState(ConnectionDelegate delegate, String serverId, String serverLocatorURI)
      throws Exception
   {
      super(null, delegate);
      
      if (log.isTraceEnabled()) { log.trace("Creating connection state"); }
      
      children = new SyncSet(new HashSet(), new WriterPreferenceReadWriteLock());
      
      resourceManager = ResourceManagerFactory.instance.getResourceManager(serverId);
      
      //TODO size should be configurable
      pooledExecutor = new PooledExecutor(new LinkedQueue(), 50);
      pooledExecutor.setMinimumPoolSize(50);
      
      
      //NOTE
      //Multiplex code is commented out and we have reverted to socket transport until
      //it is fixed
      
      
            
//      String guid = new GUID().toString();
//      
//      int bindPort;
//      
//      synchronized (PortUtil.class)
//      {
//         bindPort = PortUtil.findFreePort();
//      }
      
//      String callbackServerURI = "multiplex://0.0.0.0:" + bindPort + "/?serverMultiplexId=" + guid;
      
      String callbackServerURI = "socket://0.0.0.0:0";
            
      InvokerLocator callbackServerLocator = new InvokerLocator(callbackServerURI);
      
      //Create a Client for invoking on the server
      //The same Client is used for all server invocations for all "child objects" of the connection
      
      //String bindHost = callbackServerLocator.getHost();

//      InvokerLocator dummy = new InvokerLocator(serverLocatorURI);
//      String serverHost = dummy.getHost();
//      int serverPort = dummy.getPort();
//      Map serverParams = dummy.getParameters();
//      String protocol = dummy.getProtocol();
//      
//      serverURI = protocol + "://" + serverHost + ":" + serverPort +
//         "/?bindHost=" + bindHost + "&bindPort=" + bindPort + "&clientMultiplexId=" + guid;
//      
//      if (serverParams != null && !serverParams.isEmpty())
//      {
//         Iterator iter = serverParams.entrySet().iterator();
//         while (iter.hasNext())
//         {
//            Map.Entry entry = (Map.Entry)iter.next();
//            String key = (String)entry.getKey();
//            String value = (String)entry.getValue();
//            serverURI += "&" + key + "=" + value;
//         }
//      }
//                                 
//      InvokerLocator serverLocator =  new InvokerLocator(serverURI);            
//      
      serverURI = serverLocatorURI;
      InvokerLocator serverLocator = new InvokerLocator(serverLocatorURI);
      
      if (log.isTraceEnabled()) { log.trace("Connecting with server URI:" + serverURI); }
            
      client = new Client(serverLocator);

      if (log.isTraceEnabled()) { log.trace("Created client"); }
      
      //We create a "virtual" callback server on the client.
      //This doesn't actually create a "real" server listening with any
      //real server sockets.
                 
      callbackServer = new Connector();      
       
      if (log.isTraceEnabled()) { log.trace("Starting virtual callback server with uri:" 
            + callbackServerLocator.getLocatorURI()); }
            
      callbackServer.setInvokerLocator(callbackServerLocator.getLocatorURI());
      
      callbackServer.create();
      
      callbackServer.start();
      
      if (log.isTraceEnabled()) { log.trace("Created callback server"); }
      
      client.connect();               
      
   }
    
   public ResourceManager getResourceManager()
   {
      return resourceManager;
   }
   
   public Executor getPooledExecutor()
   {
      return pooledExecutor;
   }
   
   public Client getClient()
   {
      return client;
   }
   
   public Connector getCallbackServer()
   {
      return callbackServer;
   }
   
   public String getServerURI()
   {
      return serverURI;
   }
      
   
}
