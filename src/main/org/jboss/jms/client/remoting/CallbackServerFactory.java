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

import org.jboss.jms.util.MessagingJMSException;
import org.jboss.logging.Logger;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.security.SSLSocketBuilder;
import org.jboss.remoting.transport.Connector;
import org.jboss.remoting.transport.PortUtil;

/**
 * 
 * A CallbackServerFactory.
 * 
 * We maintain only one callbackserver per transport per client VM.
 * This is to avoid having too many resources e.g. server sockets in use on the client
 * E.g. in the case of a socket transport, if we had one callbackserver per connection then
 * we would have one server socket listening per connection, so we could run out of available
 * ports with a lot of connections.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class CallbackServerFactory
{      
   private static final Logger log = Logger.getLogger(CallbackServerFactory.class);
   
   private static final String CALLBACK_SERVER_PARAMS =
      "/?marshaller=org.jboss.jms.server.remoting.JMSWireFormat&" +
      "unmarshaller=org.jboss.jms.server.remoting.JMSWireFormat&" +
      "dataType=jms&" +
      "timeout=0&" +
      "socket.check_connection=false";
   
   public static final String JMS_CALLBACK_SUBSYSTEM = "CALLBACK";
   
   public static final String CLIENT_HOST = System.getProperty("jboss.messaging.callback.bind.address");
   
   public static CallbackServerFactory instance = new CallbackServerFactory();
   
   private Map holders;
   
   private CallbackServerFactory()
   {      
      holders = new HashMap();
   }
      
   public synchronized boolean containsCallbackServer(String protocol)
   {
      return holders.containsKey(protocol);
   }
   
   public synchronized Connector getCallbackServer(InvokerLocator serverLocator) throws Exception
   {
      String protocol = serverLocator.getProtocol();
      
      Holder h = (Holder)holders.get(protocol);           
      
      if (h == null)
      {         
         h = new Holder();
         
         h.server = startCallbackServer(serverLocator);
         
         holders.put(protocol, h);
      }
      else
      {
         h.refCount++;
      }
      
      return h.server;
   }
   
   public synchronized void stopCallbackServer(String protocol)
   {
      Holder h = (Holder)holders.get(protocol);
      
      if (h == null)
      {
         throw new IllegalArgumentException("Cannot find callback server for protocol: " + protocol);
      }
      
      h.refCount--;
      
      if (h.refCount == 0)
      {
         stopCallbackServer(h.server);
         
         holders.remove(protocol);
      }      
   }
   
   protected Connector startCallbackServer(InvokerLocator serverLocator) throws Exception
   {
      if (log.isTraceEnabled()) { log.trace(this + " setting up connection to " + serverLocator); }

      final int MAX_RETRIES = 50;
      boolean completed = false;
      Connector server = null;
      String serializationType = null;
      int count = 0;

      String thisAddress = CLIENT_HOST;
      
      if (thisAddress==null)
      {
    	  thisAddress = InetAddress.getLocalHost().getHostAddress();
      }
    	  
      boolean isSSL = serverLocator.getProtocol().equals("sslsocket");
      Map params = serverLocator.getParameters();

      if (params != null)
      {
         //serializationType = (String)params.get("serializationtype");
         
         //Always use jms
         serializationType = "jms";
      }
            
      while (!completed && count < MAX_RETRIES)
      {
         try
         {      
            int bindPort = PortUtil.findFreePort(thisAddress);
      
            String callbackServerURI;
      
            if (isSSL)
            {
               // See http://jira.jboss.com/jira/browse/JBREM-470
               callbackServerURI =
                  "sslsocket://" + thisAddress + ":" +  bindPort + CALLBACK_SERVER_PARAMS +
                  "&" + SSLSocketBuilder.REMOTING_SERVER_SOCKET_USE_CLIENT_MODE + "=true";
            }
            else
            {
               callbackServerURI = serverLocator.getProtocol() + "://" + thisAddress +
                                   ":" + bindPort + CALLBACK_SERVER_PARAMS;
            }           
      
            if (serializationType != null)
            {
               callbackServerURI += "&serializationType=" + serializationType;
            }
      
            InvokerLocator callbackServerLocator = new InvokerLocator(callbackServerURI);
      
            log.debug(this + " starting callback server " + callbackServerLocator.getLocatorURI());
      
            server = new Connector();
            server.setInvokerLocator(callbackServerLocator.getLocatorURI());
            server.create();
            server.addInvocationHandler(JMS_CALLBACK_SUBSYSTEM, new CallbackManager());
            server.start();
      
            if (log.isTraceEnabled()) { log.trace("callback server started"); }
            
            completed = true;
         }
         catch (Exception e)
         {
            log.warn("Failed to start connection. Will retry", e);

            // Intermittently we can fail to open a socket on the address since it's already in use
            // This is despite remoting having checked the port is free. This is probably because
            // of the small window between remoting checking the port is free and getting the
            // port number and actually opening the connection during which some one else can use
            // that port. Therefore we catch this and retry.

            count++;
            
            if (count == MAX_RETRIES)
            {
               final String msg = "Cannot start callbackserver after " + MAX_RETRIES + " retries";
               log.error(msg, e);
               throw new MessagingJMSException(msg, e);
            }
         }
      }
      
      return server;
   }
   
   protected void stopCallbackServer(Connector server)
   {
      log.debug("Stopping and destroying callback server " + server.getLocator().getLocatorURI());
      server.stop();
      server.destroy();
   }
   
   private class Holder
   {
      Connector server;
      int refCount = 1;
   }
  
}

