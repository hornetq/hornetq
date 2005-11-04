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
package org.jboss.jms.server.endpoint;

import java.io.Serializable;
import java.lang.reflect.Proxy;

import javax.jms.JMSException;

import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.client.container.JMSInvocationHandler;
import org.jboss.jms.client.container.RemotingClientInterceptor;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.server.ClientManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.logging.Logger;

/**
 * Creates ConnectionFactoryDelegate instances. Instances of this class are constructed only on the
 * server.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerConnectionFactoryDelegate implements ConnectionFactoryDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConnectionFactoryDelegate.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;
   
   protected String clientID;

   // Constructors --------------------------------------------------

   public ServerConnectionFactoryDelegate(ServerPeer serverPeer, String defaultClientID)
   {
      this.serverPeer = serverPeer;
      this.clientID = defaultClientID;
   }

   // ConnectionFactoryDelegate implementation ----------------------

   public ConnectionDelegate createConnectionDelegate()
      throws JMSException
   {
      return createConnectionDelegate(null, null);
   }

   public ConnectionDelegate createConnectionDelegate(String username, String password)
      throws JMSException
   {
      log.debug("Creating a new connection with username=" + username);
      
      //authenticate the user
      serverPeer.getSecurityManager().authenticate(username, password);

      // create the ConnectionDelegate dynamic proxy
      ConnectionDelegate cd = null;
      Serializable oid = serverPeer.getConnectionAdvisor().getName();
      String stackName = "ConnectionStack";
      AdviceStack stack = AspectManager.instance().getAdviceStack(stackName);

      // TODO why do I need to the advisor to create the interceptor stack?
      Interceptor[] interceptors = stack.createInterceptors(serverPeer.getConnectionAdvisor(), null);

      JMSInvocationHandler h = new JMSInvocationHandler(interceptors);

      ClientManager clientManager = serverPeer.getClientManager();

      SimpleMetaData metadata = new SimpleMetaData();
      // TODO: The ConnectionFactoryDelegate and ConnectionDelegate share the same locator (TCP/IP connection?). Performance?
      metadata.addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, oid, PayloadKey.AS_IS);
      metadata.addMetaData(RemotingClientInterceptor.REMOTING,
            RemotingClientInterceptor.INVOKER_LOCATOR,
                           serverPeer.getLocator(),
                           PayloadKey.AS_IS);
      metadata.addMetaData(RemotingClientInterceptor.REMOTING,
            RemotingClientInterceptor.SUBSYSTEM,
                           "JMS",
                           PayloadKey.AS_IS);
      
      //See if there is a preconfigured client id for the user
      if (username != null)
      {
         String preconfClientID = serverPeer.getStateManager().getPreConfiguredClientID(username);
         if (preconfClientID != null)
         {
            clientID = preconfClientID;
         }
      }

      // create the corresponding "server-side" ConnectionDelegate and register it with the
      // server peer's ClientManager
      ServerConnectionDelegate scd = new ServerConnectionDelegate(serverPeer, clientID, username, password);
      clientManager.putConnectionDelegate(scd.getConnectionID(), scd);
      
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CONNECTION_ID,
                           scd.getConnectionID(), PayloadKey.AS_IS);
      
      h.getMetaData().mergeIn(metadata);

      // TODO
      ClassLoader loader = getClass().getClassLoader();
      Class[] interfaces = new Class[] { ConnectionDelegate.class };
      cd = (ConnectionDelegate)Proxy.newProxyInstance(loader, interfaces, h);

      log.debug("created connection delegate (connectionID=" + scd.getConnectionID() + "), returning it to the client");

      return cd;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
