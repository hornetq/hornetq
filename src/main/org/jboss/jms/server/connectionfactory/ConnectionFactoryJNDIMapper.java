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
package org.jboss.jms.server.connectionfactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.client.plugin.LoadBalancingFactory;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.server.ConnectionFactoryManager;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryEndpoint;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.util.JNDIUtil;
import org.jboss.messaging.util.Version;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactoryJNDIMapper implements ConnectionFactoryManager
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ConnectionFactoryJNDIMapper.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   protected Context initialContext;
   protected MessagingServer messagingServer;

   // Map<uniqueName<String> - ServerConnectionFactoryEndpoint>
   private Map endpoints;

   // Constructors ---------------------------------------------------------------------------------

   public ConnectionFactoryJNDIMapper(MessagingServer messagingServer) throws Exception
   {
      this.messagingServer = messagingServer;
      endpoints = new HashMap();
   }

   // ConnectionFactoryManager implementation ------------------------------------------------------

   /**
    * @param loadBalancingFactory - ignored for non-clustered connection factories.
    */
   public synchronized void registerConnectionFactory(String uniqueName,
                                                      String clientID,
                                                      List<String> jndiBindings,
                                                      String serverLocatorURI,
                                                      boolean clientPing,
                                                      int prefetchSize,                                                 
                                                      int defaultTempQueueFullSize,
                                                      int defaultTempQueuePageSize,
                                                      int defaultTempQueueDownCacheSize,
                                                      int dupsOKBatchSize,
                                                      boolean supportsFailover,
                                                      boolean supportsLoadBalancing,
                                                      LoadBalancingFactory loadBalancingFactory,
                                                      boolean strictTck)
      throws Exception
   {
      log.debug(this + " registering connection factory '" + uniqueName + "', bindings: " + jndiBindings);

      // Sanity check
      if (endpoints.containsKey(uniqueName))
      {
         throw new IllegalArgumentException("There's already a connection factory " +
                                            "registered with name " + uniqueName);
      }

      // See http://www.jboss.com/index.html?module=bb&op=viewtopic&p=4076040#4076040
      final String id = uniqueName;

      Version version = messagingServer.getVersion();

      ServerConnectionFactoryEndpoint endpoint =
         new ServerConnectionFactoryEndpoint(uniqueName, id, messagingServer, clientID,
                                             jndiBindings, prefetchSize,                                            
                                             defaultTempQueueFullSize,
                                             defaultTempQueuePageSize,
                                             defaultTempQueueDownCacheSize,
                                             dupsOKBatchSize);
      endpoints.put(uniqueName, endpoint);

      //The server peer strict setting overrides the connection factory
      boolean useStrict = messagingServer.getConfiguration().isStrictTck() || strictTck;

      ConnectionFactoryDelegate delegate =
         new ClientConnectionFactoryDelegate(uniqueName, id, messagingServer.getConfiguration().getMessagingServerID(),
                                             serverLocatorURI, version, clientPing, useStrict);

      log.debug(this + " created local delegate " + delegate);

      // Now bind it in JNDI
      rebindConnectionFactory(initialContext, jndiBindings, delegate);

      // Registering with the dispatcher should always be the last thing otherwise a client could
      // use a partially initialised object
           
      messagingServer.getMinaService().getDispatcher().register(endpoint.newHandler());
   }

   public synchronized void unregisterConnectionFactory(String uniqueName)
      throws Exception
   {
      log.trace("ConnectionFactory " + uniqueName + " being unregistered");
      ServerConnectionFactoryEndpoint endpoint =
         (ServerConnectionFactoryEndpoint)endpoints.remove(uniqueName);

      if (endpoint == null)
      {
         throw new IllegalArgumentException("Cannot find endpoint with name " + uniqueName);
      }

      List<String> jndiBindings = endpoint.getJNDIBindings();

      if (jndiBindings != null)
      {
         for(Iterator i = jndiBindings.iterator(); i.hasNext(); )
         {
            String jndiName = (String)i.next();
            initialContext.unbind(jndiName);
            log.debug(jndiName + " unregistered");
         }
      }     
      
      this.messagingServer.getMinaService().getDispatcher().unregister(endpoint.getID());
   }

   // MessagingComponent implementation ------------------------------------------------------------

   public void start() throws Exception
   {
      initialContext = new InitialContext();

      log.debug("started");
   }

   public void stop() throws Exception
   {
      initialContext.close();

      log.debug("stopped");
   }
   
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "Server[" + messagingServer.getConfiguration().getMessagingServerID() + "].ConnFactoryJNDIMapper";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void rebindConnectionFactory(Context ic, List<String> jndiBindings,
                                        ConnectionFactoryDelegate delegate)
      throws NamingException
   {
      JBossConnectionFactory cf = new JBossConnectionFactory(delegate);

      if (jndiBindings != null)
      {
         for(Iterator i = jndiBindings.iterator(); i.hasNext(); )
         {
            String jndiName = (String)i.next();
            log.debug(this + " rebinding " + cf + " as " + jndiName);
            JNDIUtil.rebind(ic, jndiName, cf);
         }
      }
   }

   // Inner classes --------------------------------------------------------------------------------
}
