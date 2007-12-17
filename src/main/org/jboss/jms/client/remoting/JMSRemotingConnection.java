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

import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.ConnectorRegistry;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.ServerLocator;

/**
 * Encapsulates the state and behaviour from MINA needed for a JMS connection.
 * 
 * Each JMS connection maintains a single Client instance for invoking on the server.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class JMSRemotingConnection
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSRemotingConnection.class);
   
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private ServerLocator serverLocator;

   private Client client;

   private CallbackManager callbackManager;
   private boolean strictTck;

   // When a failover is performed, this flag is set to true
   protected boolean failed = false;

   // Maintaining a reference to the remoting connection listener for cases when we need to
   // explicitly remove it from the remoting client
   private ConsolidatedRemotingConnectionListener remotingConnectionListener;

   // Constructors ---------------------------------------------------------------------------------

   public JMSRemotingConnection(String serverLocatorURI, boolean strictTck) throws Exception
   {
      this.serverLocator = new ServerLocator(serverLocatorURI);
      this.strictTck = strictTck;

      log.trace(this + " created");
   }

   // Public ---------------------------------------------------------------------------------------

   public void start() throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace(this + " created client"); }

      callbackManager = new CallbackManager();

      NIOConnector connector = ConnectorRegistry.getConnector(serverLocator);
      client = new Client(connector, serverLocator);
      client.connect();

      if (log.isDebugEnabled())
         log.debug("Using " + connector.getServerURI() + " to connect to " + serverLocator);

      log.trace(this + " started");
   }

   public void stop()
   {
      log.trace(this + " stop");

      try
      {
         client.disconnect();
         NIOConnector connector = ConnectorRegistry.removeConnector(serverLocator);
         if (connector != null)
            connector.disconnect();
      }
      catch (Throwable ignore)
      {        
         log.trace(this + " failed to disconnect the new client", ignore);
      }

      client = null;

      log.trace(this + " closed");
   }
   
   public Client getRemotingClient()
   {
      return client;
   }

   public CallbackManager getCallbackManager()
   {
      return callbackManager;
   }


   public boolean isStrictTck()
   {
       return strictTck;
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
      
      stop();
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

      log.trace(this + " removed consolidated connection listener from " + client);
      ConsolidatedRemotingConnectionListener toReturn = remotingConnectionListener;
      remotingConnectionListener = null;
      return toReturn;
   }

   public String toString()
   {
      return "JMSRemotingConnection[" + serverLocator.getURI() + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------

}
