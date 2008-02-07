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

import static org.jboss.messaging.core.remoting.ConnectorRegistrySingleton.REGISTRY;

import org.jboss.jms.client.api.FailureListener;
import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.impl.ClientImpl;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

/**
 * 
 * TODO: This class should disappear in favor of Connection/Client
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
public class MessagingRemotingConnection
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(MessagingRemotingConnection.class);
   
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private RemotingConfiguration remotingConfig;

   private Client client;

   // Constructors ---------------------------------------------------------------------------------

   public MessagingRemotingConnection(RemotingConfiguration remotingConfig) throws Exception
   {
      assert remotingConfig != null;
      
      this.remotingConfig = remotingConfig;
      
      log.trace(this + " created with configuration " + remotingConfig);
   }

   // Public ---------------------------------------------------------------------------------------

   public void start() throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace(this + " created client"); }

      //callbackManager = new CallbackManager();

      NIOConnector connector = REGISTRY.getConnector(remotingConfig);
      client = new ClientImpl(connector, remotingConfig);
      client.connect();

      if (log.isDebugEnabled())
         log.debug("Using " + connector + " to connect to " + remotingConfig);

      log.trace(this + " started");
   }

   public void stop()
   {
      log.trace(this + " stop");

      try
      {
         client.disconnect();
         NIOConnector connector = REGISTRY.removeConnector(remotingConfig);
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
   
   public String getSessionID()
   {
      return client.getSessionID();
   }
 
   /**
    * send the packet and block until a response is received (<code>oneWay</code> is set to <code>false</code>)
    */
   public AbstractPacket send(String id, AbstractPacket packet) throws MessagingException
   {
      return send(id, packet, false);
   }
   
   public AbstractPacket send(String id, AbstractPacket packet, boolean oneWay) throws MessagingException
   {
      assert packet != null;

      packet.setTargetID(id);
      
      AbstractPacket response;
      
      try
      {      
         response = (AbstractPacket) client.send(packet, oneWay);
      }
      catch (Exception e)
      {
         log.error("Caught unexpected exception", e);
         
         throw new MessagingException(MessagingException.INTERNAL_ERROR);
      }
      
      if (oneWay == false && response == null)
      {
         throw new IllegalStateException("No response received for " + packet);
      }
      
      if (response instanceof MessagingExceptionMessage)
      {
         MessagingExceptionMessage message = (MessagingExceptionMessage) response;
         
         throw message.getException();
      }
      else
      {
         return response;
      } 
   }
   
   public synchronized void setFailureListener(FailureListener listener)
   {
      if (client != null)
      {
         client.setFailureListener(listener);
      }
   }

   public synchronized FailureListener getFailureListener()
   {
      if (client != null)
      {
         return client.getFailureListener();
      }
      else
      {
         return null;
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------

}
