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

import java.util.HashSet;

import javax.jms.ExceptionListener;

import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.message.MessageIdGenerator;
import org.jboss.jms.server.Version;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.SyncSet;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

/**
 * 
 * State corresponding to a connection. This state is acessible inside aspects/interceptors.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionState extends HierarchicalStateSupport
{
   private static final Logger log = Logger.getLogger(ConnectionState.class);

   private JMSRemotingConnection remotingConnection;

   private ResourceManager resourceManager;

   private MessageIdGenerator idGenerator;

   private int serverID;

   private Version versionToUse;

   private ConnectionDelegate delegate;

   // This is filled from and for the HA interceptors only
   private transient String user;

   // This is filled from and for the HA interceptors only
   private transient String password;

   protected boolean started;

   /** This property used to be delcared on ConnectionAspect */
   private String clientID;

    /** This property used to be delcared on ConnectionAspect */
   private ExceptionListener exceptionListener;

    /** This property used to be delcared on ConnectionAspect */
   private boolean justCreated = true;

    /** This property used to be delcared on ConnectionAspect */
   private boolean listenerAdded;
   
   public ConnectionState(int serverID, ConnectionDelegate delegate,
                          JMSRemotingConnection remotingConnection, Version versionToUse,
                          MessageIdGenerator gen)
      throws Exception
   {
      super(null, (DelegateSupport)delegate);

      if (log.isTraceEnabled()) { log.trace(this + " constructing connection state"); }

      children = new SyncSet(new HashSet(), new WriterPreferenceReadWriteLock());

      this.remotingConnection = remotingConnection;
      this.versionToUse = versionToUse;

      // Each connection has its own resource manager. If we can failover all connections with the
      // same server id at the same time then we can maintain one rm per unique server as opposed to
      // per connection.
      this.resourceManager = new ResourceManager();

      this.idGenerator = gen;
      this.serverID = serverID;
   }

   public ResourceManager getResourceManager()
   {
      return resourceManager;
   }

   public MessageIdGenerator getIdGenerator()
   {
      return idGenerator;
   }

   public JMSRemotingConnection getRemotingConnection()
   {
      return remotingConnection;
   }

   public void setRemotingConnection(JMSRemotingConnection remotingConnection)
   {
       this.remotingConnection=remotingConnection;
   }

   public Version getVersionToUse()
   {
      return versionToUse;
   }

   public int getServerID()
   {
      return serverID;
   }

   public DelegateSupport getDelegate()
   {
      return (DelegateSupport) delegate;
   }

   public void setDelegate(DelegateSupport delegate)
   {
      this.delegate = (ConnectionDelegate) delegate;
   }

   /**
    * Connection doesn't have a parent
    */
   public void setParent(HierarchicalState parent)
   {
   }

   public boolean isStarted()
   {
      return started;
   }

   public void setStarted(boolean started)
   {
      this.started = started;
   }

   public String getPassword()
   {
      return password;
   }

   public void setPassword(String password)
   {
      this.password = password;
   }

   public String getUser()
   {
      return user;
   }

   public void setUser(String user)
   {
      this.user = user;
   }

   public String getClientID()
   {
      return clientID;
   }

   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }

   public ExceptionListener getExceptionListener()
   {
      return exceptionListener;
   }

   public void setExceptionListener(ExceptionListener exceptionListener)
   {
      this.exceptionListener = exceptionListener;
   }

   public boolean isJustCreated()
   {
      return justCreated;
   }

   public void setJustCreated(boolean justCreated)
   {
      this.justCreated = justCreated;
   }

   public boolean isListenerAdded()
   {
      return listenerAdded;
   }

   public void setListenerAdded(boolean listenerAdded)
   {
      this.listenerAdded = listenerAdded;
   }

   /** Connection doesn't have a parent */
   public HierarchicalState getParent()
   {
      return null;
   }

   //When failing over a connection, we keep the old connection's state but there are certain fields
   //we need to update
   public void copyState(ConnectionState newState)
   {
      this.remotingConnection = newState.remotingConnection;
      this.idGenerator = newState.idGenerator;
      this.serverID = newState.serverID;
      this.versionToUse = newState.versionToUse;
      this.delegate = newState.delegate;
   }

   public String toString()
   {
      return "ConnectionState[" + ((ClientConnectionDelegate)delegate).getID() + "]";
   }
}
