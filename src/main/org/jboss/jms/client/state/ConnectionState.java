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
import java.util.Iterator;

import org.jboss.jms.client.FailoverCommandCenter;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.message.MessageIdGenerator;
import org.jboss.jms.server.Version;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.SyncSet;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

/**
 * State corresponding to a connection. This state is acessible inside aspects/interceptors.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionState extends HierarchicalStateSupport
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ConnectionState.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private int serverID;

   private Version versionToUse;

   private ConnectionDelegate delegate;

   protected boolean started;

   private boolean justCreated = true;

   private String clientID;

   private JMSRemotingConnection remotingConnection;
   private ResourceManager resourceManager;
   private MessageIdGenerator idGenerator;

   // Cached by the connection state in case ClusteringAspect needs to re-try establishing
   // connection on a different node
   private transient String username;

   // Cached by the connection state in case ClusteringAspect needs to re-try establishing
   // connection on a different node
   private transient String password;

   // needed to try re-creating connection in case failure is detected on the current connection
   private ConnectionFactoryDelegate clusteredConnectionFactoryDelegate;

   private FailoverCommandCenter fcc;

   // Constructors ---------------------------------------------------------------------------------

   public ConnectionState(int serverID, ConnectionDelegate delegate,
                          JMSRemotingConnection remotingConnection,
                          Version versionToUse,
                          MessageIdGenerator gen)
      throws Exception
   {
      super(null, (DelegateSupport)delegate);

      if (log.isTraceEnabled()) { log.trace(this + " constructing connection state"); }

      children = new SyncSet(new HashSet(), new WriterPreferenceReadWriteLock());

      this.remotingConnection = remotingConnection;
      this.versionToUse = versionToUse;

      // Each connection has its own resource manager. If we can failover all connections with the
      // same server id at the same time then we can maintain one rm per unique server as opposed
      // to per connection.
      this.resourceManager = ResourceManagerFactory.instance.checkOutResourceManager(serverID);

      this.idGenerator = gen;
      this.serverID = serverID;
   }

   // HierarchicalState implementation -------------------------------------------------------------

   public DelegateSupport getDelegate()
   {
      return (DelegateSupport) delegate;
   }

   public void setDelegate(DelegateSupport delegate)
   {
      this.delegate = (ConnectionDelegate) delegate;
   }

   public HierarchicalState getParent()
   {
      // A connection doesn't have a parent
      return null;
   }

   public void setParent(HierarchicalState parent)
   {
      // noop - a connection doesn't have a parent
   }

   public Version getVersionToUse()
   {
      return versionToUse;
   }

   public void synchronizeWith(HierarchicalState ns) throws Exception
   {
      ConnectionState newState = (ConnectionState)ns;

      remotingConnection = newState.remotingConnection;
      idGenerator = newState.idGenerator;
      serverID = newState.serverID;
      versionToUse = newState.versionToUse;
      
      ConnectionDelegate newDelegate = (ConnectionDelegate)newState.getDelegate();
      
      for(Iterator i = getChildren().iterator(); i.hasNext(); )
      {                 
         SessionState sessionState = (SessionState)i.next();
         
         ClientSessionDelegate sessionDelegate = (ClientSessionDelegate)sessionState.getDelegate();

         // create a new session on the new connection for each session on the old connection
         ClientSessionDelegate newSessionDelegate = (ClientSessionDelegate)newDelegate.
            createSessionDelegate(sessionState.isTransacted(),
                                  sessionState.getAcknowledgeMode(),
                                  sessionState.isXA());

         sessionDelegate.synchronizeWith(newSessionDelegate);
      }
      
      //We weren't picking up the new fcc before so new delegates were using the old fcc!!
      fcc = newState.fcc;
      fcc.setState(this);
   }

   // Public ---------------------------------------------------------------------------------------

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

   public int getServerID()
   {
      return serverID;
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

   public String getUsername()
   {
      return username;
   }

   public void setUsername(String username)
   {
      this.username = username;
   }

   public String getClientID()
   {
      return clientID;
   }

   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }

   public boolean isJustCreated()
   {
      return justCreated;
   }

   public void setJustCreated(boolean justCreated)
   {
      this.justCreated = justCreated;
   }

   public void setClusteredConnectionFactoryDeleage(ConnectionFactoryDelegate d)
   {
      this.clusteredConnectionFactoryDelegate = d;
   }

   public ConnectionFactoryDelegate getClusteredConnectionFactoryDelegate()
   {
      return clusteredConnectionFactoryDelegate;
   }

   public FailoverCommandCenter getFailoverCommandCenter()
   {
      return fcc;
   }

   public void initializeFailoverCommandCenter()
   {
      fcc = new FailoverCommandCenter(this);
   }

   public String toString()
   {
      return "ConnectionState[" + ((ClientConnectionDelegate)delegate).getID() + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
