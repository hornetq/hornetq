/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.server.impl;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision: 3789 $</tt>
 *
 * $Id: ServerConnectionImpl.java 3789 2008-02-25 15:34:18Z ataylor $
 */
public class ServerConnectionImpl implements ServerConnection, FailureListener
{
   // Constants ------------------------------------------------------------------------------------
  
   private static final Logger log = Logger.getLogger(ServerConnectionImpl.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final long id;

   private final String username;
   
   private final String password;

   private final RemotingConnection remotingConnection;
   
   private final Set<ServerSession> sessions = new ConcurrentHashSet<ServerSession>();

   private final Set<Queue> temporaryQueues = new ConcurrentHashSet<Queue>();
   
   private final Set<SimpleString> temporaryDestinations = new ConcurrentHashSet<SimpleString>();
     
   private volatile boolean started;
   
   private final PostOffice postOffice;

   private final PacketDispatcher dispatcher;
   
   private final StorageManager storageManager;
   
   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;
   
   private final ResourceManager resourceManager;
   
   private final SecurityStore securityStore;
   
   private final ExecutorFactory executorFactory;
   
   private volatile boolean closed;

   // Constructors ---------------------------------------------------------------------------------
      
   public ServerConnectionImpl(final String username, final String password,
                               final RemotingConnection remotingConnection,
                               final PostOffice postOffice,
                               final PacketDispatcher packetDispatcher,
                               final StorageManager storageManager,
                               final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                               final ResourceManager resourceManager,
                               final SecurityStore securityStore,
                               final ExecutorFactory executorFactory)
   {          	      
   	this.username = username;
      
      this.password = password;
      
      this.remotingConnection = remotingConnection;

      this.postOffice = postOffice;
      
      this.dispatcher = packetDispatcher;
      
      this.storageManager = storageManager;
      
      this.queueSettingsRepository = queueSettingsRepository;
      
      this.resourceManager = resourceManager;
      
      this.securityStore = securityStore;
      
      this.executorFactory = executorFactory;
      
      this.id = dispatcher.generateID();    
   }

   // ServerConnection implementation ------------------------------------------------------------

   public long getID()
   {
   	return id;
   }
   
   public ConnectionCreateSessionResponseMessage createSession(final boolean xa, final boolean autoCommitSends,
   		                                                      final boolean autoCommitAcks) throws Exception
   {            
      ServerSession session = doCreateSession(autoCommitSends, autoCommitAcks, xa);

      sessions.add(session);
      
      dispatcher.register(new ServerSessionPacketHandler(session, remotingConnection));
      
      return new ConnectionCreateSessionResponseMessage(session.getID());
   }
   
   protected ServerSession doCreateSession(final boolean autoCommitSends, final boolean autoCommitAcks,
                                           final boolean xa)
      throws Exception
   {
      Executor executor = executorFactory.getExecutor();
            
      return new ServerSessionImpl(this, autoCommitSends, autoCommitAcks, xa, remotingConnection,
                                   storageManager, postOffice, queueSettingsRepository,
                                   resourceManager, securityStore, dispatcher,
                                   executor);
   }
      
   public void start() throws Exception
   {
      setStarted(true);
   }

   public synchronized void stop() throws Exception
   {
      setStarted(false);
   }

   public void close() throws Exception
   {
      if (closed)
      {
         return;
      }
      
      Set<ServerSession> sessionsClone = new HashSet<ServerSession>(sessions);
      
      for (ServerSession session: sessionsClone)
      {
         session.close();
      }

      sessions.clear();
      
      Set<SimpleString> addresses = new HashSet<SimpleString>();

      for (Queue tempQueue: temporaryQueues)
      {                        
         SimpleString name = tempQueue.getName();
         
         Binding binding = postOffice.getBinding(name);
         
         addresses.add(binding.getAddress());     
         
         postOffice.removeBinding(name);         
      }
      
      for (SimpleString address: addresses)
      {
         postOffice.removeDestination(address, true);
      }
      
      for (SimpleString address: temporaryDestinations)
      {
      	postOffice.removeDestination(address, true);
      }

      temporaryQueues.clear();      
      
      temporaryDestinations.clear();

      remotingConnection.removeFailureListener(this);
      
      dispatcher.unregister(id);
      
      closed = true;
   }
   
   public boolean isClosed()
   {
      return closed;
   }
   
   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }
   
   public void removeSession(final ServerSession session) throws Exception
   {
      if (!sessions.remove(session))
      {
         throw new IllegalStateException("Cannot find session with id " + session.getID() + " to remove");
      }      
      
      dispatcher.unregister(session.getID());
   }

   public void addTemporaryQueue(final Queue queue)
   {
      if (temporaryQueues.contains(queue))
      {
         throw new IllegalStateException("Connection already has temporary queue " + queue);
      }
      
      temporaryQueues.add(queue);      
   }
   
   public void removeTemporaryQueue(final Queue queue)
   {
      if (!temporaryQueues.remove(queue))
      {
         throw new IllegalStateException("Cannot find temporary queue to remove " + queue);
      }
   }
   
   public void addTemporaryDestination(final SimpleString address)
   {
      if (temporaryDestinations.contains(address))
      {
         throw new IllegalStateException("Connection already has temporary destination " + address);
      }
      
      temporaryDestinations.add(address);     
   }
   
   public void removeTemporaryDestination(final SimpleString address)
   {
      if (!temporaryDestinations.remove(address))
      {
         throw new IllegalStateException("Cannot find temporary destination to remove " + address);
      }
   }
   
   public boolean isStarted()
   {
      return started;
   }
   
   // FailureListener implementation -----------------------------------------------------
   
   public void connectionFailed(final MessagingException me)
   {
      try
      {
         close();
      }
      catch (Throwable t)
      {
         log.error("Failed to close connection " + this);
      }
   }
   
   // Public ---------------------------------------------------------------------------------------
    
   public void addSession(final ServerSession session)
   {
      sessions.add(session);
   }
   
   public Set<Queue> getTemporaryQueues()
   {
      return new HashSet<Queue>(temporaryQueues);
   }
   
   public Set<SimpleString> getTemporaryDestinations()
   {
      return new HashSet<SimpleString>(temporaryDestinations);
   }
   
   public Set<ServerSession> getSessions()
   {
      return new HashSet<ServerSession>(sessions);
   }
   
   public String toString()
   {
      return "ConnectionEndpoint[" + id + "]";
   }

   // Private --------------------------------------------------------------------------------------
   
   private void setStarted(final boolean started) throws Exception
   {
      Set<ServerSession> sessionsClone = new HashSet<ServerSession>(sessions);
            
      for (ServerSession session: sessionsClone)
      {
         session.setStarted(started);
      }
      
      this.started = started;      
   }         
}
