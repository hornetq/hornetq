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
package org.jboss.messaging.core.server.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.server.ConnectionManager;
import org.jboss.messaging.core.server.ObjectIDGenerator;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.util.ConcurrentHashSet;

/**
 * Concrete implementation of ConnectionEndpoint.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision: 3789 $</tt>
 *
 * $Id: ServerConnectionImpl.java 3789 2008-02-25 15:34:18Z ataylor $
 */
public class ServerConnectionImpl implements ServerConnection
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConnectionImpl.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private final long id;

   private final String username;
   
   private final String password;

   private final long remotingClientSessionID;
   
   private final String clientAddress;
      
   private final PacketDispatcher dispatcher;
   
   private final ResourceManager resourceManager;
   
   private final StorageManager persistenceManager;  
   
   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;
      
   private final PostOffice postOffice;
   
   private final SecurityStore securityStore;
   
   private final ConnectionManager connectionManager;
   
   private final ObjectIDGenerator objectIDGenerator;

   private final long createdTime;
         
   private final Set<ServerSession> sessions = new ConcurrentHashSet<ServerSession>();

   private final Set<Queue> temporaryQueues = new ConcurrentHashSet<Queue>();
   
   private final Set<String> temporaryDestinations = new ConcurrentHashSet<String>();
      
   private volatile boolean started;


   // Constructors ---------------------------------------------------------------------------------
      
   public ServerConnectionImpl(final String username, final String password,
   		                      final long remotingClientSessionID,
   		                      final String clientAddress,
   		                      final PacketDispatcher dispatcher,
   		                      final ResourceManager resourceManager,
   		                      final StorageManager persistenceManager,
   		                      final HierarchicalRepository<QueueSettings> queueSettingsRepository,
   		                      final PostOffice postOffice, final SecurityStore securityStore,
   		                      final ConnectionManager connectionManager,
   		                      final ObjectIDGenerator objectIDGenerator)
   {
   	this.id = objectIDGenerator.generateID();
      
   	this.username = username;
      
      this.password = password;
      
      this.remotingClientSessionID = remotingClientSessionID;

      this.clientAddress = clientAddress;

      this.dispatcher = dispatcher;
      
      this.resourceManager = resourceManager;
      
      this.persistenceManager = persistenceManager;
      
      this.queueSettingsRepository = queueSettingsRepository;      
      
      this.postOffice = postOffice;
      
      this.securityStore = securityStore;
      
      this.connectionManager = connectionManager;
      
      this.objectIDGenerator = objectIDGenerator;
      
      started = false;
      
      createdTime = System.currentTimeMillis();

      connectionManager.registerConnection(remotingClientSessionID, this);
   }

   // ServerConnection implementation ------------------------------------------------------------

   public long getID()
   {
   	return id;
   }
   
   public ConnectionCreateSessionResponseMessage createSession(final boolean xa, final boolean autoCommitSends,
   		                                                      final boolean autoCommitAcks,
                                                               final PacketSender sender) throws Exception
   {           
      ServerSession session =
         new ServerSessionImpl(autoCommitSends, autoCommitAcks, xa, this, resourceManager,
         		sender, dispatcher, persistenceManager, queueSettingsRepository, postOffice, securityStore,
         		objectIDGenerator);

      sessions.add(session);
      
      dispatcher.register(new ServerSessionPacketHandler(session));
      
      return new ConnectionCreateSessionResponseMessage(session.getID());
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
      Set<ServerSession> sessionsClone = new HashSet<ServerSession>(sessions);
      
      for (ServerSession session: sessionsClone)
      {
         session.close();
      }

      sessions.clear();
      
      Set<String> addresses = new HashSet<String>();

      for (Queue tempQueue: temporaryQueues)
      {                        
         Binding binding = postOffice.getBinding(tempQueue.getName());
         
         addresses.add(binding.getAddress());     
         
         postOffice.removeBinding(tempQueue.getName());         
      }
      
      for (String address: addresses)
      {
         postOffice.removeDestination(address, true);
      }
      
      for (String address: temporaryDestinations)
      {
      	postOffice.removeDestination(address, true);
      }

      temporaryQueues.clear();      
      
      temporaryDestinations.clear();

      connectionManager.unregisterConnection(remotingClientSessionID, this);

      dispatcher.unregister(id);
   }
   
   public SecurityStore getSecurityStore()
   {
      return securityStore;
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
      temporaryQueues.add(queue);      
   }
   
   public void removeTemporaryQueue(final Queue queue)
   {
      temporaryQueues.remove(queue);      
   }
   
   public void addTemporaryDestination(final String address)
   {
      temporaryDestinations.add(address);     
   }
   
   public void removeTemporaryDestination(final String address)
   {
      temporaryDestinations.remove(address);
   }
   
   public boolean isStarted()
   {
      return started;
   }
   
   public long getCreatedTime()
   {
      return createdTime;
   }

   public String getClientAddress()
   {
      return clientAddress;
   }

   public long getCreated()
   {
      return createdTime;
   }

   public Collection<ServerSession> getSessions()
   {
      return sessions;
   }

   // Public ---------------------------------------------------------------------------------------
    
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
