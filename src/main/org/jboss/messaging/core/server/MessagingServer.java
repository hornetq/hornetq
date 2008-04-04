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
package org.jboss.messaging.core.server;

import java.util.HashSet;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.security.SecurityStore;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.version.Version;

/**
 * This interface defines the internal interface of the Messaging Server exposed
 * to other components of the server.
 * 
 * The external management interface of the Messaging Server is defined by the
 * MessagingServerManagement interface
 * 
 * This interface is never exposed outside the messaging server, e.g. by JMX or other means
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface MessagingServer extends MessagingComponent
{  
   /**
    * @return The configuration for this server
    */
   Configuration getConfiguration(); 
   
   /**
    * 
    * @return The server version
    */
   Version getVersion();
   
   boolean isStarted();
   
   void setConfiguration(Configuration configuration);
   
   void setRemotingService(RemotingService remotingService);
   
   RemotingService getRemotingService();
  
   ConnectionManager getConnectionManager();

   StorageManager getStorageManager();

   void setStorageManager(StorageManager storageManager);
   
   PostOffice getPostOffice();
   
   HierarchicalRepository<HashSet<Role>> getSecurityRepository();
   
   SecurityStore getSecurityStore();

   HierarchicalRepository<QueueSettings> getQueueSettingsRepository();

   void setPostOffice(PostOffice postOffice);
   
   CreateConnectionResponse createConnection(String username, String password,
                                             String remotingClientSessionID, String clientVMID,
                                             String clientAddress) throws Exception;

   DeploymentManager getDeploymentManager();
}
