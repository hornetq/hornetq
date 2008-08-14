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

package org.jboss.messaging.core.server;

import java.util.Set;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.Role;
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
   void setConfiguration(Configuration configuration);
   
   Configuration getConfiguration(); 
   
   void setRemotingService(RemotingService remotingService);
   
   RemotingService getRemotingService();
   
   void setStorageManager(StorageManager storageManager);
      
   StorageManager getStorageManager();

   JBMSecurityManager getSecurityManager();
      
   void setSecurityManager(JBMSecurityManager securityManager);

   ManagementService getManagementService();
  
   void setManagementService(ManagementService managementService);
   
   Version getVersion();
  
   CreateConnectionResponse createConnection(String username, String password,                                          
                                             int incrementingVersion,
                                             RemotingConnection remotingConnection) throws Exception;
        
   boolean isStarted();
   
   MessagingServerControlMBean getServerManagement();  
   
   HierarchicalRepository<Set<Role>> getSecurityRepository();
   
   HierarchicalRepository<QueueSettings> getQueueSettingsRepository();
   
   int getConnectionCount();
   
   PostOffice getPostOffice();
}
