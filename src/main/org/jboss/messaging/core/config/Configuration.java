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

package org.jboss.messaging.core.config;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.JournalType;

/**
 * 
 * A Configuration
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Configuration extends Serializable
{
   // General attributes -------------------------------------------------------------------
   
   boolean isClustered();
   
   void setClustered(boolean clustered);

   int getScheduledThreadPoolMaxSize();
   
   void setScheduledThreadPoolMaxSize(int maxSize);
   
   long getSecurityInvalidationInterval();
   
   void setSecurityInvalidationInterval(long interval);
   
   boolean isSecurityEnabled();
   
   void setSecurityEnabled(boolean enabled);
   
   boolean isRequireDestinations();
   
   void setRequireDestinations(boolean require);

   // Remoting related attributes ----------------------------------------------------------
   
   List<String> getInterceptorClassNames();
   
   Set<String> getAcceptorFactoryClassNames();
   
   ConnectionParams getConnectionParams();
   
   TransportType getTransport();
   
   void setTransport(TransportType transport);

   String getHost();
   
   void setHost(String host);

   int getPort();
   
   void setPort(int port);

   Location getLocation();
         
   String getKeyStorePath();
   
   void setKeyStorePath(String path);

   String getKeyStorePassword();
   
   void setKeyStorePassword(String password);

   String getTrustStorePath();
   
   void setTrustStorePath(String path);

   String getTrustStorePassword();
   
   void setTrustStorePassword(String password);
   
   boolean isSSLEnabled();
   
   void setSSLEnabled(boolean enabled);
   
   // Journal related attributes
   
   String getBindingsDirectory();
   
   void setBindingsDirectory(String dir);

   String getJournalDirectory();
   
   void setJournalDirectory(String dir);

   JournalType getJournalType();
   
   void setJournalType(JournalType type);

   boolean isJournalSyncTransactional();
   
   void setJournalSyncTransactional(boolean sync);
   
   boolean isJournalSyncNonTransactional();
   
   void setJournalSyncNonTransactional(boolean sync);

   int getJournalFileSize();
   
   void setJournalFileSize(int size);

   int getJournalMinFiles();
   
   void setJournalMinFiles(int files);
   
   int getJournalMaxAIO();
   
   void setJournalMaxAIO(int maxAIO);
   
   boolean isCreateBindingsDir();
   
   void setCreateBindingsDir(boolean create);

   boolean isCreateJournalDir();
   
   void setCreateJournalDir(boolean create);
}
