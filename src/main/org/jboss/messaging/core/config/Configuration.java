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
package org.jboss.messaging.core.config;

import java.util.List;

import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.JournalType;

/**
 * 
 * A Configuration
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface Configuration
{

   int getMessagingServerID();

   String getSecurityDomain();

   List<String> getDefaultInterceptors();

   long getMessageCounterSamplePeriod();

   Integer getDefaultMessageCounterHistoryDayLimit();

   Boolean isStrictTck();

   Boolean isClustered();

   Integer getScheduledThreadPoolMaxSize();

   long getSecurityInvalidationInterval();

   TransportType getTransport();

   String getHost();

   int getPort();

   String getLocation();

   boolean isTcpNoDelay();
   
   int getTcpReceiveBufferSize();

   int getTcpSendBufferSize();

   int getKeepAliveInterval();

   int getKeepAliveTimeout();

   int getTimeout();

   String getKeyStorePath();

   String getKeyStorePassword();

   String getTrustStorePath();

   String getTrustStorePassword();

   boolean isInvmDisabled();

   boolean isSSLEnabled();

   String getURI();

   String getBindingsDirectory();

   String getJournalDirectory();

   JournalType getJournalType();

   boolean isJournalSync();

   int getJournalFileSize();

   int getJournalMinFiles();

   long getJournalTaskPeriod();

   boolean isCreateBindingsDir();

   boolean isCreateJournalDir();

   boolean isRequireDestinations();
}
