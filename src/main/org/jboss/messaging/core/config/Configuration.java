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

   public int getMessagingServerID();

   public String getSecurityDomain();

   public List<String> getDefaultInterceptors();

   public long getMessageCounterSamplePeriod();

//   public void setMessageCounterSamplePeriod(long messageCounterSamplePeriod);

   public Integer getDefaultMessageCounterHistoryDayLimit();

//   public void setDefaultMessageCounterHistoryDayLimit(Integer defaultMessageCounterHistoryDayLimit);

   public Boolean isStrictTck();

 //  public void setStrictTck(Boolean strictTck);

   public Boolean isClustered();

   public Integer getScheduledThreadPoolMaxSize();

//   public void setScheduledThreadPoolMaxSize(int size);

//   public void setClustered(Boolean clustered);

   public long getSecurityInvalidationInterval();

   public TransportType getTransport();

   public void setTransport(TransportType transport);

   public String getHost();

   public void setHost(String host);

   public int getPort();

   public void setPort(int port);

   public String getLocation();

   public int getKeepAliveInterval();

   public void setKeepAliveInterval(int keepAliveInterval);

   public int getKeepAliveTimeout();

   public void setKeepAliveTimeout(int keepAliveTimeout);

   public int getTimeout();

   public String getKeyStorePath();

   public void setKeyStorePath(String keyStorePath);

   public String getKeyStorePassword();

   public void setKeyStorePassword(String keyStorePassword);

   public String getTrustStorePath();

   public void setTrustStorePath(String trustStorePath);

   public String getTrustStorePassword();

   public void setTrustStorePassword(String trustStorePassword);

   public boolean isInvmDisabled();

   public void setInvmDisabled(boolean invmDisabled);

   public boolean isSSLEnabled();

   public void setSSLEnabled(boolean sslEnabled);

   public String getURI();

   public String getBindingsDirectory();

//   public void setBindingsDirectory(String bindingsDirectory);

   public String getJournalDirectory();

//   public void setJournalDirectory(String journalDirectory);

   public JournalType getJournalType();

//   public void setJournalType(JournalType journalType);

   public boolean isJournalSync();

  // public void setJournalSync(boolean journalSync);

   public int getJournalFileSize();

 //  public void setJournalFileSize(int journalFileSize);

   public int getJournalMinFiles();

//   public void setJournalMinFiles(int journalMinFiles);

   public long getJournalTaskPeriod();

//   public void setJournalTaskPeriod(long journalTaskPeriod);

   public boolean isCreateBindingsDir();

  // public void setCreateBindingsDir(boolean createBindingsDir);

   public boolean isCreateJournalDir();

//   public void setCreateJournalDir(boolean createJournalDir);

   public boolean isRequireDestinations();

//   public void setRequireDestinations(boolean requireDestinations);

}
