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
package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.ConnectionParams;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ConnectionParamsImpl implements ConnectionParams
{
   private static final long serialVersionUID = 1662480686951551534L;
   
   protected long timeout = DEFAULT_REQRES_TIMEOUT;
   protected long keepAliveInterval = DEFAULT_KEEP_ALIVE_INTERVAL;
   protected long keepAliveTimeout = DEFAULT_KEEP_ALIVE_TIMEOUT;
   protected boolean invmDisabled = DEFAULT_INVM_DISABLED;
   protected boolean invmDisabledModified = false;
   protected boolean tcpNoDelay;
   protected int tcpReceiveBufferSize = -1;
   protected int tcpSendBufferSize = -1;
   protected boolean sslEnabled = DEFAULT_SSL_ENABLED;
   protected boolean sslEnabledModified = false;
   protected String keyStorePath;
   protected String keyStorePassword;
   protected String trustStorePath;
   protected String trustStorePassword;
   protected long writeQueueBlockTimeout = 10000;
   protected long writeQueueMinBytes = 32 * 1024L;
   protected long writeQueueMaxBytes = 64 * 1024L;
   
   public long getTimeout()
   {
      return timeout;
   }

   public void setTimeout(long timeout)
   {
      this.timeout = timeout;
   }

   public long getKeepAliveInterval()
   {
      return keepAliveInterval;
   }

   public void setKeepAliveInterval(long keepAliveInterval)
   {
      this.keepAliveInterval = keepAliveInterval;
   }

   public long getKeepAliveTimeout()
   {
      return keepAliveTimeout;
   }

   public void setKeepAliveTimeout(long keepAliveTimeout)
   {
      this.keepAliveTimeout = keepAliveTimeout;
   }

   public boolean isInvmDisabled()
   {
      return invmDisabled;
   }

   public void setInvmDisabled(boolean invmDisabled)
   {
      this.invmDisabled = invmDisabled;
   }

   public boolean isInvmDisabledModified()
   {
      return invmDisabledModified;
   }

   public void setInvmDisabledModified(boolean invmDisabledModified)
   {
      this.invmDisabledModified = invmDisabledModified;
   }

   public boolean isTcpNoDelay()
   {
      return tcpNoDelay;
   }

   public void setTcpNoDelay(boolean tcpNoDelay)
   {
      this.tcpNoDelay = tcpNoDelay;
   }

   public int getTcpReceiveBufferSize()
   {
      return tcpReceiveBufferSize;
   }

   public void setTcpReceiveBufferSize(int tcpReceiveBufferSize)
   {
      this.tcpReceiveBufferSize = tcpReceiveBufferSize;
   }

   public int getTcpSendBufferSize()
   {
      return tcpSendBufferSize;
   }

   public void setTcpSendBufferSize(int tcpSendBufferSize)
   {
      this.tcpSendBufferSize = tcpSendBufferSize;
   }

   public long getWriteQueueBlockTimeout()
   {
      return writeQueueBlockTimeout;
   }

   public long getWriteQueueMaxBytes()
   {
      return writeQueueMaxBytes;
   }

   public long getWriteQueueMinBytes()
   {
      return writeQueueMinBytes;
   }

   public void setWriteQueueBlockTimeout(final long timeout)
   {
      this.writeQueueBlockTimeout = timeout;
   }

   public void setWriteQueueMaxBytes(final long bytes)
   {
      this.writeQueueMaxBytes = bytes;
   }

   public void setWriteQueueMinBytes(final long bytes)
   {
      this.writeQueueMinBytes = bytes;
   }

   public boolean isSSLEnabled()
   {
      String sslEnabledProperty = System.getProperty(REMOTING_ENABLE_SSL);
      return sslEnabledProperty==null?sslEnabled:sslEnabledProperty.equalsIgnoreCase("true");
   }

   public void setSSLEnabled(boolean sslEnabled)
   {
      this.sslEnabled = sslEnabled;
   }

   public boolean isSSLEnabledModified()
   {
      return sslEnabledModified;
   }

   public void setSSLEnabledModified(boolean sslEnabledModified)
   {
      this.sslEnabledModified = sslEnabledModified;
   }

   public String getKeyStorePath()
   {
      String sslKeystorePath = System.getProperty(REMOTING_SSL_KEYSTORE_PATH);
      return sslKeystorePath == null?keyStorePath:sslKeystorePath;
   }

   public void setKeyStorePath(String keyStorePath)
   {
      this.keyStorePath = keyStorePath;
   }

   public String getKeyStorePassword()
   {
      String keyStorePass = System.getProperty(REMOTING_SSL_KEYSTORE_PASSWORD);
      return keyStorePass == null?keyStorePassword:keyStorePass;
   }

   public void setKeyStorePassword(String keyStorePassword)
   {
      this.keyStorePassword = keyStorePassword;
   }

   public String getTrustStorePath()
   {
      String sslTruststorePath = System.getProperty(REMOTING_SSL_TRUSTSTORE_PATH);
      return sslTruststorePath==null?trustStorePath:sslTruststorePath;
   }

   public void setTrustStorePath(String trustStorePath)
   {
      this.trustStorePath = trustStorePath;
   }

   public String getTrustStorePassword()
   {
      String trustStorePass = System.getProperty(REMOTING_SSL_TRUSTSTORE_PASSWORD);
      return trustStorePass==null?trustStorePassword:trustStorePass;
   }

   public void setTrustStorePassword(String trustStorePassword)
   {
      this.trustStorePassword = trustStorePassword;
   }

   public String getURI()
   {
      StringBuffer buff = new StringBuffer();
      //buff.append(transport + "://" + host + ":" + port);
      buff.append("?").append("timeout=").append(timeout);
      buff.append("&").append("keepAliveInterval=").append(keepAliveInterval);
      buff.append("&").append("keepAliveTimeout=").append(keepAliveTimeout);
      buff.append("&").append("invmDisabled=").append(invmDisabled);
      buff.append("&").append("tcpNoDelay=").append(tcpNoDelay);
      buff.append("&").append("tcpReceiveBufferSize=").append(tcpReceiveBufferSize);
      buff.append("&").append("tcpSendBufferSize=").append(tcpSendBufferSize);
      buff.append("&").append("sslEnabled=").append(sslEnabled);
      buff.append("&").append("keyStorePath=").append(keyStorePath);
      buff.append("&").append("trustStorePath=").append(trustStorePath);
      return buff.toString();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof ConnectionParams == false)
      {
         return false;
      }
      
      ConnectionParams cp = (ConnectionParams)other;
      
      return cp.getTimeout() == timeout &&
             cp.getKeepAliveTimeout() == this.keepAliveTimeout &&
             cp.getKeepAliveInterval() == this.keepAliveInterval &&
             cp.isInvmDisabled() == this.isInvmDisabled() &&
             cp.isInvmDisabledModified() == this.isInvmDisabledModified() &&
             cp.isTcpNoDelay() == this.isTcpNoDelay() &&
             cp.getTcpReceiveBufferSize() == this.getTcpReceiveBufferSize() &&
             cp.getTcpSendBufferSize() == this.getTcpSendBufferSize() &&
             cp.isSSLEnabled() == this.isSSLEnabled() &&
             cp.isSSLEnabledModified() == this.isSSLEnabledModified() &&
             cp.getWriteQueueBlockTimeout() == this.getWriteQueueBlockTimeout() &&
             cp.getWriteQueueMinBytes() == this.getWriteQueueMinBytes() &&
             cp.getWriteQueueMaxBytes() == this.getWriteQueueMaxBytes();
   }
}
