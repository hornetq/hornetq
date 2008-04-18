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

import java.io.Serializable;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ConnectionParamsImpl implements ConnectionParams
{

   protected int timeout = DEFAULT_REQRES_TIMEOUT;
   protected int keepAliveInterval = DEFAULT_KEEP_ALIVE_INTERVAL;
   protected int keepAliveTimeout = DEFAULT_KEEP_ALIVE_TIMEOUT;
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

   public int getTimeout()
   {
      return timeout;
   }

   public void setTimeout(int timeout)
   {
      this.timeout = timeout;
   }

   public int getKeepAliveInterval()
   {
      return keepAliveInterval;
   }

   public void setKeepAliveInterval(int keepAliveInterval)
   {
      this.keepAliveInterval = keepAliveInterval;
   }

   public int getKeepAliveTimeout()
   {
      return keepAliveTimeout;
   }

   public void setKeepAliveTimeout(int keepAliveTimeout)
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

   public boolean isSSLEnabled()
   {
      return sslEnabled;
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
      return keyStorePath;
   }

   public void setKeyStorePath(String keyStorePath)
   {
      this.keyStorePath = keyStorePath;
   }

   public String getKeyStorePassword()
   {
      return keyStorePassword;
   }

   public void setKeyStorePassword(String keyStorePassword)
   {
      this.keyStorePassword = keyStorePassword;
   }

   public String getTrustStorePath()
   {
      return trustStorePath;
   }

   public void setTrustStorePath(String trustStorePath)
   {
      this.trustStorePath = trustStorePath;
   }

   public String getTrustStorePassword()
   {
      return trustStorePassword;
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
}
