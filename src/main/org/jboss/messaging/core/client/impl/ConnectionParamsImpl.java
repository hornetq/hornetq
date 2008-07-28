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

package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.ConnectionParams;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ConnectionParamsImpl implements ConnectionParams
{
   //Constants ---------------------------------------------------------------------------------------
      
   public static final int DEFAULT_PING_INTERVAL = 10000; // in ms
     
   public static final int DEFAULT_CALL_TIMEOUT = 5000; // in ms
   
   public static final boolean DEFAULT_INVM_OPTIMISATION_ENABLED = true;
   
   public static final boolean DEFAULT_TCP_NODELAY = true;
   
   public static final int DEFAULT_TCP_RECEIVE_BUFFER_SIZE = 32 * 1024; // in bytes
   
   public static final int DEFAULT_TCP_SEND_BUFFER_SIZE = 32 * 1024; // in bytes
   
   public static final boolean DEFAULT_SSL_ENABLED = false;
   
   public static final String SSL_KEYSTORE_PATH_PROPERTY_NAME = "jbm.remoting.ssl.keystore.path";
   
   public static final String SSL_KEYSTORE_PASSWORD_PROPERTY_NAME = "jbm.remoting.ssl.keystore.password";
   
   public static final String SSL_TRUSTSTORE_PATH_PROPERTY_NAME = "jbm.remoting.ssl.truststore.path";
   
   public static final String SSL_TRUSTSTORE_PASSWORD_PROPERTY_NAME = "jbm.remoting.ssl.truststore.password";
   
   public static final String ENABLE_SSL_PROPERTY_NAME = "jbm.remoting.enable.ssl";
   
   
   
   private static final long serialVersionUID = 1662480686951551534L;
   
   // Attributes -------------------------------------------------------------------------------------
   
   private long callTimeout = DEFAULT_CALL_TIMEOUT;
   
   private long pingInterval = DEFAULT_PING_INTERVAL;
   
   private boolean inVMOptimisationEnabled = DEFAULT_INVM_OPTIMISATION_ENABLED;
   
   private boolean tcpNoDelay = DEFAULT_TCP_NODELAY;
   
   private int tcpReceiveBufferSize = DEFAULT_TCP_RECEIVE_BUFFER_SIZE;
   
   private int tcpSendBufferSize = DEFAULT_TCP_SEND_BUFFER_SIZE;
   
   private boolean sslEnabled = DEFAULT_SSL_ENABLED;
   
   private String keyStorePath;
   
   private String keyStorePassword;
   
   private String trustStorePath;
   
   private String trustStorePassword;
   
   public long getCallTimeout()
   {
      return callTimeout;
   }

   public void setCallTimeout(final long timeout)
   {
      this.callTimeout = timeout;
   }

   public long getPingInterval()
   {
      return pingInterval;
   }

   public void setPingInterval(final long pingInterval)
   {
      this.pingInterval = pingInterval;
   }
   public boolean isInVMOptimisationEnabled()
   {
      return inVMOptimisationEnabled;
   }

   public void setInVMOptimisationEnabled(final boolean enabled)
   {
      this.inVMOptimisationEnabled = enabled;
   }

   public boolean isTcpNoDelay()
   {
      return tcpNoDelay;
   }

   public void setTcpNoDelay(final boolean tcpNoDelay)
   {
      this.tcpNoDelay = tcpNoDelay;
   }

   public int getTcpReceiveBufferSize()
   {
      return tcpReceiveBufferSize;
   }

   public void setTcpReceiveBufferSize(final int tcpReceiveBufferSize)
   {
      this.tcpReceiveBufferSize = tcpReceiveBufferSize;
   }

   public int getTcpSendBufferSize()
   {
      return tcpSendBufferSize;
   }

   public void setTcpSendBufferSize(final int tcpSendBufferSize)
   {
      this.tcpSendBufferSize = tcpSendBufferSize;
   }

   public boolean isSSLEnabled()
   {
      String sslEnabledProperty = System.getProperty(ENABLE_SSL_PROPERTY_NAME);
      
      return sslEnabledProperty == null ? sslEnabled : sslEnabledProperty.equalsIgnoreCase("true");
   }

   public void setSSLEnabled(final boolean sslEnabled)
   {
      this.sslEnabled = sslEnabled;
   }

   public String getKeyStorePath()
   {
      String sslKeystorePath = System.getProperty(SSL_KEYSTORE_PATH_PROPERTY_NAME);
      
      return sslKeystorePath == null ? keyStorePath : sslKeystorePath;
   }

   public void setKeyStorePath(final String keyStorePath)
   {
      this.keyStorePath = keyStorePath;
   }

   public String getKeyStorePassword()
   {
      String keyStorePass = System.getProperty(SSL_KEYSTORE_PASSWORD_PROPERTY_NAME);
      
      return keyStorePass == null ? keyStorePassword : keyStorePass;
   }

   public void setKeyStorePassword(final String keyStorePassword)
   {
      this.keyStorePassword = keyStorePassword;
   }

   public String getTrustStorePath()
   {
      String sslTruststorePath = System.getProperty(SSL_TRUSTSTORE_PATH_PROPERTY_NAME);
      
      return sslTruststorePath == null ? trustStorePath : sslTruststorePath;
   }

   public void setTrustStorePath(final String trustStorePath)
   {
      this.trustStorePath = trustStorePath;
   }

   public String getTrustStorePassword()
   {
      String trustStorePass = System.getProperty(SSL_TRUSTSTORE_PASSWORD_PROPERTY_NAME);
      
      return trustStorePass == null ? trustStorePassword : trustStorePass;
   }

   public void setTrustStorePassword(final String trustStorePassword)
   {
      this.trustStorePassword = trustStorePassword;
   }
        
   public boolean equals(Object other)
   {
      if (other instanceof ConnectionParams == false)
      {
         return false;
      }
      
      ConnectionParams cp = (ConnectionParams)other;
      
      return cp.getCallTimeout() == callTimeout &&
             cp.getPingInterval() == this.pingInterval &&
             cp.isInVMOptimisationEnabled() == this.isInVMOptimisationEnabled() &&
             cp.isTcpNoDelay() == this.isTcpNoDelay() &&
             cp.getTcpReceiveBufferSize() == this.getTcpReceiveBufferSize() &&
             cp.getTcpSendBufferSize() == this.getTcpSendBufferSize() &&
             cp.isSSLEnabled() == this.isSSLEnabled();
   }
}
