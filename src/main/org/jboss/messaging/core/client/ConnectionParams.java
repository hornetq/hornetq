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
package org.jboss.messaging.core.client;

import java.io.Serializable;

/**
 * A set of connection params used by the client connection.
 * 
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 */
public interface ConnectionParams extends Serializable
{   
   long getCallTimeout();

   void setCallTimeout(long timeout);

   long getPingInterval();

   void setPingInterval(long pingInterval);

   long getPingTimeout();

   void setPingTimeout(long pingTimeout);

   boolean isInVMDisabled();

   void setInVMDisabled(boolean invmDisabled);

   boolean isTcpNoDelay();

   void setTcpNoDelay(boolean tcpNoDelay);   

   int getTcpReceiveBufferSize();

   void setTcpReceiveBufferSize(int tcpReceiveBufferSize);

   int getTcpSendBufferSize();

   void setTcpSendBufferSize(int tcpSendBufferSize);

   boolean isSSLEnabled();

   void setSSLEnabled(boolean sslEnabled);

   String getKeyStorePath();

   void setKeyStorePath(String keyStorePath);

   String getKeyStorePassword();

   void setKeyStorePassword(String keyStorePassword);

   String getTrustStorePath();

   void setTrustStorePath(String trustStorePath);

   String getTrustStorePassword();

   void setTrustStorePassword(String trustStorePassword);

   String getURI();
}
