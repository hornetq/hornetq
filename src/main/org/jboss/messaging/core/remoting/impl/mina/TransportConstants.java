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
package org.jboss.messaging.core.remoting.impl.mina;

/**
 * A TransportConstants
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransportConstants
{
   public static final String SSL_ENABLED_PROP_NAME = "jbm.remoting.mina.sslenabled";
   
   public static final String HOST_PROP_NAME = "jbm.remoting.mina.host";
   
   public static final String PORT_PROP_NAME = "jbm.remoting.mina.port";
   
   public static final String KEYSTORE_PATH_PROP_NAME = "jbm.remoting.mina.keystorepath";
   
   public static final String KEYSTORE_PASSWORD_PROP_NAME = "jbm.remoting.mina.keystorepassword";
   
   public static final String TRUSTSTORE_PATH_PROP_NAME = "jbm.remoting.mina.truststorepath";
   
   public static final String TRUSTSTORE_PASSWORD_PROP_NAME = "jbm.remoting.mina.truststorepassword";
   
   public static final String TCP_NODELAY_PROPNAME = "jbm.remoting.mina.tcpnodelay";
   
   public static final String TCP_SENDBUFFER_SIZE_PROPNAME = "jbm.remoting.mina.tcpsendbuffersize";
   
   public static final String TCP_RECEIVEBUFFER_SIZE_PROPNAME = "jbm.remoting.mina.tcpreceivebuffersize";
   
   public static final boolean DEFAULT_SSL_ENABLED = false;
   
   public static final String DEFAULT_HOST = "localhost";
   
   public static final int DEFAULT_PORT = 5400;
   
   public static final String DEFAULT_KEYSTORE_PATH = "messaging.keystore";
 
   public static final String DEFAULT_KEYSTORE_PASSWORD = "secureexample";    
 
   public static final String DEFAULT_TRUSTSTORE_PATH = "messaging.truststore";
 
   public static final String DEFAULT_TRUSTSTORE_PASSWORD = "secureexample";
   
   public static final boolean DEFAULT_TCP_NODELAY = true;
   
   public static final int DEFAULT_TCP_SENDBUFFER_SIZE = 32768;
   
   public static final int DEFAULT_TCP_RECEIVEBUFFER_SIZE = 32768;  
}
