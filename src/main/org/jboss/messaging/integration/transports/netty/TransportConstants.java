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
package org.jboss.messaging.integration.transports.netty;

/**
 * A TransportConstants
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransportConstants
{
   public static final String SSL_ENABLED_PROP_NAME = "jbm.remoting.netty.sslenabled";

   public static final String HTTP_ENABLED_PROP_NAME = "jbm.remoting.netty.httpenabled";

   public static final String HTTP_CLIENT_IDLE_PROP_NAME = "jbm.remoting.netty.httpclientidletime";

   public static final String HTTP_CLIENT_IDLE_SCAN_PERIOD = "jbm.remoting.netty.httpclientidlescanperiod";

   public static final String HTTP_RESPONSE_TIME_PROP_NAME = "jbm.remoting.netty.httpresponsetime";

   public static final String HTTP_SERVER_SCAN_PERIOD_PROP_NAME = "jbm.remoting.netty.httpserverscanperiod";

   public static final String HTTP_REQUIRES_SESSION_ID = "jbm.remoting.netty.httprequiressessionid";

   public static final String USE_SERVLET_PROP_NAME = "jbm.remoting.netty.useservlet";

   public static final String USE_NIO_PROP_NAME = "jbm.remoting.netty.usenio";

   public static final String HOST_PROP_NAME = "jbm.remoting.netty.host";

   public static final String PORT_PROP_NAME = "jbm.remoting.netty.port";

   public static final String KEYSTORE_PATH_PROP_NAME = "jbm.remoting.netty.keystorepath";

   public static final String KEYSTORE_PASSWORD_PROP_NAME = "jbm.remoting.netty.keystorepassword";

   public static final String TRUSTSTORE_PATH_PROP_NAME = "jbm.remoting.netty.truststorepath";

   public static final String TRUSTSTORE_PASSWORD_PROP_NAME = "jbm.remoting.netty.truststorepassword";

   public static final String TCP_NODELAY_PROPNAME = "jbm.remoting.netty.tcpnodelay";

   public static final String TCP_SENDBUFFER_SIZE_PROPNAME = "jbm.remoting.netty.tcpsendbuffersize";

   public static final String TCP_RECEIVEBUFFER_SIZE_PROPNAME = "jbm.remoting.netty.tcpreceivebuffersize";

   public static final boolean DEFAULT_SSL_ENABLED = false;

   public static final boolean DEFAULT_USE_NIO = true;

   public static final boolean DEFAULT_USE_SERVLET = false;

   public static final String DEFAULT_HOST = "localhost";

   public static final int DEFAULT_PORT = 5445;

   public static final String DEFAULT_KEYSTORE_PATH = "messaging.keystore";

   public static final String DEFAULT_KEYSTORE_PASSWORD = "secureexample";

   public static final String DEFAULT_TRUSTSTORE_PATH = "messaging.truststore";

   public static final String DEFAULT_TRUSTSTORE_PASSWORD = "secureexample";

   public static final boolean DEFAULT_TCP_NODELAY = true;

   public static final int DEFAULT_TCP_SENDBUFFER_SIZE = 32768;

   public static final int DEFAULT_TCP_RECEIVEBUFFER_SIZE = 32768;

   public static final boolean DEFAULT_HTTP_ENABLED = false;

   public static final long DEFAULT_HTTP_CLIENT_IDLE_TIME = 500;

   public static final long DEFAULT_HTTP_CLIENT_SCAN_PERIOD = 500;

   public static final long DEFAULT_HTTP_RESPONSE_TIME = 10000;

   public static final long DEFAULT_HTTP_SERVER_SCAN_PERIOD = 5000;

   public static final boolean DEFAULT_HTTP_REQUIRES_SESSION_ID = false;
}
