/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.integration.transports.netty;

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

   public static final String SERVLET_PATH = "jbm.remoting.netty.servletpath";

   public static final String USE_NIO_PROP_NAME = "jbm.remoting.netty.usenio";

   public static final String USE_INVM_PROP_NAME = "jbm.remoting.netty.useinvm";

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

   public static final boolean DEFAULT_USE_INVM = false;

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

   public static final String DEFAULT_SERVLET_PATH = "/messaging/JBMServlet";

}
