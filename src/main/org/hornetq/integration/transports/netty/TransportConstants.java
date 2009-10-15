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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A TransportConstants
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransportConstants
{
   public static final String SSL_ENABLED_PROP_NAME = "hornetq.remoting.netty.sslenabled";

   public static final String HTTP_ENABLED_PROP_NAME = "hornetq.remoting.netty.httpenabled";

   public static final String HTTP_CLIENT_IDLE_PROP_NAME = "hornetq.remoting.netty.httpclientidletime";

   public static final String HTTP_CLIENT_IDLE_SCAN_PERIOD = "hornetq.remoting.netty.httpclientidlescanperiod";

   public static final String HTTP_RESPONSE_TIME_PROP_NAME = "hornetq.remoting.netty.httpresponsetime";

   public static final String HTTP_SERVER_SCAN_PERIOD_PROP_NAME = "hornetq.remoting.netty.httpserverscanperiod";

   public static final String HTTP_REQUIRES_SESSION_ID = "hornetq.remoting.netty.httprequiressessionid";

   public static final String USE_SERVLET_PROP_NAME = "hornetq.remoting.netty.useservlet";

   public static final String SERVLET_PATH = "hornetq.remoting.netty.servletpath";

   public static final String USE_NIO_PROP_NAME = "hornetq.remoting.netty.usenio";

   public static final String USE_INVM_PROP_NAME = "hornetq.remoting.netty.useinvm";

   public static final String HOST_PROP_NAME = "hornetq.remoting.netty.host";

   public static final String PORT_PROP_NAME = "hornetq.remoting.netty.port";

   public static final String KEYSTORE_PATH_PROP_NAME = "hornetq.remoting.netty.keystorepath";

   public static final String KEYSTORE_PASSWORD_PROP_NAME = "hornetq.remoting.netty.keystorepassword";

   public static final String TRUSTSTORE_PATH_PROP_NAME = "hornetq.remoting.netty.truststorepath";

   public static final String TRUSTSTORE_PASSWORD_PROP_NAME = "hornetq.remoting.netty.truststorepassword";

   public static final String TCP_NODELAY_PROPNAME = "hornetq.remoting.netty.tcpnodelay";

   public static final String TCP_SENDBUFFER_SIZE_PROPNAME = "hornetq.remoting.netty.tcpsendbuffersize";

   public static final String TCP_RECEIVEBUFFER_SIZE_PROPNAME = "hornetq.remoting.netty.tcpreceivebuffersize";

   public static final boolean DEFAULT_SSL_ENABLED = false;

   public static final boolean DEFAULT_USE_NIO = true;

   public static final boolean DEFAULT_USE_INVM = false;

   public static final boolean DEFAULT_USE_SERVLET = false;

   public static final String DEFAULT_HOST = "localhost";

   public static final int DEFAULT_PORT = 5445;

   public static final String DEFAULT_KEYSTORE_PATH = "hornetq.keystore";

   public static final String DEFAULT_KEYSTORE_PASSWORD = "secureexample";

   public static final String DEFAULT_TRUSTSTORE_PATH = "hornetq.truststore";

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

   public static final String DEFAULT_SERVLET_PATH = "/messaging/HornetQServlet";

   public static final Set<String> ALLOWABLE_CONNECTOR_KEYS;

   public static final Set<String> ALLOWABLE_ACCEPTOR_KEYS;

   static
   {
      Set<String> allowableAcceptorKeys = new HashSet<String>();
      allowableAcceptorKeys.add(SSL_ENABLED_PROP_NAME);
      allowableAcceptorKeys.add(HTTP_ENABLED_PROP_NAME);
      allowableAcceptorKeys.add(HTTP_RESPONSE_TIME_PROP_NAME);
      allowableAcceptorKeys.add(HTTP_SERVER_SCAN_PERIOD_PROP_NAME);                  
      allowableAcceptorKeys.add(USE_NIO_PROP_NAME);
      allowableAcceptorKeys.add(USE_INVM_PROP_NAME);
      allowableAcceptorKeys.add(HOST_PROP_NAME);
      allowableAcceptorKeys.add(PORT_PROP_NAME);
      allowableAcceptorKeys.add(KEYSTORE_PATH_PROP_NAME);
      allowableAcceptorKeys.add(KEYSTORE_PASSWORD_PROP_NAME);
      allowableAcceptorKeys.add(TRUSTSTORE_PATH_PROP_NAME);
      allowableAcceptorKeys.add(TRUSTSTORE_PASSWORD_PROP_NAME);
      allowableAcceptorKeys.add(TCP_NODELAY_PROPNAME);
      allowableAcceptorKeys.add(TCP_SENDBUFFER_SIZE_PROPNAME);
      allowableAcceptorKeys.add(TCP_RECEIVEBUFFER_SIZE_PROPNAME);
      
      ALLOWABLE_CONNECTOR_KEYS = Collections.unmodifiableSet(allowableAcceptorKeys);
      
      Set<String> allowableConnectorKeys = new HashSet<String>();
      allowableConnectorKeys.add(SSL_ENABLED_PROP_NAME);
      allowableConnectorKeys.add(HTTP_ENABLED_PROP_NAME);
      allowableConnectorKeys.add(HTTP_CLIENT_IDLE_PROP_NAME);
      allowableConnectorKeys.add(HTTP_CLIENT_IDLE_SCAN_PERIOD);           
      allowableConnectorKeys.add(HTTP_REQUIRES_SESSION_ID);
      allowableConnectorKeys.add(USE_SERVLET_PROP_NAME);
      allowableConnectorKeys.add(SERVLET_PATH);
      allowableConnectorKeys.add(USE_NIO_PROP_NAME);      
      allowableConnectorKeys.add(HOST_PROP_NAME);
      allowableConnectorKeys.add(PORT_PROP_NAME);
      allowableConnectorKeys.add(KEYSTORE_PATH_PROP_NAME);
      allowableConnectorKeys.add(KEYSTORE_PASSWORD_PROP_NAME);          
      allowableConnectorKeys.add(TCP_NODELAY_PROPNAME);
      allowableConnectorKeys.add(TCP_SENDBUFFER_SIZE_PROPNAME);
      allowableConnectorKeys.add(TCP_RECEIVEBUFFER_SIZE_PROPNAME);
      
      ALLOWABLE_ACCEPTOR_KEYS = Collections.unmodifiableSet(allowableConnectorKeys);
   }

}
