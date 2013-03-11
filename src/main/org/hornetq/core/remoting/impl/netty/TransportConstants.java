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
package org.hornetq.core.remoting.impl.netty;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.hornetq.core.config.Configuration;
import org.hornetq.spi.core.protocol.ProtocolType;

/**
 * A TransportConstants
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransportConstants
{
   public static final String SSL_ENABLED_PROP_NAME = "ssl-enabled";

   public static final String HTTP_ENABLED_PROP_NAME = "http-enabled";

   public static final String HTTP_CLIENT_IDLE_PROP_NAME = "http-client-idle-time";

   public static final String HTTP_CLIENT_IDLE_SCAN_PERIOD = "http-client-idle-scan-period";

   public static final String HTTP_RESPONSE_TIME_PROP_NAME = "http-response-time";

   public static final String HTTP_SERVER_SCAN_PERIOD_PROP_NAME = "http-server-scan-period";

   public static final String HTTP_REQUIRES_SESSION_ID = "http-requires-session-id";

   public static final String USE_SERVLET_PROP_NAME = "use-servlet";

   public static final String SERVLET_PATH = "servlet-path";

   public static final String USE_NIO_PROP_NAME = "use-nio";

   public static final String USE_INVM_PROP_NAME = "use-invm";

   public static final String PROTOCOL_PROP_NAME = "protocol";

   public static final String HOST_PROP_NAME = "host";

   public static final String PORT_PROP_NAME = "port";

   public static final String KEYSTORE_PATH_PROP_NAME = "key-store-path";

   public static final String KEYSTORE_PASSWORD_PROP_NAME = "key-store-password";

   public static final String TRUSTSTORE_PATH_PROP_NAME = "trust-store-path";

   public static final String TRUSTSTORE_PASSWORD_PROP_NAME = "trust-store-password";

   public static final String NEED_CLIENT_AUTH_PROP_NAME = "need-client-auth";

   public static final String TCP_NODELAY_PROPNAME = "tcp-no-delay";

   public static final String TCP_SENDBUFFER_SIZE_PROPNAME = "tcp-send-buffer-size";

   public static final String TCP_RECEIVEBUFFER_SIZE_PROPNAME = "tcp-receive-buffer-size";

   public static final String NIO_REMOTING_THREADS_PROPNAME = "nio-remoting-threads";

   public static final String BATCH_DELAY = "batch-delay";

   public static final String DIRECT_DELIVER = "direct-deliver";

   public static final String CLUSTER_CONNECTION = "cluster-connection";

   public static final String STOMP_CONSUMERS_CREDIT = "stomp-consumer-credits";

   public static final int STOMP_DEFAULT_CONSUMERS_CREDIT = 10 * 1024; // 10K

   public static final boolean DEFAULT_SSL_ENABLED = false;

   public static final boolean DEFAULT_USE_NIO_SERVER = false;

   // For client, using old IO can be quicker
   public static final boolean DEFAULT_USE_NIO_CLIENT = false;

   public static final boolean DEFAULT_USE_INVM = false;

   public static final boolean DEFAULT_USE_SERVLET = false;

   public static final String DEFAULT_PROTOCOL = ProtocolType.CORE.toString();

   public static final String DEFAULT_HOST = "localhost";

   public static final int DEFAULT_PORT = 5445;

   public static final int DEFAULT_STOMP_PORT = 61613;

   public static final String DEFAULT_KEYSTORE_PATH = null;

   public static final String DEFAULT_KEYSTORE_PASSWORD = null;

   public static final String DEFAULT_TRUSTSTORE_PATH = null;

   public static final String DEFAULT_TRUSTSTORE_PASSWORD = null;

   public static final boolean DEFAULT_NEED_CLIENT_AUTH = false;

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

   public static final long DEFAULT_BATCH_DELAY = 0;

   public static final boolean DEFAULT_DIRECT_DELIVER = true;

   public static final Set<String> ALLOWABLE_CONNECTOR_KEYS;

   public static final Set<String> ALLOWABLE_ACCEPTOR_KEYS;

   public static final String CONNECTION_TTL = "connection-ttl";

   public static final String STOMP_ENABLE_MESSAGE_ID = "stomp-enable-message-id";

   static
   {
      Set<String> allowableAcceptorKeys = new HashSet<String>();
      allowableAcceptorKeys.add(TransportConstants.SSL_ENABLED_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.HTTP_ENABLED_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.HTTP_RESPONSE_TIME_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.HTTP_SERVER_SCAN_PERIOD_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.USE_NIO_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.USE_INVM_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.PROTOCOL_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.HOST_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.PORT_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.KEYSTORE_PATH_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.TRUSTSTORE_PATH_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.NEED_CLIENT_AUTH_PROP_NAME);
      allowableAcceptorKeys.add(TransportConstants.TCP_NODELAY_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.NIO_REMOTING_THREADS_PROPNAME);
      allowableAcceptorKeys.add(TransportConstants.BATCH_DELAY);
      allowableAcceptorKeys.add(TransportConstants.DIRECT_DELIVER);
      allowableAcceptorKeys.add(TransportConstants.CLUSTER_CONNECTION);
      allowableAcceptorKeys.add(TransportConstants.STOMP_CONSUMERS_CREDIT);
      allowableAcceptorKeys.add(TransportConstants.CONNECTION_TTL);
      allowableAcceptorKeys.add(TransportConstants.STOMP_ENABLE_MESSAGE_ID);
      allowableAcceptorKeys.add(Configuration.PROP_MASK_PASSWORD);
      allowableAcceptorKeys.add(Configuration.PROP_PASSWORD_CODEC);

      ALLOWABLE_ACCEPTOR_KEYS = Collections.unmodifiableSet(allowableAcceptorKeys);

      Set<String> allowableConnectorKeys = new HashSet<String>();
      allowableConnectorKeys.add(TransportConstants.SSL_ENABLED_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.HTTP_ENABLED_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.HTTP_CLIENT_IDLE_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.HTTP_CLIENT_IDLE_SCAN_PERIOD);
      allowableConnectorKeys.add(TransportConstants.HTTP_REQUIRES_SESSION_ID);
      allowableConnectorKeys.add(TransportConstants.USE_SERVLET_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.SERVLET_PATH);
      allowableConnectorKeys.add(TransportConstants.USE_NIO_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.HOST_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.PORT_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.KEYSTORE_PATH_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.TRUSTSTORE_PATH_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME);
      allowableConnectorKeys.add(TransportConstants.TCP_NODELAY_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.TCP_SENDBUFFER_SIZE_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.TCP_RECEIVEBUFFER_SIZE_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.NIO_REMOTING_THREADS_PROPNAME);
      allowableConnectorKeys.add(TransportConstants.BATCH_DELAY);
      allowableConnectorKeys.add(Configuration.PROP_MASK_PASSWORD);
      allowableConnectorKeys.add(Configuration.PROP_PASSWORD_CODEC);

      ALLOWABLE_CONNECTOR_KEYS = Collections.unmodifiableSet(allowableConnectorKeys);
   }

}
