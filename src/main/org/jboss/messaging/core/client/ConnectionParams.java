package org.jboss.messaging.core.client;

import java.io.Serializable;

/**
 * A set of connection params used by the client connection.
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface ConnectionParams extends Serializable
{
   int DEFAULT_KEEP_ALIVE_INTERVAL = 10; // in seconds
   int DEFAULT_KEEP_ALIVE_TIMEOUT = 5; // in seconds
   int DEFAULT_REQRES_TIMEOUT = 5; // in seconds
   boolean DEFAULT_INVM_DISABLED = false;
   boolean DEFAULT_SSL_ENABLED = false;
   public final static String REMOTING_SSL_KEYSTORE_PATH = "remoting.ssl.keystore.path";
   public final static String REMOTING_SSL_KEYSTORE_PASSWORD = "remoting.ssl.keystore.password";
   public final static String REMOTING_SSL_TRUSTSTORE_PATH = "remoting.ssl.truststore.path";
   public final static String REMOTING_SSL_TRUSTSTORE_PASSWORD = "remoting.ssl.truststore.password";
   public final static String REMOTING_ENABLE_SSL = "remoting.enable.ssl";

   int getTimeout();

   void setTimeout(int timeout);

   int getKeepAliveInterval();

   void setKeepAliveInterval(int keepAliveInterval);

   int getKeepAliveTimeout();

   void setKeepAliveTimeout(int keepAliveTimeout);

   boolean isInvmDisabled();

   void setInvmDisabled(boolean invmDisabled);

   boolean isInvmDisabledModified();

   void setInvmDisabledModified(boolean invmDisabledModified);

   boolean isTcpNoDelay();

   void setTcpNoDelay(boolean tcpNoDelay);
   
   long getWriteQueueMaxBytes();
   
   void setWriteQueueMaxBytes(long maxBytes);
   
   long getWriteQueueMinBytes();
   
   void setWriteQueueMinBytes(long minBytes);
   
   long getWriteQueueBlockTimeout();
   
   void setWriteQueueBlockTimeout(long timeout);

   int getTcpReceiveBufferSize();

   void setTcpReceiveBufferSize(int tcpReceiveBufferSize);

   int getTcpSendBufferSize();

   void setTcpSendBufferSize(int tcpSendBufferSize);

   boolean isSSLEnabled();

   void setSSLEnabled(boolean sslEnabled);

   boolean isSSLEnabledModified();

   void setSSLEnabledModified(boolean sslEnabledModified);

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
