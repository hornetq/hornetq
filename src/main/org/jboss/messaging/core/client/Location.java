package org.jboss.messaging.core.client;

import org.jboss.messaging.core.remoting.TransportType;

import java.io.Serializable;

/**
 * The location of a JBM server and the type of transport to use. Used by clients when creating a connection factory
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface Location extends Serializable
{
   int DEFAULT_REMOTING_PORT = 5400;

   String getLocation();

   TransportType getTransport();

   String getHost();

   int getPort();
}
