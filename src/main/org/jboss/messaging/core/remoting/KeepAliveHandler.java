package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;

/**
 * pluggable component that defines how a client responds to a server ping command
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface KeepAliveHandler
{
   Pong ping(Ping pong);
}
