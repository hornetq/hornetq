package org.jboss.messaging.core.server;

import org.jboss.messaging.core.remoting.impl.wireformat.Pong;
import org.jboss.messaging.core.remoting.impl.mina.CleanUpNotifier;
import org.jboss.messaging.core.remoting.PacketReturner;

/**
 * Used by a MessagingServer to detect that a client is still alive.
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface ClientPinger extends Runnable
{
   /**
    * this will be scheduled to run at the keep alive interval period
    */
   void run();


}
