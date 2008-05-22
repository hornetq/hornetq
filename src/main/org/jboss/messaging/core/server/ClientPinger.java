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

   /**
    * pong received from client
    * @param pong the pong
    */
   void pong(Pong pong);

   /**
    * register a connection.
    *
    * @param remotingSessionID  the session id
    * @param sender the sender
    */
   void registerConnection(long remotingSessionID, PacketReturner sender);

   /**
    * unregister a connection.
    *
    * @param remotingSessionID the session id
    */
   void unregister(long remotingSessionID);

   /**
    * register the cleanup notifier to use
    *
    * @param cleanUpNotifier the notifier
    */
   void registerCleanUpNotifier(CleanUpNotifier cleanUpNotifier);
}
