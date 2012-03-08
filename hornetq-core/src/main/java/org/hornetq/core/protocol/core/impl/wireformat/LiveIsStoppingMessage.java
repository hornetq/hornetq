/**
 *
 */
package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * Message indicating that the live is stopping.
 * <p>
 * The backup starts the fail-over immediately after receiving this.
 */
public final class LiveIsStoppingMessage extends PacketImpl
{

   public LiveIsStoppingMessage()
   {
      super(PacketImpl.REPLICATION_SCHEDULED_FAILOVER);
   }
}
