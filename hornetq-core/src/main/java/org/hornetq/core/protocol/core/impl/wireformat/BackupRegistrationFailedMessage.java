/**
 *
 */
package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * Informs the Backup trying to start replicating of an error.
 */
public final class BackupRegistrationFailedMessage extends PacketImpl
{

   int errorCode;

   public BackupRegistrationFailedMessage(HornetQException e)
   {
      super(BACKUP_REGISTRATION_FAILED);
      errorCode = e.getCode();
   }

   public int getCause()
   {
      return errorCode;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(errorCode);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      errorCode = buffer.readInt();
   }
}
