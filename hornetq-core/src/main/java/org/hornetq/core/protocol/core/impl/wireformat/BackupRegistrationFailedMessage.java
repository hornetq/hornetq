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

   enum BackupRegistrationProblem
   {
      EXCEPTION(0), AUTHENTICATION(1);
      final int code;

      private BackupRegistrationProblem(int code)
      {
         this.code = code;
      }
   }

   int errorCode = -1;
   BackupRegistrationProblem problem;

   public BackupRegistrationFailedMessage(HornetQException e)
   {
      super(BACKUP_REGISTRATION_FAILED);
      if (e != null)
      {
         errorCode = e.getCode();
         problem = BackupRegistrationProblem.EXCEPTION;
      }
      else
      {
         problem = BackupRegistrationProblem.AUTHENTICATION;
      }
   }

   public BackupRegistrationFailedMessage()
   {
      super(BACKUP_REGISTRATION_FAILED);
   }

   public int getCause()
   {
      return errorCode;
   }

   public BackupRegistrationProblem getRegistrationProblem()
   {
      return problem;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(problem.code);
      if (problem == BackupRegistrationProblem.EXCEPTION)
      {
         buffer.writeInt(errorCode);
      }
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      if (buffer.readInt() == BackupRegistrationProblem.AUTHENTICATION.code)
      {
         problem = BackupRegistrationProblem.AUTHENTICATION;
      }
      else
      {
         problem = BackupRegistrationProblem.EXCEPTION;
         errorCode = buffer.readInt();
      }
   }
}
