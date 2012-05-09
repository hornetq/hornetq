/**
 *
 */
package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
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

   HornetQExceptionType errorCode = null;
   BackupRegistrationProblem problem;

   public BackupRegistrationFailedMessage(HornetQException e)
   {
      super(BACKUP_REGISTRATION_FAILED);
      if (e != null)
      {
         errorCode = e.getType();
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

   public HornetQExceptionType getCause()
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
         buffer.writeInt(errorCode != null?errorCode.getCode():-1);
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
         errorCode = HornetQExceptionType.getType(buffer.readInt());
      }
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (errorCode != null?errorCode.getCode():-1);
      result = prime * result + ((problem == null) ? 0 : problem.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
      {
         return true;
      }
      if (!super.equals(obj))
      {
         return false;
      }
      if (!(obj instanceof BackupRegistrationFailedMessage))
      {
         return false;
      }
      BackupRegistrationFailedMessage other = (BackupRegistrationFailedMessage)obj;
      if (errorCode != other.errorCode)
      {
         return false;
      }
      if (problem != other.problem)
      {
         return false;
      }
      return true;
   }
}
