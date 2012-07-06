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
 * @see org.hornetq.core.server.impl.ReplicationError
 */
public final class BackupReplicationStartFailedMessage extends PacketImpl
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

   private HornetQExceptionType errorCode;
   private BackupRegistrationProblem problem;
   private String msg;

   public BackupReplicationStartFailedMessage(HornetQException e)
   {
      super(BACKUP_REGISTRATION_FAILED);
      if (e != null)
      {
         msg = e.getMessage();
         errorCode = e.getType();
         problem = BackupRegistrationProblem.EXCEPTION;
      }
      else
      {
         problem = BackupRegistrationProblem.AUTHENTICATION;
      }
   }

   public BackupReplicationStartFailedMessage()
   {
      super(BACKUP_REGISTRATION_FAILED);
   }

   public HornetQExceptionType getCause()
   {
      return errorCode;
   }

   public HornetQException getException() {
      if (errorCode == null)
         return null;
      return HornetQExceptionType.createException(errorCode.getCode(), msg);
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
         buffer.writeNullableString(msg);
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
         msg = buffer.readNullableString();
      }
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((errorCode == null) ? 0 : errorCode.hashCode());
      result = prime * result + ((msg == null) ? 0 : msg.hashCode());
      result = prime * result + ((problem == null) ? 0 : problem.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof BackupReplicationStartFailedMessage))
         return false;
      BackupReplicationStartFailedMessage other = (BackupReplicationStartFailedMessage)obj;
      if (errorCode != other.errorCode)
         return false;
      if (msg == null)
      {
         if (other.msg != null)
            return false;
      }
      else if (!msg.equals(other.msg))
         return false;
      if (problem != other.problem)
         return false;
      return true;
   }
}
