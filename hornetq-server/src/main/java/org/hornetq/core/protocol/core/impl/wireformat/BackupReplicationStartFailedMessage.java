/**
 *
 */
package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.protocol.core.impl.PacketImpl;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Informs the Backup trying to start replicating of an error.
 * @see org.hornetq.core.server.impl.ReplicationError
 */
public final class BackupReplicationStartFailedMessage extends PacketImpl
{

   public enum BackupRegistrationProblem
   {
      EXCEPTION(0), AUTHENTICATION(1), ALREADY_REPLICATING(2);

      private static final Map<Integer, BackupRegistrationProblem> TYPE_MAP;

      final int code;

      private BackupRegistrationProblem(int code)
      {
         this.code = code;
      }

      static
      {
         HashMap<Integer, BackupRegistrationProblem> map = new HashMap<Integer, BackupRegistrationProblem>();
         for (BackupRegistrationProblem type : EnumSet.allOf(BackupRegistrationProblem.class))
         {
            map.put(type.code, type);
         }
         TYPE_MAP = Collections.unmodifiableMap(map);
      }
   }

   private BackupRegistrationProblem problem;

   private static BackupRegistrationProblem getType(int type)
   {
      return BackupRegistrationProblem.TYPE_MAP.get(type);
   }

   public BackupReplicationStartFailedMessage(BackupRegistrationProblem registrationProblem)
   {
      super(BACKUP_REGISTRATION_FAILED);
      problem = registrationProblem;
   }

   public BackupReplicationStartFailedMessage()
   {
      super(BACKUP_REGISTRATION_FAILED);
   }

   public BackupRegistrationProblem getRegistrationProblem()
   {
      return problem;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(problem.code);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      problem = getType(buffer.readInt());
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      BackupReplicationStartFailedMessage that = (BackupReplicationStartFailedMessage) o;

      if (problem != that.problem) return false;

      return true;
   }

   @Override
   public int hashCode()
   {
      int result = super.hashCode();
      result = 31 * result + (problem != null ? problem.hashCode() : 0);
      return result;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", problem=" + problem.name() + "]";
   }
}
