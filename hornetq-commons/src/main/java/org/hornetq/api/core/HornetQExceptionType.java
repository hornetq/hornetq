package org.hornetq.api.core;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Set;

public enum HornetQExceptionType implements Serializable
{

   // Error codes -------------------------------------------------

   INTERNAL_ERROR(000)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new InternalErrorException(msg);
      }
   },
   UNSUPPORTED_PACKET(001)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new UnsupportedPacketException(msg);
      }
   },
   NOT_CONNECTED(002)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new NotConnectedException(msg);
      }
   },
   CONNECTION_TIMEDOUT(003)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new ConnectionTimedOutException(msg);
      }
   },
   DISCONNECTED(004)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new DisconnectedException(msg);
      }
   },
   UNBLOCKED(005)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new UnBlockedException(msg);
      }
   },
   IO_ERROR(006)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new IOErrorException(msg);
      }
   },
   QUEUE_DOES_NOT_EXIST(100)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new NonExistentQueueException(msg);
      }
   },
   QUEUE_EXISTS(101)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new QueueExistsException(msg);
      }
   },
   OBJECT_CLOSED(102)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new ObjectClosedException(msg);
      }
   },
   INVALID_FILTER_EXPRESSION(103)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new InvalidFilterExpressionException(msg);
      }
   },
   ILLEGAL_STATE(104)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new IllegalStateException(msg);
      }
   },
   SECURITY_EXCEPTION(105)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new SecurityException(msg);
      }
   },
   ADDRESS_EXISTS(107)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new AddressExistsException(msg);
      }
   },
   INCOMPATIBLE_CLIENT_SERVER_VERSIONS(108)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new IncompatibleClientServerException(msg);
      }
   },
   LARGE_MESSAGE_ERROR_BODY(110)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new LargeMessageException(msg);
      }
   },
   TRANSACTION_ROLLED_BACK(111)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new TransactionRolledBackException(msg);
      }
   },
   SESSION_CREATION_REJECTED(112)
   {
         @Override
      HornetQException createException(String msg)
      {
         return new SessionCreationException(msg);
      }
   },
   DUPLICATE_ID_REJECTED(113)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new DuplicateIdException(msg);
      }
   },
   DUPLICATE_METADATA(114)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new DuplicateMetaDataException(msg);
      }
   },
   TRANSACTION_OUTCOME_UNKNOWN(115)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new TransactionOutcomeUnknownException(msg);
      }
   },
   ALREADY_REPLICATING(116)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new AlreadyReplicatingException(msg);
      }
   },
   GENERIC_EXCEPTION(999)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQException(msg);
      }
   },
   NATIVE_ERROR_INTERNAL(200)
   {
      @Override
      HornetQException createException(String msg)
      {
         return null;
      }
   },
   NATIVE_ERROR_INVALID_BUFFER(201)
   {
      @Override
      HornetQException createException(String msg)
      {
         return null;
      }
   },
   NATIVE_ERROR_NOT_ALIGNED(202)
   {
      @Override
      HornetQException createException(String msg)
      {
         return null;
      }
   },
   NATIVE_ERROR_CANT_INITIALIZE_AIO(203)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new NativeIOError(msg);
      }
   },
   NATIVE_ERROR_CANT_RELEASE_AIO(204)
   {
      @Override
      HornetQException createException(String msg)
      {
         return null;
      }
   },
   NATIVE_ERROR_CANT_OPEN_CLOSE_FILE(205)
   {
      @Override
      HornetQException createException(String msg)
      {
         return null;
      }
   },
   NATIVE_ERROR_CANT_ALLOCATE_QUEUE(206)
   {
      @Override
      HornetQException createException(String msg)
      {
         return null;
      }
   },
   NATIVE_ERROR_PREALLOCATE_FILE(208)
   {
      @Override
      HornetQException createException(String msg)
      {
         return null;
      }
   },
   NATIVE_ERROR_ALLOCATE_MEMORY(209)
   {
      @Override
      HornetQException createException(String msg)
      {
         return null;
      }
   };

   private final static Set<HornetQExceptionType> SET = EnumSet.allOf(HornetQExceptionType.class);

   private int code;

   HornetQExceptionType(int code)
   {
      this.code = code;
   }

   public int getCode()
   {
      return code;
   }

   abstract HornetQException createException(String msg);

   public static HornetQException createException(int code, String msg)
   {
      return getType(code).createException(msg);
   }

   public static HornetQExceptionType getType(int code)
   {
      for (HornetQExceptionType hornetQExceptionType : SET)
      {
         if (hornetQExceptionType.getCode() == code)
         {
            return hornetQExceptionType;
         }
      }
      return GENERIC_EXCEPTION;
   }
}