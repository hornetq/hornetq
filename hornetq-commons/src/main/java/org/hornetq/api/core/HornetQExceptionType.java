package org.hornetq.api.core;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum HornetQExceptionType
{

   // Error codes -------------------------------------------------

   INTERNAL_ERROR(000)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQInternalErrorException(msg);
      }
   },
   UNSUPPORTED_PACKET(001)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQUnsupportedPacketException(msg);
      }
   },
   NOT_CONNECTED(002)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQNotConnectedException(msg);
      }
   },
   CONNECTION_TIMEDOUT(003)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQConnectionTimedOutException(msg);
      }
   },
   DISCONNECTED(004)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQDisconnectedException(msg);
      }
   },
   UNBLOCKED(005)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQUnBlockedException(msg);
      }
   },
   IO_ERROR(006)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQIOErrorException(msg);
      }
   },
   QUEUE_DOES_NOT_EXIST(100)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQNonExistentQueueException(msg);
      }
   },
   QUEUE_EXISTS(101)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQQueueExistsException(msg);
      }
   },
   OBJECT_CLOSED(102)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQObjectClosedException(msg);
      }
   },
   INVALID_FILTER_EXPRESSION(103)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQInvalidFilterExpressionException(msg);
      }
   },
   ILLEGAL_STATE(104)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQIllegalStateException(msg);
      }
   },
   SECURITY_EXCEPTION(105)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQSecurityException(msg);
      }
   },
   ADDRESS_EXISTS(107)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQAddressExistsException(msg);
      }
   },
   INCOMPATIBLE_CLIENT_SERVER_VERSIONS(108)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQIncompatibleClientServerException(msg);
      }
   },
   LARGE_MESSAGE_ERROR_BODY(110)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQLargeMessageException(msg);
      }
   },
   TRANSACTION_ROLLED_BACK(111)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQTransactionRolledBackException(msg);
      }
   },
   SESSION_CREATION_REJECTED(112)
   {
         @Override
      HornetQException createException(String msg)
      {
         return new HornetQSessionCreationException(msg);
      }
   },
   DUPLICATE_ID_REJECTED(113)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQDuplicateIdException(msg);
      }
   },
   DUPLICATE_METADATA(114)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQDuplicateMetaDataException(msg);
      }
   },
   TRANSACTION_OUTCOME_UNKNOWN(115)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQTransactionOutcomeUnknownException(msg);
      }
   },
   ALREADY_REPLICATING(116)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQAlreadyReplicatingException(msg);
      }
   },
   INTERCEPTOR_REJECTED_PACKET(117)
   {
      @Override
      HornetQException createException(String msg)
      {
         return new HornetQInterceptorRejectedPacketException(msg);
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
         return new HornetQNativeIOError(msg);
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
   },
   ADDRESS_FULL(210)
   {
      @Override
      HornetQException createException(String msg)
      {
          return null;
      }
   };

   private final static Map<Integer, HornetQExceptionType> TYPE_MAP;
   static
   {
      HashMap<Integer, HornetQExceptionType> map = new HashMap<Integer, HornetQExceptionType>();
      for (HornetQExceptionType type : EnumSet.allOf(HornetQExceptionType.class))
      {
         map.put(type.getCode(), type);
      }
      TYPE_MAP = Collections.unmodifiableMap(map);
   }

   private final int code;

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
      HornetQExceptionType type = TYPE_MAP.get(code);
      if (type != null)
         return type;
      return HornetQExceptionType.GENERIC_EXCEPTION;
   }
}