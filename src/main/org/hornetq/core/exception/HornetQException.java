/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.exception;

/**
 * 
 * A HornetQException
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class HornetQException extends Exception
{
   private static final long serialVersionUID = -4802014152804997417L;

   // Error codes -------------------------------------------------

   public static final int INTERNAL_ERROR = 000;

   public static final int UNSUPPORTED_PACKET = 001;

   public static final int NOT_CONNECTED = 002;

   public static final int CONNECTION_TIMEDOUT = 003;

   public static final int DISCONNECTED = 004;
   
   public static final int UNBLOCKED = 005;

   public static final int IO_ERROR = 006;


   public static final int QUEUE_DOES_NOT_EXIST = 100;

   public static final int QUEUE_EXISTS = 101;

   public static final int OBJECT_CLOSED = 102;

   public static final int INVALID_FILTER_EXPRESSION = 103;

   public static final int ILLEGAL_STATE = 104;

   public static final int SECURITY_EXCEPTION = 105;

   public static final int ADDRESS_DOES_NOT_EXIST = 106;

   public static final int ADDRESS_EXISTS = 107;

   public static final int INCOMPATIBLE_CLIENT_SERVER_VERSIONS = 108;

   public static final int SESSION_EXISTS = 109;
   
   public static final int LARGE_MESSAGE_ERROR_BODY = 110;
   
   public static final int TRANSACTION_ROLLED_BACK = 111;
   
   public static final int SESSION_CREATION_REJECTED = 112;

   
   // Native Error codes ----------------------------------------------
   
   public static final int NATIVE_ERROR_INTERNAL = 200;

   public static final int NATIVE_ERROR_INVALID_BUFFER = 201;

   public static final int NATIVE_ERROR_NOT_ALIGNED = 202;

   public static final int NATIVE_ERROR_CANT_INITIALIZE_AIO = 203;

   public static final int NATIVE_ERROR_CANT_RELEASE_AIO = 204;

   public static final int NATIVE_ERROR_CANT_OPEN_CLOSE_FILE = 205;

   public static final int NATIVE_ERROR_CANT_ALLOCATE_QUEUE = 206;

   public static final int NATIVE_ERROR_PREALLOCATE_FILE = 208;

   public static final int NATIVE_ERROR_ALLOCATE_MEMORY = 209;

   public static final int NATIVE_ERROR_AIO_FULL = 211;
   
   

   private int code;

   public HornetQException()
   {
   }

   public HornetQException(int code)
   {
      this.code = code;
   }

   public HornetQException(int code, String msg)
   {
      super(msg);

      this.code = code;
   }

   public HornetQException(int code, String msg, Throwable cause)
   {
      super(msg, cause);

      this.code = code;
   }

   public int getCode()
   {
      return code;
   }

   public String toString()
   {
      return "HornetQException[errorCode=" + code + " message=" + getMessage() + "]";
   }

}
