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

package org.hornetq.api.core;

/**
 * 
 * HornetQException is the root exception for HornetQ API. 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class HornetQException extends Exception
{
   private static final long serialVersionUID = -4802014152804997417L;

   // Error codes -------------------------------------------------

   /**
    * Internal error which prevented HornetQ to perform.
    */
   public static final int INTERNAL_ERROR = 000;

   /**
    * A packet of unsupported type was received by HornetQ PacketHandler.
    */
   public static final int UNSUPPORTED_PACKET = 001;

   /**
    * A client is not able to connect to HornetQ server.
    */
   public static final int NOT_CONNECTED = 002;

   /**
    * A client timed out will connecting to HornetQ server.
    */
   public static final int CONNECTION_TIMEDOUT = 003;

   /**
    * A client was disconnected from HornetQ server when the server has shut down.
    */
   public static final int DISCONNECTED = 004;

   /**
    * A blocking call from a client was unblocked during failover.
    */
   public static final int UNBLOCKED = 005;

   /**
    * Unexpected I/O error occured on the server.
    */
   public static final int IO_ERROR = 006;

   /**
    * An operation failed because a queue does not exist on the server.
    */
   public static final int QUEUE_DOES_NOT_EXIST = 100;

   /**
    * An operation failed because a queue exists on the server.
    */
   public static final int QUEUE_EXISTS = 101;

   /**
    * A client operation failed because the calling resource
    * (ClientSession, ClientProducer, etc.) is closed.
    */
   public static final int OBJECT_CLOSED = 102;

   /**
    * An filter expression has not been validated
    */
   public static final int INVALID_FILTER_EXPRESSION = 103;

   /**
    * A HornetQ resource is not in a legal state (e.g. calling 
    * ClientConsumer.receive() if a MessageHandler is set)
    */
   public static final int ILLEGAL_STATE = 104;

   /**
    * A security problem occured (authentication issues, permission issues,...)
    */
   public static final int SECURITY_EXCEPTION = 105;

   /**
    * An operation failed because an address does not exist on the server.
    */
   public static final int ADDRESS_DOES_NOT_EXIST = 106;

   /**
    * An operation failed because an address exists on the server.
    */
   public static final int ADDRESS_EXISTS = 107;

   /**
    * A incompatibility between HornetQ versions on the client and the server has been detected
    */
   public static final int INCOMPATIBLE_CLIENT_SERVER_VERSIONS = 108;

   /**
    * An operation failed because a session exists on the server.
    */
   public static final int SESSION_EXISTS = 109;

   /**
    * An problem occurred while manipulating the body of a large message.
    */
   public static final int LARGE_MESSAGE_ERROR_BODY = 110;

   /**
    * A transaction was rolled back.
    */
   public static final int TRANSACTION_ROLLED_BACK = 111;

   /**
    * The creation of a session was rejected by the server (e.g. if the
    * server is starting and has not finish to be initialized)
    */
   public static final int SESSION_CREATION_REJECTED = 112;

   
   // Native Error codes ----------------------------------------------

   /**
    * A internal error occured in the AIO native code
    */
   public static final int NATIVE_ERROR_INTERNAL = 200;

   /**
    * A buffer is invalid in the AIO native code
    */
   public static final int NATIVE_ERROR_INVALID_BUFFER = 201;

   /**
    * Alignment error in the AIO native code
    */
   public static final int NATIVE_ERROR_NOT_ALIGNED = 202;

   /**
    * AIO has not been properly initialized
    */
   public static final int NATIVE_ERROR_CANT_INITIALIZE_AIO = 203;

   /**
    * AIO has not been properly released
    */
   public static final int NATIVE_ERROR_CANT_RELEASE_AIO = 204;

   /**
    * A closed file has not be properly reopened
    */
   public static final int NATIVE_ERROR_CANT_OPEN_CLOSE_FILE = 205;

   /**
    * An error occured while allocating a queue in AIO native code
    */
   public static final int NATIVE_ERROR_CANT_ALLOCATE_QUEUE = 206;

   /**
    * An error occured while pre-allocating a file in AIO native code
    */
   public static final int NATIVE_ERROR_PREALLOCATE_FILE = 208;

   /**
    * An error occurred while allocating memory in the AIO native code
    */
   public static final int NATIVE_ERROR_ALLOCATE_MEMORY = 209;

   /**
    * AIO is full
    */
   public static final int NATIVE_ERROR_AIO_FULL = 211;

   private int code;

   public HornetQException()
   {
   }

   public HornetQException(final int code)
   {
      this.code = code;
   }

   public HornetQException(final int code, final String msg)
   {
      super(msg);

      this.code = code;
   }

   public HornetQException(final int code, final String msg, final Throwable cause)
   {
      super(msg, cause);

      this.code = code;
   }

   public int getCode()
   {
      return code;
   }

   @Override
   public String toString()
   {
      return "HornetQException[errorCode=" + code + " message=" + getMessage() + "]";
   }

}
