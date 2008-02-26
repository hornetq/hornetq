/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.messaging.core.server;

/**
 * 
 * A MessagingException
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MessagingException extends Exception
{
   private static final long serialVersionUID = -4802014152804997417L;
   
   // Error codes -------------------------------------------------
            
   public static final int INTERNAL_ERROR = 000;
   
   public static final int UNSUPPORTED_PACKET = 001;
   
   public static final int NOT_CONNECTED = 002;
   
   public static final int CONNECTION_TIMEDOUT = 003;

   
   public static final int QUEUE_DOES_NOT_EXIST = 100;
   
   public static final int QUEUE_EXISTS = 101;
   
   public static final int OBJECT_CLOSED = 102;
   
   public static final int INVALID_FILTER_EXPRESSION = 103;
   
   public static final int ILLEGAL_STATE = 104;
   
   public static final int SECURITY_EXCEPTION = 105;
   
   public static final int ADDRESS_DOES_NOT_EXIST = 106;
   
   public static final int ADDRESS_EXISTS = 107;
   
   private int code;
   
   public MessagingException()
   {      
   }
   
   public MessagingException(int code)
   {
      this.code = code;
   }
   
   public MessagingException(int code, String msg)
   {
      super(msg);
      
      this.code = code;
   }
   
   public int getCode()
   {
      return code;
   }
   
   public String toString()
   {
      return "MessagingException[errorCode=" + code + " message=" + getMessage() + "]";
   }

}
