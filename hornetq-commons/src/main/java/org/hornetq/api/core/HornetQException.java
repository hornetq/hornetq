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
 * HornetQException is the root exception for the HornetQ API.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class HornetQException extends Exception
{
   private static final long serialVersionUID = -4802014152804997417L;

   private final HornetQExceptionType type;

   public HornetQException()
   {
      type = HornetQExceptionType.GENERIC_EXCEPTION;
   }

   public HornetQException(final String msg)
   {
      super(msg);
      type = HornetQExceptionType.GENERIC_EXCEPTION;
   }

   /*
   * This constructor is needed only for the native layer
   */
   public HornetQException(int code, String msg)
   {
      super(msg);

      this.type = HornetQExceptionType.getType(code);
   }

   public HornetQException(HornetQExceptionType type, String msg)
   {
      super(msg);

      this.type = type;
   }

   public HornetQException(HornetQExceptionType type)
   {
      this.type = type;
   }

   public HornetQException(HornetQExceptionType type, String message, Throwable t)
   {
      super(message, t);
      this.type = type;
   }

   public HornetQExceptionType getType()
   {
      return type;
   }

   @Override
   public String toString()
   {
      return this.getClass().getSimpleName() + "[errorType=" + type + " message=" + getMessage() + "]";
   }

}
