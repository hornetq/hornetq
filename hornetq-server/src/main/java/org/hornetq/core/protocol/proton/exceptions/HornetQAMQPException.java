/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.core.protocol.proton.exceptions;

import org.hornetq.api.core.HornetQException;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         6/6/13
 */
public class HornetQAMQPException extends HornetQException
{

   private final static String ERROR_PREFIX = "amqp:";

   public String getAmqpError()
   {
      return amqpError.getError();
   }

   public enum AmqpError
   {
      INTERNAL_ERROR("internal-error"),
      NOT_FOUND("not-found"),
      UNAUTHORIZED_ACCESS("unuathorized-access"),
      DECODE_ERROR("decode-error"),
      RESOURCE_LIMIT_EXCEEDED("resource-limit-exceeded"),
      NOT_ALLOWED("not-allowed"),
      INVALID_FIELD("invalid-field"),
      NOT_IMPLEMENTED("not-implemented"),
      RESOURCE_LOCKED("resource-locked"),
      PRECONDITIONS_FAILED("preconditions-failed"),
      RESOURCE_DELETED("resource-deleted"),
      ILLEGAL_STATE("illegal-state"),
      FRAME_SIZE_TOO_SMALL("frame-size-too-small");

      private final String error;

      AmqpError(String error)
      {
         this.error = error;
      }

      public String getError()
      {
         return ERROR_PREFIX + error;
      }

   }

   private final AmqpError amqpError;

   public HornetQAMQPException(AmqpError amqpError, String message)
   {
      super(message);
      this.amqpError = amqpError;
   }
}
