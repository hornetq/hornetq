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

import javax.transaction.xa.XAException;

/**
 * A HornetQXAException
 *
 * @author Tim Fox
 *
 *
 */
public class HornetQXAException extends XAException
{
   private static final long serialVersionUID = 6535914602965015803L;

   public HornetQXAException(final int errorCode, final String message)
   {
      super(message);

      this.errorCode = errorCode;
   }

   public HornetQXAException(final int errorCode)
   {
      super(errorCode);
   }
}
