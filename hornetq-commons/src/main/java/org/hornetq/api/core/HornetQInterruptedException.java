/*
 * Copyright 2010 Red Hat, Inc.
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
 * When an interruption happens, we will just throw a non-checked exception.
 * @author clebertsuconic
 */
public final class HornetQInterruptedException extends RuntimeException
{
   private static final long serialVersionUID = -5744690023549671221L;

   public HornetQInterruptedException(Throwable cause)
   {
      super(cause);
   }
}
