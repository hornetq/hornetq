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
package org.hornetq.core.protocol.core.impl;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.logging.Logger;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.utils.DataConstants;

/**
 * A AbstractBufferHandler
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public abstract class AbstractBufferHandler implements BufferHandler
{
   private static final Logger log = Logger.getLogger(AbstractBufferHandler.class);

   public int isReadyToHandle(final HornetQBuffer buffer)
   {
      if (buffer.readableBytes() < DataConstants.SIZE_INT)
      {
         return -1;
      }

      int length = buffer.readInt();

      if (buffer.readableBytes() < length)
      {
         return -1;
      }

      return length;
   }
}
