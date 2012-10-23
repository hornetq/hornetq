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

package org.hornetq.spi.core.remoting;

import org.hornetq.api.core.HornetQBuffer;

/**
 * A BufferDecoder
 *
 * @author tim
 *
 *
 */
public interface BufferDecoder
{
   /**
    * called by the remoting system prior to {@link org.hornetq.spi.core.remoting.BufferHandler#bufferReceived(Object, org.hornetq.api.core.HornetQBuffer)}.
    * <p/>
    * The implementation should return true if there is enough data in the buffer to decode. otherwise false.
    *
    * @param buffer the buffer
    * @return true id the buffer can be decoded..
    */
   int isReadyToHandle(HornetQBuffer buffer);
}
