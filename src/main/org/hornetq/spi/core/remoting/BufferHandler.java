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
package org.hornetq.spi.core.remoting;

import org.hornetq.api.core.HornetQBuffer;

/**
 * A BufferHandler that will handle buffers received by an acceptor.
 * <p/>
 * The Buffer Handler will decode the buffer and take the appropriate action, typically forwarding to the correct channel.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface BufferHandler
{
   /**
    * called by the remoting connection when a buffer is received.
    *
    * @param connectionID the connection the buffer was received on
    * @param buffer       the buffer to decode
    */
   void bufferReceived(Object connectionID, HornetQBuffer buffer);
}
