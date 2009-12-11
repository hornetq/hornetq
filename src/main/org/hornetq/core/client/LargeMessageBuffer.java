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

package org.hornetq.core.client;

import java.io.OutputStream;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.exception.HornetQException;

/**
 * A LargeMessageBuffer represents the buffer of a large message.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public interface LargeMessageBuffer extends HornetQBuffer
{
   /**
    * Returns the size of this buffer.

    */
   long getSize();

   /**
    * Discards packets unused by this buffer.
    */
   void discardUnusedPackets();

   /**
    * Closes this buffer.
    */
   void close();

   /**
    * Cancels this buffer.
    */
   void cancel();

   /**
    * Sets the OutputStream of this buffer to the specified output.
    */
   void setOutputStream(final OutputStream output) throws HornetQException;

   /**
    * Saves this buffer to the specified output.
    */
   void saveBuffer(final OutputStream output) throws HornetQException;

   /**
    * Waits for the completion for the specified waiting time (in milliseconds).
    */
   boolean waitCompletion(long timeWait) throws HornetQException;

}
