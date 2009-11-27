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


package org.hornetq.core.message;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.exception.HornetQException;

import java.nio.ByteBuffer;

/**
 * Class used to encode message body into buffers.
 * Used to send large streams over the wire
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *         Created Nov 2, 2009
 */
public interface BodyEncoder
{
   void open() throws HornetQException;

   void close() throws HornetQException;

   int encode(ByteBuffer bufferRead) throws HornetQException;

   int encode(HornetQBuffer bufferOut, int size) throws HornetQException;

   long getLargeBodySize();
}
