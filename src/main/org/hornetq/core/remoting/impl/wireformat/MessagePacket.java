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

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.Message;

/**
 * A MessagePacket
 *
 * @author Tim Fox
 *
 *
 */
public abstract class MessagePacket extends PacketImpl
{
   private static final Logger log = Logger.getLogger(MessagePacket.class);

   protected Message message;
      
   public MessagePacket(final byte type, final Message message)
   {
      super(type);
      
      this.message = message;
   }
   
   public Message getMessage()
   {
      return message;
   }
   
}
