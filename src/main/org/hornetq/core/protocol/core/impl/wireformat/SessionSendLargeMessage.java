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

package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionSendLargeMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** Used only if largeMessage */
   private MessageInternal largeMessage;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   // To be used by the PacketDecoder
   public SessionSendLargeMessage()
   {
      this(new ServerMessageImpl());
   }

   public SessionSendLargeMessage(final MessageInternal largeMessage)
   {
      super(PacketImpl.SESS_SEND_LARGE);

      this.largeMessage = largeMessage;
   }

   // Public --------------------------------------------------------

   public MessageInternal getLargeMessage()
   {
      return largeMessage;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      largeMessage.encodeHeadersAndProperties(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      largeMessage.decodeHeadersAndProperties(buffer);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
