/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

package org.jboss.messaging.core.remoting.impl.mina;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.*;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.MessagingCodec;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.MessagingCodecImpl;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * A Mina ProtocolEncoder used to encode/decode messages.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MinaProtocolCodecFilter extends CumulativeProtocolDecoder
        implements ProtocolEncoder, ProtocolCodecFactory
{
   private static final Logger log = Logger.getLogger(MessagingCodecImpl.class);

   // ProtocolCodecFactory implementation
   // -----------------------------------------------------------------------------------
   private MessagingCodec messagingCodec = new MessagingCodecImpl();

   public ProtocolDecoder getDecoder(final IoSession session)
   {
      return this;
   }

   public ProtocolEncoder getEncoder(final IoSession session)
   {
      return this;
   }

   // ProtocolEncoder implementation ------------------------------------------

   public void dispose(final IoSession session) throws Exception
   {
   }

   public void encode(final IoSession session, final Object message,
                      final ProtocolEncoderOutput out) throws Exception
   {

      IoBuffer iobuf = IoBuffer.allocate(1024, false);

      iobuf.setAutoExpand(true);

      MessagingBuffer buffer = new IoBufferWrapper(iobuf);

      messagingCodec.encode(buffer, message);

      out.write(iobuf);
   }

   // CumulativeProtocolDecoder overrides
   // -------------------------------------------------------------------------------------

   public boolean doDecode(final IoSession session, final IoBuffer in, final ProtocolDecoderOutput out) throws Exception
   {
      MessagingBuffer buff = new IoBufferWrapper(in);

      Packet packet = messagingCodec.decode(buff);
      if(packet != null)
      {
         out.write(packet);
      }
      return packet != null;
   }
}

