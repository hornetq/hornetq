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

package org.jboss.messaging.integration.transports.mina;

import static org.jboss.messaging.utils.DataConstants.SIZE_INT;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.BufferHandler;

/**
 * A Mina ProtocolEncoder used to encode/decode messages.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MinaProtocolCodecFilter extends CumulativeProtocolDecoder
        implements ProtocolEncoder, ProtocolCodecFactory
{
   private static final Logger log = Logger.getLogger(MinaProtocolCodecFilter.class);

   private final BufferHandler handler;

   public MinaProtocolCodecFilter(final BufferHandler handler)
   {
      this.handler = handler;
   }

   // ProtocolCodecFactory implementation
   // -----------------------------------------------------------------------------------

   public ProtocolDecoder getDecoder(final IoSession session)
   {
      return this;
   }

   public ProtocolEncoder getEncoder(final IoSession session)
   {
      return this;
   }

   // ProtocolEncoder implementation ------------------------------------------

   @Override
   public void dispose(final IoSession session) throws Exception
   {
   }

   public void encode(final IoSession session, final Object message,
                      final ProtocolEncoderOutput out) throws Exception
   {
      out.write(message);
   }

   // CumulativeProtocolDecoder overrides
   // -------------------------------------------------------------------------------------

   @Override
   public boolean doDecode(final IoSession session, final IoBuffer in, final ProtocolDecoderOutput out) throws Exception
   {
      //TODO - we can avoid this entirely if we maintain fragmented packets in the handler

      int start = in.position();

      int length = handler.isReadyToHandle(new IoBufferWrapper(in));

      if (length == -1)
      {
         in.position(start);
         
         return false;
      }

      IoBuffer sliced = in.slice();
      
      in.position(start + length + SIZE_INT);

      out.write(sliced);

      return true;
   }
}




