/*
 * Copyright 2012 Red Hat, Inc.
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
package org.hornetq.core.remoting.impl.netty;

import org.junit.Test;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.spi.core.remoting.BufferDecoder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;
import org.jboss.netty.util.CharsetUtil;

import org.junit.Assert;

/**
 *
 * @author <a href="mailto:nmaurer@redhat.com">Norman Maurer</a>
 *
 */
public class HornetQFrameDecoderTest extends Assert{

   @Test
    public void testDecoding() {
        final ChannelBuffer buffer = ChannelBuffers.copiedBuffer("TestBytes", CharsetUtil.US_ASCII);

        DecoderEmbedder<ChannelBuffer> decoder = new DecoderEmbedder<ChannelBuffer>(new HornetQFrameDecoder(new BufferDecoder() {

            @Override
            public int isReadyToHandle(HornetQBuffer buffer) {
                if (buffer.readableBytes()  > 2) {
                    return 2;
                }
                return -1;
            }
        }));

        assertFalse("Should not readable", decoder.offer(buffer.duplicate().slice(0, 2)));
        assertTrue("Should be readable", decoder.offer(buffer.duplicate().slice(3, 2)));

        assertTrue("There must be something to poll", decoder.finish());
        ChannelBuffer buf = decoder.poll();
        assertEquals("Expected created ChannelBuffer which contains 2 bytes", 2, buf.readableBytes());
        assertEquals("Buffer content missmatch", buffer.slice(0,2), buf);
        assertNull("Not expected buffer", decoder.poll());
    }
}
