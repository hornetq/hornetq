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
package org.jboss.messaging.tests.timing.core.remoting.impl;

import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.ResponseHandler;
import org.jboss.messaging.core.remoting.impl.ResponseHandlerImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import org.jboss.messaging.tests.util.UnitTestCase;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ResponseHandlerImplTest extends UnitTestCase
{
   protected static final long TIMEOUT = 500;

   public void testReceiveResponseTooLate() throws Exception
   {
      final ResponseHandler handler = new ResponseHandlerImpl(randomLong());
      final AtomicReference<Packet> receivedPacket = new AtomicReference<Packet>();

      Executors.newSingleThreadExecutor().execute(new Runnable() {

         public void run()
         {
            Packet response = handler.waitForResponse(TIMEOUT);
            receivedPacket.set(response);
         }
      });
      // pause for twice the timeout before handling the packet
      Thread.sleep(TIMEOUT * 2);
      handler.handle(new Ping(handler.getID()), null);

      assertNull(receivedPacket.get());
   }
}
