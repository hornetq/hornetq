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

package org.jboss.messaging.tests.unit.core.remoting.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.ResponseHandler;
import org.jboss.messaging.core.remoting.impl.ResponseHandlerImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ResponseHandlerImplTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected static final long TIMEOUT = 500;

   public void testReceiveResponseInTime() throws Exception
   {
      long id = randomLong();
      final ResponseHandler handler = new ResponseHandlerImpl(id);

      final AtomicReference<Packet> receivedPacket = new AtomicReference<Packet>();
      final CountDownLatch latch = new CountDownLatch(1);

      Thread t = new Thread() {
         public void run()
         {
            Packet response = handler.waitForResponse(TIMEOUT);
            receivedPacket.set(response);
            latch.countDown();
         }         
      };
      
      t.start();

      Packet ping = new PacketImpl(PacketImpl.PING);
      handler.handle(1243, ping);

      boolean gotPacketBeforeTimeout = latch.await(TIMEOUT, MILLISECONDS);
      assertTrue(gotPacketBeforeTimeout);
      assertNotNull(receivedPacket.get());
      t.join();
   }

   public void testSetFailed() throws Exception
   {
      ResponseHandler handler = new ResponseHandlerImpl(randomLong());
      handler.setFailed();
      try
      {
         handler.waitForResponse(TIMEOUT);
         fail("should throw a IllegalStateException");
      }
      catch (IllegalStateException e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
