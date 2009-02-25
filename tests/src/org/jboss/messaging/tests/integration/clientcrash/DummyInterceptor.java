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

package org.jboss.messaging.tests.integration.clientcrash;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class DummyInterceptor implements Interceptor
{
   protected Logger log = Logger.getLogger(DummyInterceptor.class);

   boolean sendException = false;
   boolean changeMessage = false;
   AtomicInteger syncCounter = new AtomicInteger(0);
   
   public int getCounter()
   {
      return syncCounter.get();
   }
   
   public void clearCounter()
   {
      syncCounter.set(0);
   }
   
   public boolean intercept(final Packet packet, final RemotingConnection conn) throws MessagingException
   {
      log.debug("DummyFilter packet = " + packet.getClass().getName());
      syncCounter.addAndGet(1);
      if (sendException)
      {
         throw new MessagingException(MessagingException.INTERNAL_ERROR);
      }
      if (changeMessage)
      {
         if (packet instanceof SessionReceiveMessage)
         {
            SessionReceiveMessage deliver = (SessionReceiveMessage)packet;
            log.debug("msg = " + deliver.getServerMessage().getClass().getName());
            deliver.getServerMessage().putStringProperty(new SimpleString("DummyInterceptor"), new SimpleString("was here"));
         }
      }
      return true;
   }

}
