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

package org.jboss.messaging.tests.integration.core.remoting.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class DummyInterceptorB implements Interceptor
{

   protected Logger log = Logger.getLogger(DummyInterceptorB.class);

   static AtomicInteger syncCounter = new AtomicInteger(0);
   
   public static int getCounter()
   {
      return syncCounter.get();
   }
   
   public static void clearCounter()
   {
      syncCounter.set(0);
   }
   
   public void intercept(final Packet packet) throws MessagingException
   {
      syncCounter.addAndGet(1);
      log.info("DummyFilter packet = " + packet);
   }

}
