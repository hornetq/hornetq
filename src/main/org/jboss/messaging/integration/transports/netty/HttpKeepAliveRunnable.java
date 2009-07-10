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
package org.jboss.messaging.integration.transports.netty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * A simple Runnable to allow HttpAcceptorHandlers to be called intermittently.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class HttpKeepAliveRunnable implements Runnable
{
   private final List<HttpAcceptorHandler> handlers = new ArrayList<HttpAcceptorHandler>();
   private boolean closed = false;
   private Future<?> future;

   public synchronized void run()
   {
      if (closed)
      {
         return;
      }
      
      long time = System.currentTimeMillis();
      for (HttpAcceptorHandler handler : handlers)
      {
         handler.keepAlive(time);
      }
   }

   public synchronized void registerKeepAliveHandler(HttpAcceptorHandler httpAcceptorHandler)
   {
      handlers.add(httpAcceptorHandler);
   }

   public synchronized void unregisterKeepAliveHandler(HttpAcceptorHandler httpAcceptorHandler)
   {
      handlers.remove(httpAcceptorHandler);
   }

   public void close()
   {
      if (future != null)
      {             
         future.cancel(false);
      }

      closed  = true;
   }

   public synchronized void setFuture(Future<?> future)
   {
      this.future = future;
   }
}
