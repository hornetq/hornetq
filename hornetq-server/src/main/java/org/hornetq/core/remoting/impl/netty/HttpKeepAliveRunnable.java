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
package org.hornetq.core.remoting.impl.netty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * A simple Runnable to allow {@link HttpAcceptorHandler}s to be called intermittently.
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

   public synchronized void registerKeepAliveHandler(final HttpAcceptorHandler httpAcceptorHandler)
   {
      handlers.add(httpAcceptorHandler);
   }

   public synchronized void unregisterKeepAliveHandler(final HttpAcceptorHandler httpAcceptorHandler)
   {
      handlers.remove(httpAcceptorHandler);
   }

   public void close()
   {
      if (future != null)
      {
         future.cancel(false);
      }

      closed = true;
   }

   public synchronized void setFuture(final Future<?> future)
   {
      this.future = future;
   }
}
