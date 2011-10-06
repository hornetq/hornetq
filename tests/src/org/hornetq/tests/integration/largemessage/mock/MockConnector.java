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

package org.hornetq.tests.integration.largemessage.mock;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.remoting.impl.invm.InVMConnection;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;

/**
 * A MockConnector
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Oct 22, 2008 11:23:18 AM
 *
 *
 */
public class MockConnector extends InVMConnector
{
   private final MockCallback callback;

   /**
    * @param configuration
    * @param handler
    * @param listener
    */
   public MockConnector(final Map<String, Object> configuration,
                        final BufferHandler handler,
                        final ConnectionLifeCycleListener listener)
   {
      super(configuration, handler, listener, Executors.newSingleThreadExecutor(), Executors.newCachedThreadPool());
      callback = (MockCallback)configuration.get("callback");
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected Connection internalCreateConnection(final BufferHandler handler,
                                                 final ConnectionLifeCycleListener listener,
                                                 final Executor serverExecutor)
   {
      return new MockConnection(id, handler, listener);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   public static interface MockCallback
   {
      void onWrite(final HornetQBuffer buffer);
   }

   class MockConnection extends InVMConnection
   {
      /**
       * @param handler
       * @param listener
       */
      public MockConnection(final int serverID, final BufferHandler handler, final ConnectionLifeCycleListener listener)
      {
         super(null, serverID, handler, listener, Executors.newSingleThreadExecutor());
      }

      @Override
      public void write(final HornetQBuffer buffer, final boolean flush, final boolean batch)
      {
         InVMConnector.log.info("calling mock connection write");
         if (callback != null)
         {
            callback.onWrite(buffer);
         }

         super.write(buffer, flush, batch);
      }
   }
}
