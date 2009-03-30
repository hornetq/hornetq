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

package org.jboss.messaging.tests.integration.chunkmessage.mock;

import java.util.Map;

import org.jboss.messaging.core.remoting.impl.invm.InVMConnection;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnector;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

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
      super(configuration, handler, listener);
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
   protected Connection internalCreateConnection(final BufferHandler handler, final ConnectionLifeCycleListener listener)
   {
      return new MockConnection(id, handler, listener);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   public static interface MockCallback
   {
      void onWrite(final MessagingBuffer buffer);
   }

   class MockConnection extends InVMConnection
   {
      /**
       * @param handler
       * @param listener
       */
      public MockConnection(final int serverID, final BufferHandler handler, final ConnectionLifeCycleListener listener)
      {
         super(serverID, handler, listener);
      }

      @Override
      public void write(final MessagingBuffer buffer, final boolean flush)
      {
         log.info("calling mock connection write");
         if (callback != null)
         {
            callback.onWrite(buffer);
         }

         super.write(buffer, flush);
      }
   }
}
