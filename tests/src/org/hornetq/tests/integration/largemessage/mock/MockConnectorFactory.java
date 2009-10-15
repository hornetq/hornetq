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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.core.remoting.spi.BufferHandler;
import org.hornetq.core.remoting.spi.ConnectionLifeCycleListener;
import org.hornetq.core.remoting.spi.Connector;
import org.hornetq.core.remoting.spi.ConnectorFactory;

/**
 * A MockConnectorFactory
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Oct 22, 2008 12:04:11 PM
 *
 *
 */
public class MockConnectorFactory implements ConnectorFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.remoting.spi.ConnectorFactory#createConnector(java.util.Map, org.hornetq.core.remoting.spi.BufferHandler, org.hornetq.core.remoting.spi.ConnectionLifeCycleListener)
    */
   public Connector createConnector(final Map<String, Object> configuration,
                                    final BufferHandler handler,
                                    final ConnectionLifeCycleListener listener,
                                    final Executor executor, ScheduledExecutorService scheduledThreadPool)
   {
      return new MockConnector(configuration, handler, listener);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.remoting.spi.ConnectorFactory#getAllowableProperties()
    */
   public Set<String> getAllowableProperties()
   {
      return Collections.EMPTY_SET;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
