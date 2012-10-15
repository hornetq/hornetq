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
package org.hornetq.spi.core.remoting;

import org.hornetq.api.core.HornetQException;
import org.hornetq.spi.core.protocol.ProtocolType;

/**
 * A ConnectionLifeCycleListener is called by the remoting implementation to notify of connection events.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ConnectionLifeCycleListener
{
   /**
    * This method is used both by client connector creation and server connection creation through acceptors.
    * the acceptor will be set to null on client operations
    *
    * @param The acceptor here will be always null on a client connection created event.
    * @param connection the connection that has been created
    */
   void connectionCreated(Acceptor acceptor, Connection connection, ProtocolType protocol);

   /**
    * called when a connection is destroyed.
    *
    * @param connectionID the connection being destroyed.
    */
   void connectionDestroyed(Object connectionID);


   /**
    * called when an error occurs on the connection.
    *
    * @param connectionID the id of the connection.
    * @param me           the exception.
    */
   void connectionException(Object connectionID, HornetQException me);

   void connectionReadyForWrites(Object connectionID, boolean ready);
}
