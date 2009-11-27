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

package org.hornetq.core.client.impl;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.SessionFailureListener;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.RemotingConnection;

/**
 * A ConnectionManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 27 Nov 2008 18:45:46
 *
 *
 */
public interface FailoverManager
{
   ClientSession createSession(final String username,
                               final String password,
                               final boolean xa,
                               final boolean autoCommitSends,
                               final boolean autoCommitAcks,
                               final boolean preAcknowledge,
                               final int ackBatchSize,
                               final boolean cacheLargeMessageClient,
                               final int minLargeMessageSize,
                               final boolean blockOnAcknowledge,
                               final boolean autoGroup,
                               final int confirmationWindowSize,
                               final int producerWindowSize,
                               final int consumerWindowSize,
                               final int producerMaxRate,
                               final int consumerMaxRate,
                               final boolean blockOnNonPersistentSend,
                               final boolean blockOnPersistentSend,
                               final int initialMessagePacketSize) throws HornetQException;

   void removeSession(final ClientSessionInternal session);
   
   public RemotingConnection getConnection();
   
   int numConnections();

   int numSessions();
   
   void addFailureListener(SessionFailureListener listener);

   boolean removeFailureListener(SessionFailureListener listener);
   
   void causeExit();
}
