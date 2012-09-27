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

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.SessionFailureListener;

/**
 * A ClientSessionFactoryInternal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ClientSessionFactoryInternal extends ClientSessionFactory
{
   void causeExit();
   
   void addFailureListener(SessionFailureListener listener);

   boolean removeFailureListener(SessionFailureListener listener);
   
   void disableFinalizeCheck();

   // for testing

   int numConnections();

   int numSessions();
   
   void removeSession(final ClientSessionInternal session, boolean failingOver);

   void connect(int reconnectAttempts, boolean failoverOnInitialConnection) throws HornetQException;
   
   void sendNodeAnnounce(final long currentEventID, String nodeID, String nodeName, boolean isBackup, TransportConfiguration config, TransportConfiguration backupConfig);

   TransportConfiguration getConnectorConfiguration();

   void setBackupConnector(TransportConfiguration live, TransportConfiguration backUp);

   Object getConnector();

   Object getBackupConnector();

   void setReconnectAttempts(int i);
}
