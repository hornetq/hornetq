/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.jms.client;

import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicSession;

import org.hornetq.api.core.client.ClientSession;

/**
 * A HornetQXASession
 *
 * @author clebertsuconic
 *
 *
 */
public class HornetQXASession extends HornetQSession implements XAQueueSession, XATopicSession, XASession
{

   /**
    * @param connection
    * @param transacted
    * @param xa
    * @param ackMode
    * @param session
    * @param sessionType
    */
   protected HornetQXASession(HornetQConnection connection,
                              boolean transacted,
                              boolean xa,
                              int ackMode,
                              ClientSession session,
                              int sessionType)
   {
      super(connection, transacted, xa, ackMode, session, sessionType);
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
