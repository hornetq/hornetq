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

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;

import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;

/**
 * HornetQ implementation of a JMS XAConnection.
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class HornetQXAConnection extends HornetQConnection implements XAConnection
{

   protected HornetQXAConnection(final String username, final String password,
                                 final ClientSessionFactory sessionFactory, HornetQConnectionFactory factory)
   {
      super(username, password, HornetQConnection.TYPE_GENERIC_CONNECTION, sessionFactory, factory);
   }

   protected HornetQXAConnection(final String username, final String password, final int type,
                                 final ClientSessionFactory sessionFactory, HornetQConnectionFactory factory)
   {
      super(username, password, type, sessionFactory, factory);
   }

   @Override
   public XASession createXASession() throws JMSException
   {
      checkClosed();
      return (XASession)createSessionInternal(true, Session.SESSION_TRANSACTED, HornetQSession.TYPE_GENERIC_SESSION);
   }

   @Override
   protected final boolean isXa()
   {
      return true;
   }

   @Override
   protected final HornetQSession createHQSession(boolean transacted, int acknowledgeMode, ClientSession session,
                                                  int type)
   {
      return new HornetQXASession(this, transacted, acknowledgeMode, session, type);
   }
}
