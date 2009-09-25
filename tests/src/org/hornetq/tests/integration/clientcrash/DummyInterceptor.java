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

package org.hornetq.tests.integration.clientcrash;

import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class DummyInterceptor implements Interceptor
{
   protected Logger log = Logger.getLogger(DummyInterceptor.class);

   boolean sendException = false;
   boolean changeMessage = false;
   AtomicInteger syncCounter = new AtomicInteger(0);
   
   public int getCounter()
   {
      return syncCounter.get();
   }
   
   public void clearCounter()
   {
      syncCounter.set(0);
   }
   
   public boolean intercept(final Packet packet, final RemotingConnection conn) throws HornetQException
   {
      log.debug("DummyFilter packet = " + packet.getClass().getName());
      syncCounter.addAndGet(1);
      if (sendException)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR);
      }
      if (changeMessage)
      {
         if (packet instanceof SessionReceiveMessage)
         {
            SessionReceiveMessage deliver = (SessionReceiveMessage)packet;
            log.debug("msg = " + deliver.getServerMessage().getClass().getName());
            deliver.getServerMessage().putStringProperty(new SimpleString("DummyInterceptor"), new SimpleString("was here"));
         }
      }
      return true;
   }   

}
