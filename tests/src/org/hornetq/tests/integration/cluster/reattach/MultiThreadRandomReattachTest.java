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

package org.hornetq.tests.integration.cluster.reattach;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.HornetQ;


/**
 * 
 * A MultiThreadRandomReattachTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class MultiThreadRandomReattachTest extends MultiThreadRandomReattachTestBase
{
   @Override
   protected void start() throws Exception
   {      
      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));      
      liveServer = HornetQ.newHornetQServer(liveConf, false);
      liveServer.start();
   }

   /* (non-Javadoc)
    * @see org.hornetq.tests.integration.cluster.failover.MultiThreadRandomReattachTestBase#setBody(org.hornetq.core.client.ClientMessage)
    */
   @Override
   protected void setBody(final ClientMessage message) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.tests.integration.cluster.failover.MultiThreadRandomReattachTestBase#checkSize(org.hornetq.core.client.ClientMessage)
    */
   @Override
   protected boolean checkSize(final ClientMessage message)
   {
      return 0 == message.getBody().writerIndex();
   }

}
