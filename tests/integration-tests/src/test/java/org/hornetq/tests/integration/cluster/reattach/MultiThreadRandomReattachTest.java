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

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.core.config.Configuration;

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
      Configuration liveConf = createDefaultConfig();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations().add(new TransportConfiguration(INVM_ACCEPTOR_FACTORY));
      liveServer = createServer(false, liveConf);
      liveServer.start();
      waitForServer(liveServer);
   }

   @Override
   protected void setBody(final ClientMessage message) throws Exception
   {
      // Give each msg a body
      message.getBodyBuffer().writeBytes(new byte[250]);
   }

   @Override
   protected boolean checkSize(final ClientMessage message)
   {
      return message.getBodyBuffer().readableBytes() == 250;
   }

}
