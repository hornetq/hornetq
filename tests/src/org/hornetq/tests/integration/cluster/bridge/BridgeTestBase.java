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

package org.hornetq.tests.integration.cluster.bridge;

import java.util.Map;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A BridgeTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Nov 2008 10:32:23
 *
 *
 */
public abstract class BridgeTestBase extends UnitTestCase
{
   protected HornetQServer createHornetQServer(final int id, final Map<String, Object> params)
   {
      return createHornetQServer(id, params, false);
   }

   protected HornetQServer createHornetQServer(final int id, final Map<String, Object> params, final boolean backup)
   {
      Configuration serviceConf = new ConfigurationImpl();
      serviceConf.setClustered(true);
      serviceConf.setSecurityEnabled(false);
      serviceConf.setBackup(backup);
      serviceConf.setSharedStore(true);
      serviceConf.setBindingsDirectory(getBindingsDir(id, false));
      serviceConf.setJournalMinFiles(2);
      serviceConf.setJournalDirectory(getJournalDir(id, false));
      serviceConf.setPagingDirectory(getPageDir(id, false));
      serviceConf.setLargeMessagesDirectory(getLargeMessagesDir(id, false));
      
      params.put(TransportConstants.SERVER_ID_PROP_NAME, id);
      serviceConf.getAcceptorConfigurations()
                 .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory", params));
      HornetQServer service = HornetQ.newHornetQServer(serviceConf, true);
      return service;
   }

}
