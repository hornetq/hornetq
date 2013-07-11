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

package org.hornetq.tests.integration.jms.client;

import javax.jms.ConnectionFactory;
import javax.jms.Message;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;

/**
 * A AutoGroupingTest
 *
 * @author Tim Fox
 *
 *
 */
public class AutoGroupingTest extends GroupingTest
{

   @Override
   protected ConnectionFactory getCF() throws Exception
   {
      HornetQJMSConnectionFactory cf1 =
               (HornetQJMSConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                                              new TransportConfiguration(
                                                                                                                         INVM_CONNECTOR_FACTORY));

      cf1.setAutoGroup(true);

      return cf1;
   }

   @Override
   protected void setProperty(Message message)
   {
   }

}
