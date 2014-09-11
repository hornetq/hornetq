/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.tests.unit.core.config.impl.fakes;

import org.hornetq.core.server.ConnectorService;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class FakeConnectorService implements ConnectorService
{
   @Override
   public String getName()
   {
      return null;
   }

   @Override
   public void start() throws Exception
   {
   }

   @Override
   public void stop() throws Exception
   {
   }

   @Override
   public boolean isStarted()
   {
      return false;
   }
}
