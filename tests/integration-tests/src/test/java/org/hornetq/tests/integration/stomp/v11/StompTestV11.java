/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hornetq.tests.integration.stomp.v11;

import org.hornetq.core.logging.Logger;
import org.hornetq.tests.integration.stomp.util.StompClientConnection;

public class StompTestV11 extends StompTestBase2
{
   private static final transient Logger log = Logger.getLogger(StompTestV11.class);
   
   public void testConnection() throws Exception
   {
      StompClientConnection connection = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
   }
}
