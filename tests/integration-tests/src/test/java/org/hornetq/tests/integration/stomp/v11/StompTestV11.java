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
import org.hornetq.tests.integration.stomp.util.ClientStompFrame;
import org.hornetq.tests.integration.stomp.util.StompClientConnection;
import org.hornetq.tests.integration.stomp.util.StompClientConnectionFactory;


public class StompTestV11 extends StompTestBase2
{
   private static final transient Logger log = Logger.getLogger(StompTestV11.class);
   
   private StompClientConnection connV11;
   
   protected void setUp() throws Exception
   {
      super.setUp();
      connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
   }
   
   protected void tearDown() throws Exception
   {
      if (connV11.isConnected())
      {
         connV11.disconnect();
      }
      super.tearDown();
   }
   
   public void testConnection() throws Exception
   {
      StompClientConnection connection = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);
      
      connection.connect(defUser, defPass);
      
      assertTrue(connection.isConnected());
      
      assertEquals("1.0", connection.getVersion());
      
      connection.disconnect();

      connection = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);
      
      connection.connect(defUser, defPass);
      
      assertTrue(connection.isConnected());
      
      assertEquals("1.1", connection.getVersion());
      
      connection.disconnect();
   }
   
   /*
    * test case:
    * 1 accept-version absent. It is a 1.0 connect
    * 2 accept-version=1.0, result: 1.0
    * 3 accept-version=1.0,1.1,1.2, result 1.1
    * 4 accept-version="1.2,1.3", result error
    */
   public void testNegotiation() throws Exception
   {
      // case 1
      ClientStompFrame frame = connV11.createFrame("CONNECT");
      //frame.addHeader("accept-version", "1.0,1.1,1.2");
      frame.addHeader("host", "127.0.0.1");
      frame.addHeader("login", this.defUser);
      frame.addHeader("passcode", this.defPass);
      
      ClientStompFrame reply = connV11.sendFrame(frame);
      
      assertEquals("CONNECTED", reply.getCommand());
      
      //reply headers: version, session, server
      assertEquals(null, reply.getHeader("version"));
      
      connV11.disconnect();
      
   }
}
