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
package org.hornetq.tests.integration.stomp.util;

import java.util.StringTokenizer;

/**
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 * 1.0 frames
 *
 * 1. CONNECT
 * 2. CONNECTED
 * 3. SEND
 * 4. SUBSCRIBE
 * 5. UNSUBSCRIBE
 * 6. BEGIN
 * 7. COMMIT
 * 8. ACK
 * 9. ABORT
 * 10. DISCONNECT
 * 11. MESSAGE
 * 12. RECEIPT
 * 13. ERROR
 */
public class StompFrameFactoryV10 implements StompFrameFactory
{

   public ClientStompFrame createFrame(String data)
   {
      //split the string at "\n\n"
      String[] dataFields = data.split("\n\n");

      StringTokenizer tokenizer = new StringTokenizer(dataFields[0], "\n");

      String command = tokenizer.nextToken();
      ClientStompFrame frame = new ClientStompFrameV10(command);

      while (tokenizer.hasMoreTokens())
      {
         String header = tokenizer.nextToken();
         String[] fields = header.split(":");
         frame.addHeader(fields[0], fields[1]);
      }

      //body (without null byte)
      if (dataFields.length == 2)
      {
         frame.setBody(dataFields[1]);
      }
      return frame;
   }

   @Override
   public ClientStompFrame newFrame(String command)
   {
      return new ClientStompFrameV10(command);
   }

}
