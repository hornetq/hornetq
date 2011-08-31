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
package org.hornetq.stomp.tests.util.client;

import java.io.IOException;

/**
 * 
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public class StompClientConnectionV11 extends AbstractStompClientConnection
{
   public static final String STOMP_COMMAND = "STOMP";
   
   public static final String ACCEPT_HEADER = "accept-version";
   public static final String HOST_HEADER = "host";
   public static final String VERSION_HEADER = "version";
   public static final String RECEIPT_HEADER = "receipt";

   public StompClientConnectionV11(String host, int port) throws IOException
   {
      super("1.1", host, port);
   }

   public void connect(String username, String passcode) throws IOException, InterruptedException
   {
      ClientStompFrame frame = factory.newFrame(CONNECT_COMMAND);
      frame.addHeader(ACCEPT_HEADER, "1.0,1.1");
      frame.addHeader(HOST_HEADER, "localhost");
      if (username != null)
      {
         frame.addHeader(LOGIN_HEADER, username);
         frame.addHeader(PASSCODE_HEADER, passcode);
      }

      ClientStompFrame response = this.sendFrame(frame);
      
      if (response.getCommand().equals(CONNECTED_COMMAND))
      {
         String version = response.getHeader(VERSION_HEADER);
         assert(version.equals("1.1"));
         
         this.username = username;
         this.passcode = passcode;
         this.connected = true;
      }
   }

   public void connect1(String username, String passcode) throws IOException, InterruptedException
   {
      ClientStompFrame frame = factory.newFrame(STOMP_COMMAND);
      frame.addHeader(ACCEPT_HEADER, "1.0,1.1");
      frame.addHeader(HOST_HEADER, "localhost");
      if (username != null)
      {
         frame.addHeader(LOGIN_HEADER, username);
         frame.addHeader(PASSCODE_HEADER, passcode);
      }

      ClientStompFrame response = this.sendFrame(frame);
      
      if (response.getCommand().equals(CONNECTED_COMMAND))
      {
         String version = response.getHeader(VERSION_HEADER);
         assert(version.equals("1.1"));
         
         this.username = username;
         this.passcode = passcode;
         this.connected = true;
      }
   }

   @Override
   public void disconnect() throws IOException, InterruptedException
   {
      ClientStompFrame frame = factory.newFrame(DISCONNECT_COMMAND);
      frame.addHeader(RECEIPT_HEADER, "77");
      
      this.sendFrame(frame);
      
      close();
   }

}
