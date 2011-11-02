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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * 
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 * pls use factory to create frames.
 */
public interface ClientStompFrame
{

   public ByteBuffer toByteBuffer() throws UnsupportedEncodingException;

   public boolean needsReply();

   public void setCommand(String command);

   public void addHeader(String string, String string2);

   public void setBody(String string);

   public String getCommand();

   public String getHeader(String header);

   public String getBody();

   public ByteBuffer toByteBufferWithExtra(String str)  throws UnsupportedEncodingException;
   
   public boolean isPing();
   
}
