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
package org.hornetq.core.protocol.stomp;

import java.util.ArrayList;
import java.util.List;

public class HornetQStompException extends Exception {

   private static final long serialVersionUID = -274452327574950068L;
   
   private List<Header> headers = new ArrayList<Header>(10);
   private String body;
   
   public HornetQStompException(String msg)
   {
      super(msg);
   }
   
   public HornetQStompException(String msg, Throwable t)
   {
      super(msg, t);
   }
   
   public HornetQStompException(Throwable t)
   {
      super(t);
   }

   public void addHeader(String header, String value)
   {
      headers.add(new Header(header, value));
   }
   
   public void setBody(String body)
   {
      this.body = body;
   }

   public StompFrame getFrame()
   {
      return null;
   }

   private class Header
   {
      public String key;
      public String val;
      
      public Header(String key, String val)
      {
         this.key = key;
         this.val = val;
      }
   }
}
