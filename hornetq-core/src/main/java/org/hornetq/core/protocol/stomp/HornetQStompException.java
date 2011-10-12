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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class HornetQStompException extends Exception {

   private static final long serialVersionUID = -274452327574950068L;
   
   private List<Header> headers = new ArrayList<Header>(10);
   private String body;
   private VersionedStompFrameHandler handler;
   private boolean disconnect;
   
   public HornetQStompException(StompConnection connection, String msg)
   {
      super(msg);
      handler = connection.getFrameHandler();
   }
   public HornetQStompException(String msg)
   {
      super(msg);
   }
   
   public HornetQStompException(String msg, Throwable t)
   {
      super(msg, t);
      this.body = t.getMessage();
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
      StompFrame frame = null;
      if (handler == null)
      {
         frame = new StompFrame("ERROR");
         frame.addHeader("message", this.getMessage());
         if (body != null)
         {
            try
            {
               frame.setByteBody(body.getBytes("UTF-8"));
            }
            catch (UnsupportedEncodingException e)
            {
            }
         }
         else
         {
            frame.setByteBody(new byte[0]);
         }
      }
      else
      {
         frame = handler.createStompFrame("ERROR");
         frame.addHeader("message", this.getMessage());
      }
      frame.setNeedsDisconnect(disconnect);
      return frame;
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

   public void setDisconnect(boolean b)
   {
      disconnect = b;
   }
}
