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
package org.hornetq.rest.queue;

import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.jms.client.ConnectionFactoryOptions;
import org.hornetq.utils.ObjectInputStreamWithClassLoader;

import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class ConsumedObjectMessage extends ConsumedMessage
{
   protected Object readObject;
   private ConnectionFactoryOptions options;

   public ConsumedObjectMessage(ClientMessage message,  ConnectionFactoryOptions options)
   {
      super(message);
      if (message.getType() != ClientMessage.OBJECT_TYPE) throw new IllegalArgumentException("Client message must be an OBJECT_TYPE");
      this.options = options;
   }

   @Override
   public void build(Response.ResponseBuilder builder)
   {
      buildHeaders(builder);
      if (readObject == null)
      {
         int size = message.getBodyBuffer().readInt();
         if (size > 0)
         {
            byte[] body = new byte[size];
            message.getBodyBuffer().readBytes(body);
            ByteArrayInputStream bais = new ByteArrayInputStream(body);
            try
            {
               ObjectInputStreamWithClassLoader ois = new ObjectInputStreamWithClassLoader(bais);
               if (options != null)
               {
                  ois.setWhiteList(options.getDeserializationWhiteList());
                  ois.setBlackList(options.getDeserializationBlackList());
               }
               readObject = ois.readObject();
            }
            catch (Exception e)
            {
               throw new RuntimeException(e);
            }
         }

      }
      builder.entity(readObject);
   }
}
