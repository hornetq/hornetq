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
