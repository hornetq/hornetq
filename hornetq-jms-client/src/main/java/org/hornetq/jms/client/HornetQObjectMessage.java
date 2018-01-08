/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.jms.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.utils.ObjectInputStreamWithClassLoader;

/**
 * HornetQ implementation of a JMS ObjectMessage.
 * <br>
 * Don't used ObjectMessage if you want good performance!
 *
 * Serialization is slooooow!
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 * @version $Revision: 3412 $
 *
 * $Id: HornetQRAObjectMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class HornetQObjectMessage extends HornetQMessage implements ObjectMessage
{
   // Constants -----------------------------------------------------

   public static final byte TYPE = Message.OBJECT_TYPE;

   // Attributes ----------------------------------------------------

   // keep a snapshot of the Serializable Object as a byte[] to provide Object isolation
   private byte[] data;

   private final ConnectionFactoryOptions options;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   protected HornetQObjectMessage(final ClientSession session, ConnectionFactoryOptions options)
   {
      super(HornetQObjectMessage.TYPE, session);
      this.options = options;
   }

   protected HornetQObjectMessage(final ClientMessage message, final ClientSession session, ConnectionFactoryOptions options)
   {
      super(message, session);
      this.options = options;
   }

   /**
    * A copy constructor for foreign JMS ObjectMessages.
    */
   public HornetQObjectMessage(final ObjectMessage foreign, final ClientSession session, ConnectionFactoryOptions options) throws JMSException
   {
      super(foreign, HornetQObjectMessage.TYPE, session);

      setObject(foreign.getObject());
      this.options = options;
   }

   // Public --------------------------------------------------------

   @Override
   public byte getType()
   {
      return HornetQObjectMessage.TYPE;
   }

   @Override
   public void doBeforeSend() throws Exception
   {
      message.getBodyBuffer().clear();
      if (data != null)
      {
         message.getBodyBuffer().writeInt(data.length);
         message.getBodyBuffer().writeBytes(data);
      }

      super.doBeforeSend();
   }

   @Override
   public void doBeforeReceive() throws HornetQException
   {
      super.doBeforeReceive();
      try
      {
         int len = message.getBodyBuffer().readInt();
         data = new byte[len];
         message.getBodyBuffer().readBytes(data);
      }
      catch (Exception e)
      {
         data = null;
      }

   }

   // ObjectMessage implementation ----------------------------------

   public void setObject(final Serializable object) throws JMSException
   {
      checkWrite();

      if (object != null)
      {
         try
         {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

            ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeObject(object);

            oos.flush();

            data = baos.toByteArray();
         }
         catch (Exception e)
         {
            JMSException je = new JMSException("Failed to serialize object");
            je.setLinkedException(e);
            throw je;
         }
      }
   }

   // lazy deserialize the Object the first time the client requests it
   public Serializable getObject() throws JMSException
   {
      if (data == null || data.length == 0)
      {
         return null;
      }

      try
      {
         ByteArrayInputStream bais = new ByteArrayInputStream(data);
         ObjectInputStreamWithClassLoader ois = new org.hornetq.utils.ObjectInputStreamWithClassLoader(bais);
         String blackList = getDeserializationBlackList();
         if (blackList != null)
         {
            ois.setBlackList(blackList);
         }
         String whiteList = getDeserializationWhiteList();
         if (whiteList != null)
         {
            ois.setWhiteList(whiteList);
         }
         Serializable object = (Serializable)ois.readObject();
         return object;
      }
      catch (Exception e)
      {
         JMSException je = new JMSException(e.getMessage());
         je.setStackTrace(e.getStackTrace());
         throw je;
      }
   }

   @Override
   public void clearBody() throws JMSException
   {
      super.clearBody();

      data = null;
   }


   private String getDeserializationBlackList()
   {
      if (options == null)
      {
         return null;
      }
      else
      {
         return options.getDeserializationBlackList();
      }
   }

   private String getDeserializationWhiteList()
   {
      if (options == null)
      {
         return null;
      }
      else
      {
         return options.getDeserializationWhiteList();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------


}
