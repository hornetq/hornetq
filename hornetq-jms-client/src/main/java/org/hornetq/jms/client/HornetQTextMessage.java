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

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;

/**
 * HornetQ implementation of a JMS TextMessage.
 * <br>
 * This class was ported from SpyTextMessage in JBossMQ.
 *
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 * @version $Revision: 3412 $
 *
 */
public class HornetQTextMessage extends HornetQMessage implements TextMessage
{
   // Constants -----------------------------------------------------

   public static final byte TYPE = Message.TEXT_TYPE;

   // Attributes ----------------------------------------------------

   // We cache it locally - it's more performant to cache as a SimpleString, the AbstractChannelBuffer write
   // methods are more efficient for a SimpleString
   private SimpleString text;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public HornetQTextMessage(final ClientSession session)
   {
      super(HornetQTextMessage.TYPE, session);
   }

   public HornetQTextMessage(final ClientMessage message, final ClientSession session)
   {
      super(message, session);
   }

   /**
    * A copy constructor for non-HornetQ JMS TextMessages.
    */
   public HornetQTextMessage(final TextMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, HornetQTextMessage.TYPE, session);

      setText(foreign.getText());
   }

   // Public --------------------------------------------------------

   @Override
   public byte getType()
   {
      return HornetQTextMessage.TYPE;
   }

   // TextMessage implementation ------------------------------------

   public void setText(final String text) throws JMSException
   {
      checkWrite();

      HornetQBuffer buff = message.getBodyBuffer();

      buff.clear();

      if (text != null)
      {
         this.text = new SimpleString(text);
      }
      else
      {
         this.text = null;
      }

      buff.writeNullableSimpleString(this.text);
   }

   public String getText()
   {
      if (text != null)
      {
         return text.toString();
      }
      else
      {
         return null;
      }
   }

   @Override
   public void clearBody() throws JMSException
   {
      super.clearBody();

      text = null;
   }

   // HornetQRAMessage override -----------------------------------------

   @Override
   public void doBeforeSend() throws Exception
   {
      super.doBeforeSend();
   }

   @Override
   public void doBeforeReceive() throws HornetQException
   {
      super.doBeforeReceive();

      text = message.getBodyBuffer().readNullableSimpleString();
   }

   @Override
   protected <T> T getBodyInternal(Class<T> c)
   {
      return (T)getText();
   }

   @Override
   public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes")
   Class c) throws JMSException
   {
      if (text == null)
         return true;
      return c.isAssignableFrom(java.lang.String.class);
   }
}