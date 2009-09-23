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

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.logging.Logger;

/**
 * This class implements javax.jms.TextMessage ported from SpyTextMessage in JBossMQ.
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
 * $Id: HornetQRATextMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class HornetQTextMessage extends HornetQMessage implements TextMessage
{
   // Constants -----------------------------------------------------

   public static final byte TYPE = 3;
   
   public static final Logger log = Logger.getLogger(HornetQTextMessage.class);

   // Attributes ----------------------------------------------------
   
   //We cache it locally
   private String text;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   /*
    * This constructor is used to construct messages prior to sending
    */
   public HornetQTextMessage()
   {
      super(HornetQTextMessage.TYPE);
   }
   /**
    * constructors for test purposes only
    */
   public HornetQTextMessage(final ClientSession session)
   {
      super(HornetQTextMessage.TYPE, session);
   }
   
   public HornetQTextMessage(final ClientMessage message, ClientSession session)
   {     
      super(message, session);
   }
   
   /**
    * A copy constructor for non-HornetQ JMS TextMessages.
    */
   public HornetQTextMessage(final TextMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, HornetQTextMessage.TYPE, session);
      
      text = foreign.getText();
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return HornetQTextMessage.TYPE;
   }
       
   // TextMessage implementation ------------------------------------

   public void setText(final String text) throws JMSException
   {
      checkWrite();
      
      this.text = text;
   }

   public String getText() throws JMSException
   {
      //TODO lazily get the text
      return text;
   }
   
   public void clearBody() throws JMSException
   {
      super.clearBody();
      
      text = null;
   }

   // HornetQRAMessage override -----------------------------------------
   
   public void doBeforeSend() throws Exception
   {
      getBody().clear();
      getBody().writeNullableString(text);      
      
      super.doBeforeSend();
   }
   
   public void doBeforeReceive() throws Exception
   {
      super.doBeforeReceive();
      
      text = getBody().readNullableString();                        
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   // Public --------------------------------------------------------
}