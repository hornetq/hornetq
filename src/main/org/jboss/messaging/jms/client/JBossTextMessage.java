/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.jms.client;


import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.logging.Logger;

import javax.jms.JMSException;
import javax.jms.TextMessage;

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
 * $Id: JBossTextMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class JBossTextMessage extends JBossMessage implements TextMessage
{
   // Constants -----------------------------------------------------

   public static final byte TYPE = 3;
   
   public static final Logger log = Logger.getLogger(JBossTextMessage.class);

   // Attributes ----------------------------------------------------
   
   //We cache it locally
   private String text;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossTextMessage()
   {
      super(JBossTextMessage.TYPE);
   }
   /**
    * constructors for test purposes only
    */
   public JBossTextMessage(final ClientSession session)
   {
      super(JBossTextMessage.TYPE, session);
   }
   
   public JBossTextMessage(final ClientMessage message, ClientSession session)
   {     
      super(message, session);
   }
   
   /**
    * A copy constructor for non-JBoss Messaging JMS TextMessages.
    */
   public JBossTextMessage(final TextMessage foreign, final ClientSession session) throws JMSException
   {
      super(foreign, JBossTextMessage.TYPE, session);
      
      text = foreign.getText();
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossTextMessage.TYPE;
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

   // JBossMessage override -----------------------------------------
   
   public void doBeforeSend() throws Exception
   {
      body.putNullableString(text);      
      
      super.doBeforeSend();
   }
   
   public void doBeforeReceive() throws Exception
   {
      super.doBeforeReceive();
      
      text = body.getNullableString();                        
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   // Public --------------------------------------------------------
}