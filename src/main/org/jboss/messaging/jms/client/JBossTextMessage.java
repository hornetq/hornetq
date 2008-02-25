/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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


import java.io.DataInputStream;
import java.io.DataOutputStream;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.SafeUTF;

/**
 * This class implements javax.jms.TextMessage ported from SpyTextMessage in JBossMQ.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
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
   
   public JBossTextMessage(org.jboss.messaging.core.Message message, ClientSession session)
   {
      super(message, session);
   }
   
   /**
    * A copy constructor for non-JBoss Messaging JMS TextMessages.
    */
   public JBossTextMessage(TextMessage foreign) throws JMSException
   {
      super(foreign, JBossTextMessage.TYPE);
      
      text = foreign.getText();
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossTextMessage.TYPE;
   }
   
   public void doBeforeSend() throws Exception
   {
      beforeSend();
   }
   
   public void doBeforeReceive() throws Exception
   {
      beforeReceive();
   }
          
   // TextMessage implementation ------------------------------------

   public void setText(String text) throws JMSException
   {
      checkWrite();
      
      this.text = text;
   }

   public String getText() throws JMSException
   {
      return text;
   }
   
   public void clearBody() throws JMSException
   {
      super.clearBody();
      
      text = null;
   }

   // JBossMessage override -----------------------------------------
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void writePayload(DataOutputStream daos) throws Exception
   {
      //TODO - if send strings in plain format as opposed to UTF-8 then we can calculate the size
      //in advance more easily - so we can allocate a byte buffer - as opposed to using a stream
      
      if (text == null)
      {
         daos.writeByte(NULL);
      }
      else
      {      
         daos.writeByte(NOT_NULL);
         
         SafeUTF.safeWriteUTF(daos, text);
      }
   }
   
   protected void readPayload(DataInputStream dais) throws Exception
   {
      byte b = dais.readByte();
      
      if (b == NULL)
      {
         text = null;
      }
      else
      {
         text = SafeUTF.safeReadUTF(dais);
      } 
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   // Public --------------------------------------------------------
}