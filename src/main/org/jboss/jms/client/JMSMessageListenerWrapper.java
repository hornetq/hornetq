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
package org.jboss.jms.client;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.api.MessageHandler;
import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.util.Logger;

/**
 * 
 * A JMSMessageListenerWrapper
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JMSMessageListenerWrapper implements MessageHandler
{
   private static final Logger log = Logger.getLogger(JMSMessageListenerWrapper.class);
      
   private JBossSession session;
   
   private MessageListener listener;
   
   private int ackMode;
   
   private boolean transactedOrClientAck;
   
   public JMSMessageListenerWrapper(JBossSession session, MessageListener listener, int ackMode)
   {
      this.session = session;
      
      this.listener = listener;
      
      this.transactedOrClientAck = ackMode == Session.SESSION_TRANSACTED || ackMode == Session.CLIENT_ACKNOWLEDGE;
   }

   /**
    * In this method we apply the JMS acknowledgement and redelivery semantics
    * as per JMS spec
    */
   public void onMessage(Message message)
   {
      JBossMessage jbm = JBossMessage.createMessage(message, session.getCoreSession());
      
      try
      {
         jbm.doBeforeReceive();
      }
      catch (Exception e)
      {
         log.error("Failed to prepare message", e);
         
         return;
      }
      
      if (transactedOrClientAck)
      {
         try
         {
            session.getCoreSession().delivered();
         }
         catch (JMSException e)
         {
            log.error("Failed to deliver message", e);
         }
      }
      
      try
      {         
         listener.onMessage(jbm);
      }
      catch (RuntimeException e)
      {
         //See JMS 1.1 spec, section 4.5.2
         
         log.warn("Unhandled exception thrown from onMessage", e);
         
         if (!transactedOrClientAck)
         {            
            try
            {
               session.getCoreSession().rollback();
               
               session.setRecoverCalled(true);
            }
            catch (Exception e2)
            {
               log.error("Failed to recover session", e2);
            }
         }
      }            
      
      if (!session.isRecoverCalled() && !transactedOrClientAck)
      {
         try
         {
            //We don't want to call this if the connection/session was closed from inside onMessage
            if (!session.getCoreSession().isClosed())
            {
               session.getCoreSession().delivered();
            }
         }
         catch (JMSException e)
         {
            log.error("Failed to deliver message", e);
         }
      }
      
      session.setRecoverCalled(false);
      
   }

}
