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
package org.jboss.jms.client.container;

import java.io.Serializable;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.message.BytesMessageProxy;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.jms.message.MapMessageProxy;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.message.MessageIdGenerator;
import org.jboss.jms.message.ObjectMessageProxy;
import org.jboss.jms.message.StreamMessageProxy;
import org.jboss.jms.message.TextMessageProxy;

/**
 * Constructs various things that can be created entirely or partially on the client.
 * 
 * This aspect is PER_INSTANCE.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class FactoryAspect
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Object handleCreateMessage(Invocation invocation) throws Throwable
   {
      JBossMessage jbm = new JBossMessage(0);
       
      return new MessageProxy(jbm, 0);
   }
   
   public Object handleCreateBytesMessage(Invocation invocation) throws Throwable
   {
      JBossBytesMessage jbm = new JBossBytesMessage(0);
         
      return new BytesMessageProxy(jbm, 0);
   }
   
   public Object handleCreateMapMessage(Invocation invocation) throws Throwable
   {
      JBossMapMessage jbm = new JBossMapMessage(0);
       
      return new MapMessageProxy(jbm, 0);      
   }
   
   public Object handleCreateObjectMessage(Invocation invocation) throws Throwable
   {
      JBossObjectMessage jbm = new JBossObjectMessage(0);
       
      MethodInvocation mi = (MethodInvocation)invocation;
      
      if (mi.getArguments() != null)
      {
         jbm.setObject((Serializable)mi.getArguments()[0]);
      }
      
      return new ObjectMessageProxy(jbm, 0);
   }
   
   public Object handleCreateStreamMessage(Invocation invocation) throws Throwable
   {
      JBossStreamMessage jbm = new JBossStreamMessage(0);
      
      return new StreamMessageProxy(jbm, 0);
   }
   
   public Object handleCreateTextMessage(Invocation invocation) throws Throwable
   {  
      JBossTextMessage jbm = new JBossTextMessage(0);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      if (mi.getArguments() != null)
      {
         jbm.setText((String)mi.getArguments()[0]);
      }
      
      return new TextMessageProxy(jbm, 0);
   }   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
		
   // Inner classes -------------------------------------------------
}
