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
import org.jboss.jms.message.BytesMessageDelegate;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.jms.message.MapMessageDelegate;
import org.jboss.jms.message.MessageDelegate;
import org.jboss.jms.message.ObjectMessageDelegate;
import org.jboss.jms.message.StreamMessageDelegate;
import org.jboss.jms.message.TextMessageDelegate;


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
   
   protected long ordering;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Object handleCreateMessage(Invocation invocation) throws Throwable
   {
      JBossMessage jbm = new JBossMessage((String)null);
      
      jbm.setOrdering(ordering++);
      
      return new MessageDelegate(jbm, 0);
   }
   
   public Object handleCreateBytesMessage(Invocation invocation) throws Throwable
   {
      JBossBytesMessage jbm = new JBossBytesMessage((String)null);
      
      jbm.setOrdering(ordering++);
      
      return new BytesMessageDelegate(jbm, 0);
   }
   
   public Object handleCreateMapMessage(Invocation invocation) throws Throwable
   {
      JBossMapMessage jbm = new JBossMapMessage((String)null);
      
      jbm.setOrdering(ordering++);
      
      return new MapMessageDelegate(jbm, 0);      
   }
   
   public Object handleCreateObjectMessage(Invocation invocation) throws Throwable
   {
      JBossObjectMessage jbm = new JBossObjectMessage((String)null);
      
      jbm.setOrdering(ordering++);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      if (mi.getArguments() != null)
      {
         jbm.setObject((Serializable)mi.getArguments()[0]);
      }
      
      return new ObjectMessageDelegate(jbm, 0);
   }
   
   public Object handleCreateStreamMessage(Invocation invocation) throws Throwable
   {
      JBossStreamMessage jbm = new JBossStreamMessage((String)null);
      
      jbm.setOrdering(ordering++);
      
      return new StreamMessageDelegate(jbm, 0);
   }
   
   public Object handleCreateTextMessage(Invocation invocation) throws Throwable
   {
      JBossTextMessage jbm = new JBossTextMessage((String)null);
      
      jbm.setOrdering(ordering++);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      if (mi.getArguments() != null)
      {
         jbm.setText((String)mi.getArguments()[0]);
      }
      
      return new TextMessageDelegate(jbm, 0);
   }   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
		
   // Inner classes -------------------------------------------------
}
