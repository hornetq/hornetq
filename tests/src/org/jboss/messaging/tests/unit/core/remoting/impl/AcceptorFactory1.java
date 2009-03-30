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
package org.jboss.messaging.tests.unit.core.remoting.impl;

import java.util.Map;

import org.jboss.messaging.core.remoting.spi.Acceptor;
import org.jboss.messaging.core.remoting.spi.AcceptorFactory;
import org.jboss.messaging.core.remoting.spi.BufferHandler;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;

/**
 * 
 * A AcceptorFactory1
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class AcceptorFactory1 implements AcceptorFactory
{

   public Acceptor createAcceptor(Map<String, Object> params,
         BufferHandler handler, ConnectionLifeCycleListener listener)
   {
      return new Acceptor1();
   }

   public class Acceptor1 implements Acceptor
   {

      public void start() throws Exception
      {
      }

      public void stop()
      {  
      }
      
      public void pause()
      {         
      }    
      
      public void resume()
      {         
      }
      
      public boolean isStarted()
      {
         return false;
      }      
   }
   
}
