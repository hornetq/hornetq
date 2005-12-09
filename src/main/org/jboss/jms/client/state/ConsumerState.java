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
package org.jboss.jms.client.state;

import java.util.Collections;

import javax.jms.Destination;

import org.jboss.jms.delegate.ConsumerDelegate;

/**
 * State corresponding to a Consumer
 * This state is acessible inside aspects/interceptors
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ConsumerState extends HierarchicalStateBase
{
   private Destination destination;
   
   private String selector;
   
   private boolean noLocal;
   
   private String consumerID;
   
   private boolean isConnectionConsumer;
   
   public ConsumerState(SessionState parent, ConsumerDelegate delegate, Destination dest,
                        String selector, boolean noLocal, String consumerID, boolean isCC)
   {
      super(parent, delegate);
      children = Collections.EMPTY_SET;
      this.destination = dest;
      this.selector = selector;
      this.noLocal = noLocal;      
      this.consumerID = consumerID;
      this.isConnectionConsumer = isCC;
   }
    
   public Destination getDestination()
   {
      return destination;
   }
   
   public String getSelector()
   {
      return selector;
   }
   
   public boolean isNoLocal()
   {
      return noLocal;
   }
   
   public String getConsumerID()
   {
      return consumerID;
   }
   
   public boolean isConnectionConsumer()
   {
      return isConnectionConsumer;
   }
   
}


