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
package org.jboss.jms.client.stubs;

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.remoting.InvokerLocator;

/**
 * 
 * The client stub class for BrowserDelegate
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class BrowserStub extends ClientStubBase implements BrowserDelegate
{
   private static final long serialVersionUID = 8293543769773757409L;
   
   public BrowserStub(String objectID, InvokerLocator locator)
   {
      super(objectID, locator);
   }
   
   public String getStackName()
   {
      return "BrowserStack";
   }

   public void close() throws JMSException
   {
   }

   public void closing() throws JMSException
   {
   }

   public boolean hasNextMessage() throws JMSException
   {
      return false;
   }

   public Message nextMessage() throws JMSException
   {
      return null;
   }

   public Message[] nextMessageBlock(int maxMessages) throws JMSException
   {
      return null;
   }

}
