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
package org.jboss.jms.delegate;

import javax.jms.JMSException;

import org.jboss.jms.client.Closeable;
import org.jboss.messaging.newcore.Message;

/**
 * Represents the set of methods from the BrowserDelegate that are handled on the server. The rest
 * of the methods are handled in the advice stack.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface BrowserEndpoint extends Closeable
{
   /**
    * Reset the internal state of the browser endpoint so the following
    * nextMessage()/hasNextMessage()/nextMessageBlock() invocations would reflect the state of the
    * queue at the moment of the reset.
    */
   void reset() throws JMSException;

   Message nextMessage() throws JMSException;
   
   boolean hasNextMessage() throws JMSException;
      
   Message[] nextMessageBlock(int maxMessages) throws JMSException;
}
