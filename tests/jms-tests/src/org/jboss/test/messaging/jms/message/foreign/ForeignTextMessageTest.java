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
package org.jboss.test.messaging.jms.message.foreign;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.jboss.test.messaging.jms.message.SimpleJMSTextMessage;

/**
 * Tests the delivery/receipt of a foreign text message
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ForeignTextMessageTest extends ForeignMessageTest
{

    protected Message createForeignMessage() throws Exception
    {
        SimpleJMSTextMessage m = new SimpleJMSTextMessage();
        m.setText("this is the payload");
        return m;
    }

    protected void assertEquivalent(Message m, int mode, boolean redelivery) throws JMSException
    {
       super.assertEquivalent(m, mode, redelivery);

       TextMessage tm = (TextMessage)m;
       assertEquals("this is the payload", tm.getText());
    }
}
