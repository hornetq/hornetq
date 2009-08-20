/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jmstests.message.foreign;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.hornetq.jmstests.message.SimpleJMSTextMessage;

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
