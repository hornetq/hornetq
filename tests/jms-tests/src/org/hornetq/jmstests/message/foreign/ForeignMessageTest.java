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

import javax.jms.Message;
import javax.jms.TextMessage;

import org.hornetq.jmstests.message.MessageTestBase;
import org.hornetq.jmstests.message.SimpleJMSMessage;
import org.hornetq.jmstests.message.SimpleJMSTextMessage;


/**
 *
 * Tests the delivery/receipt of a foreign message
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ForeignMessageTest extends MessageTestBase
{
    public void setUp() throws Exception
    {
       super.setUp();
       this.message = createForeignMessage();
    }

    public void tearDown() throws Exception
    {
       super.tearDown();
       message = null;
    }

    protected Message createForeignMessage() throws Exception
    {
        SimpleJMSMessage m = new SimpleJMSMessage();
        log.debug("creating JMS Message type " + m.getClass().getName());

        return m;
    }
    
    public void testForeignMessageSetDestination() throws Exception
    {
       // create a Bytes foreign message
       SimpleJMSTextMessage txt = new SimpleJMSTextMessage("hello from Brazil!");
       txt.setJMSDestination(null);

       queueProd.send(txt);

       assertNotNull(txt.getJMSDestination());

       TextMessage tm = (TextMessage)queueCons.receive();
       assertNotNull(tm);
       assertEquals("hello from Brazil!", txt.getText());
    }

}
