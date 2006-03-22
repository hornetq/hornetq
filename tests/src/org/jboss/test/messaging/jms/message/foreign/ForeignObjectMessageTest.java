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
package org.jboss.test.messaging.jms.message.foreign;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import org.jboss.test.messaging.jms.message.SimpleJMSObjectMessage;

/**
 * Tests the delivery/receipt of a foreign object message
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ForeignObjectMessageTest extends ForeignMessageTest
{
    private ForeignTestObject testObj;
    
    public ForeignObjectMessageTest(String name)
    {
        super(name);
    }
    
    public void setUp() throws Exception
    {
        testObj = new ForeignTestObject("hello",2.2D); 
        super.setUp();
       
    }

    public void tearDown() throws Exception
    {
       super.tearDown();
       testObj = null;
    }

    protected Message createForeignMessage() throws Exception
    {
        SimpleJMSObjectMessage m = new SimpleJMSObjectMessage();
        
        log.debug("creating JMS Message type " + m.getClass().getName());
        
        m.setObject(testObj);

        return m;
    }
    
    protected void assertEquivalent(Message m, int mode) throws JMSException
    {
        super.assertEquivalent(m,mode);
        
        ObjectMessage obj = (ObjectMessage)m;
        
        assertNotNull(obj.getObject());
        assertEquals(obj.getObject(),testObj);
    }
    

}
