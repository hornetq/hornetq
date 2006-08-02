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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.StreamMessage;

import org.jboss.test.messaging.jms.message.SimpleJMSStreamMessage;

/**
 * Tests the delivery/receipt of a foreign stream message
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ForeignStreamMessageTest extends ForeignMessageTest
{

    public ForeignStreamMessageTest(String name)
    {
        super(name);
    }
    
    public void setUp() throws Exception
    {
       super.setUp();
    }

    public void tearDown() throws Exception
    {
       super.tearDown();
    }

    protected Message createForeignMessage() throws Exception
    {
        SimpleJMSStreamMessage m = new SimpleJMSStreamMessage();
        
        log.debug("creating JMS Message type " + m.getClass().getName()); 
        
        m.writeBoolean(true);
        m.writeBytes("jboss".getBytes());
        m.writeChar('c');
        m.writeDouble(1.0D);
        m.writeFloat(2.0F);
        m.writeInt(3);
        m.writeLong(4L);
        m.writeObject("object");
        m.writeShort((short)5);
        m.writeString("stringvalue");

        return m;
    }
    
    protected void assertEquivalent(Message m, int mode) throws JMSException
    {
        super.assertEquivalent(m,mode);
        
        StreamMessage sm = (StreamMessage)m;
        
        assertTrue(sm.readBoolean());
        
        byte bytes[] = new byte[5];
        sm.readBytes(bytes);
        String s = new String(bytes);
        assertEquals("jboss",s);
        assertEquals(-1,sm.readBytes(bytes));
        
        assertEquals(sm.readChar(),'c');
        assertEquals(sm.readDouble(),1.0D,0.0D);
        assertEquals(sm.readFloat(),2.0F,0.0F);
        assertEquals(sm.readInt(),3);
        assertEquals(sm.readLong(),4L);
        assertEquals(sm.readObject(),"object");
        assertEquals(sm.readShort(),(short)5);
        assertEquals(sm.readString(),"stringvalue");
    }

}
