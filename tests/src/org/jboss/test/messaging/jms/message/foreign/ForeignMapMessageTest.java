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
import javax.jms.MapMessage;
import javax.jms.Message;

import org.jboss.test.messaging.jms.message.SimpleJMSMapMessage;

/**
 * Tests the delivery/receipt of a foreign map message
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ForeignMapMessageTest extends ForeignMessageTest
{
    private String obj = new String("stringobject");

    public ForeignMapMessageTest(String name)
    {
        super(name);
    }
    
    protected Message createForeignMessage() throws Exception
    {
        SimpleJMSMapMessage m = new SimpleJMSMapMessage();
        
        log.debug("creating JMS Message type " + m.getClass().getName());
        
        m.setBoolean("boolean1",true);
        m.setChar("char1",'c');
        m.setDouble("double1",1.0D);
        m.setFloat("float1",2.0F);
        m.setInt("int1",3);
        m.setLong("long1",4L);
        m.setObject("object1",obj);
        m.setShort("short1",(short)5);
        m.setString("string1","stringvalue");

        return m;
    }
    
    protected void assertEquivalent(Message m, int mode, boolean redelivery) throws JMSException
    {
        super.assertEquivalent(m,mode,redelivery);
        
        MapMessage map = (MapMessage)m;
        
        assertTrue(map.getBoolean("boolean1"));
        assertEquals(map.getChar("char1"),'c');
        assertEquals(map.getDouble("double1"),1.0D,0.0D);
        assertEquals(map.getFloat("float1"),2.0F,0.0F);
        assertEquals(map.getInt("int1"),3);
        assertEquals(map.getLong("long1"),4L);
        assertEquals(map.getObject("object1"),obj);
        assertEquals(map.getShort("short1"),(short)5);
        assertEquals(map.getString("string1"),"stringvalue");
    }
}
