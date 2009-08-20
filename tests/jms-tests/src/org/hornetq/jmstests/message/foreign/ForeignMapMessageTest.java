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
import javax.jms.MapMessage;
import javax.jms.Message;

import org.hornetq.jmstests.message.SimpleJMSMapMessage;

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
