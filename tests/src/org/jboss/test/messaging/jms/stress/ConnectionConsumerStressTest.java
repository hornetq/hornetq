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
package org.jboss.test.messaging.jms.stress;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * 
 * A ConnectionConsumerStressTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * ConnectionConsumerStressTest.java,v 1.1 2006/03/31 21:20:12 timfox Exp
 */
public class ConnectionConsumerStressTest extends StressTestBase
{
   public ConnectionConsumerStressTest(String name)
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
   
   public void testSimple() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, NUM_PERSISTENT_MESSAGES),
                                        new Receiver(conn, sessReceive, NUM_PERSISTENT_MESSAGES, queue1) };

      runRunners(runners);

      conn.close();      
   }
   
      
}
