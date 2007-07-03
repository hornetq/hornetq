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
 * @version <tt>$Revision: 2349 $</tt>
 *
 * $Id: StressTest.java 2349 2007-02-19 14:15:53Z timfox $
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
   
   
   public void testConnectionConsumer() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.start();
      
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sessSend.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      Runner[] runners = new Runner[] { new Sender("prod1", sessSend, prod, 100000),
                                        new Receiver(conn, sessReceive, 100000, queue1) };

      runRunners(runners);

      conn.close();      
   }

}
