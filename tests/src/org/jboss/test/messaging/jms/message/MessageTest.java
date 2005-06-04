/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import java.util.Enumeration;
import java.util.HashSet;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.util.InVMInitialContextFactory;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Session;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Destination;
import javax.naming.InitialContext;

/**
 * Base class for all tests concerning message headers, properties, etc.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Destination queue;
   protected Connection producerConnection, consumerConnection;
   protected Session queueProducerSession, queueConsumerSession;
   protected MessageProducer queueProducer;
   protected MessageConsumer queueConsumer;

   // Constructors --------------------------------------------------

   public MessageTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.startInVMServer();
      ServerManagement.deployQueue("Queue");

      InitialContext ic = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/messaging/ConnectionFactory");
      queue = (Destination)ic.lookup("/messaging/queues/Queue");

      producerConnection = cf.createConnection();
      consumerConnection = cf.createConnection();

      queueProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queueConsumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      queueProducer = queueProducerSession.createProducer(queue);
      queueConsumer = queueConsumerSession.createConsumer(queue);

      consumerConnection.start();
   }

   public void tearDown() throws Exception
   {
      // TODO uncomment these
      producerConnection.close();
      consumerConnection.close();

      ServerManagement.undeployQueue("Queue");
      ServerManagement.stopInVMServer();

      super.tearDown();
   }

   public void testProperties() throws Exception
   {
   	Message m1 = queueProducerSession.createMessage();
   	
     
    //Some arbitrary values
    boolean myBool = true;
    byte myByte = 13;
    short myShort = 15321;
    int myInt = 0x71ab6c80;
    long myLong = 0x20bf1e3fb6fa31dfL;
    float myFloat = Float.MAX_VALUE - 23465;
    double myDouble = Double.MAX_VALUE - 72387633;
    String myString = "abcdef&^*&!^ghijkl";
  	
   	
   	
   	m1.setBooleanProperty("myBool", myBool);  	
   	m1.setByteProperty("myByte", myByte);
   	m1.setShortProperty("myShort", myShort);
   	m1.setIntProperty("myInt", myInt);
   	m1.setLongProperty("myLong", myLong);
   	m1.setFloatProperty("myFloat", myFloat);
   	m1.setDoubleProperty("myDouble", myDouble);
   	m1.setStringProperty("myString", myString);
   	
   	m1.setObjectProperty("myBool", new Boolean(myBool));  	
   	m1.setObjectProperty("myByte", new Byte(myByte));
   	m1.setObjectProperty("myShort", new Short(myShort));
   	m1.setObjectProperty("myInt", new Integer(myInt));
   	m1.setObjectProperty("myLong", new Long(myLong));
   	m1.setObjectProperty("myFloat", new Float(myFloat));
   	m1.setObjectProperty("myDouble", new Double(myDouble));
   	m1.setObjectProperty("myString", myString);
   	
   	try
   	{
   		m1.setObjectProperty("myIllegal", new Object());
   		fail();
   	}
   	catch (javax.jms.MessageFormatException e)
   	{}
   	
   	
   	queueProducer.send(queue, m1);
   	
   	Message m2 = queueConsumer.receive(2000);
   	
   	assertNotNull(m2);
   	
   	assertEquals(myBool, m2.getBooleanProperty("myBool"));
   	assertEquals(myByte, m2.getByteProperty("myByte"));
   	assertEquals(myShort, m2.getShortProperty("myShort"));
   	assertEquals(myInt, m2.getIntProperty("myInt"));
   	assertEquals(myLong, m2.getLongProperty("myLong"));
   	assertEquals(myFloat, m2.getFloatProperty("myFloat"), 0);
   	assertEquals(myDouble, m2.getDoubleProperty("myDouble"), 0);
   	assertEquals(myString, m2.getStringProperty("myString"));
  
   	
   	//Properties should now be read-only
   	try
   	{
	   	m2.setBooleanProperty("myBool", myBool);
	   	fail();
   	}
   	catch (MessageNotWriteableException e) {}
   	
   	try
   	{
   		m2.setByteProperty("myByte", myByte);
	   	fail();
   	}
   	catch (MessageNotWriteableException e) {}
   	
   	try
   	{
   		m2.setShortProperty("myShort", myShort);
	   	fail();
   	}
   	catch (MessageNotWriteableException e) {}
   	
   	try
   	{
   		m2.setIntProperty("myInt", myInt);
	   	fail();
   	}
   	catch (MessageNotWriteableException e) {}
   	
   	try
   	{
   		m2.setLongProperty("myLong", myLong);
	   	fail();
   	}
   	catch (MessageNotWriteableException e) {}
   	
   	try
   	{
   		m2.setFloatProperty("myFloat", myFloat);
	   	fail();
   	}
   	catch (MessageNotWriteableException e) {}
   	
   	try
   	{
   		m2.setDoubleProperty("myDouble", myDouble);
	   	fail();
   	}
   	catch (MessageNotWriteableException e) {}
   	
   	try
   	{
   		m2.setStringProperty("myString", myString);
	   	fail();
   	}
   	catch (MessageNotWriteableException e) {}
   	

   	
   	assertTrue(m2.propertyExists("myBool"));
   	assertTrue(m2.propertyExists("myByte"));
   	assertTrue(m2.propertyExists("myShort"));
   	assertTrue(m2.propertyExists("myInt"));
   	assertTrue(m2.propertyExists("myLong"));
   	assertTrue(m2.propertyExists("myFloat"));
   	assertTrue(m2.propertyExists("myDouble"));
   	assertTrue(m2.propertyExists("myString"));

   	
   	assertFalse(m2.propertyExists("sausages"));
	   	
   	HashSet propNames = new HashSet();
   	Enumeration en = m2.getPropertyNames();
   	while (en.hasMoreElements())
   	{
   		String propName = (String)en.nextElement();
   		propNames.add(propName);
   	}
   	
   	assertEquals(8, propNames.size());
   	
   	assertTrue(propNames.contains("myBool"));
   	assertTrue(propNames.contains("myByte"));
   	assertTrue(propNames.contains("myShort"));
   	assertTrue(propNames.contains("myInt"));
   	assertTrue(propNames.contains("myLong"));
   	assertTrue(propNames.contains("myFloat"));
   	assertTrue(propNames.contains("myDouble"));
   	assertTrue(propNames.contains("myString"));
   	
   	
   	// Check property conversions
   	
   	//Boolean property can be read as String but not anything else
   	
   	assertEquals(String.valueOf(myBool), m2.getStringProperty("myBool"));
   	
   	try
   	{
   		m2.getByteProperty("myBool");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getShortProperty("myBool");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getIntProperty("myBool");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getLongProperty("myBool");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getFloatProperty("myBool");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getDoubleProperty("myBool");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	
   	// byte property can be read as short, int, long or String
   	
   	assertEquals((short)myByte, m2.getShortProperty("myByte"));
   	assertEquals((int)myByte, m2.getIntProperty("myByte"));
   	assertEquals((long)myByte, m2.getLongProperty("myByte"));   	
   	assertEquals(String.valueOf(myByte), m2.getStringProperty("myByte"));
   	
   	try
   	{
   		m2.getBooleanProperty("myByte");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getFloatProperty("myByte");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getDoubleProperty("myByte");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	
   	// short property can be read as int, long or String
   	
   	assertEquals((int)myShort, m2.getIntProperty("myShort"));
   	assertEquals((long)myShort, m2.getLongProperty("myShort"));   	
   	assertEquals(String.valueOf(myShort), m2.getStringProperty("myShort"));
   	
   	try
   	{
   		m2.getByteProperty("myShort");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getBooleanProperty("myShort");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getFloatProperty("myShort");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getDoubleProperty("myShort");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	// int property can be read as long or String
   	   	
   	assertEquals((long)myInt, m2.getLongProperty("myInt"));   	
   	assertEquals(String.valueOf(myInt), m2.getStringProperty("myInt"));
   	
   	try
   	{
   		m2.getShortProperty("myInt");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getByteProperty("myInt");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getBooleanProperty("myInt");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getFloatProperty("myInt");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getDoubleProperty("myInt");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	
   	// long property can be read as String
   	   	
   	assertEquals(String.valueOf(myLong), m2.getStringProperty("myLong"));
   	
   	try
   	{
   		m2.getIntProperty("myLong");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getShortProperty("myLong");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getByteProperty("myLong");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getBooleanProperty("myLong");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getFloatProperty("myLong");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getDoubleProperty("myLong");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	
   	// float property can be read as double or String
   	
   	assertEquals(String.valueOf(myFloat), m2.getStringProperty("myFloat"));
   	assertEquals((double)myFloat, m2.getDoubleProperty("myFloat"), 0);
   	
   	try
   	{
   		m2.getIntProperty("myFloat");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getShortProperty("myFloat");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getLongProperty("myFloat");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getByteProperty("myFloat");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getBooleanProperty("myFloat");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	
   	
   	// double property can be read as String
   	
   	assertEquals(String.valueOf(myDouble), m2.getStringProperty("myDouble"));

   	
   	try
   	{
   		m2.getFloatProperty("myDouble");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getIntProperty("myDouble");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getShortProperty("myDouble");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getByteProperty("myDouble");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getBooleanProperty("myDouble");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	try
   	{
   		m2.getFloatProperty("myDouble");
   		fail();
   	} catch (MessageFormatException e) {}
   	
   	m2.clearProperties();
   	
   	assertFalse(m2.getPropertyNames().hasMoreElements());
   	
   	
   	
   	// Test String -> Numeric and bool conversions
   	Message m3 = queueProducerSession.createMessage();
   	
   	m3.setStringProperty("myBool", String.valueOf(myBool));
   	m3.setStringProperty("myByte", String.valueOf(myByte));
   	m3.setStringProperty("myShort", String.valueOf(myShort));
   	m3.setStringProperty("myInt", String.valueOf(myInt));
   	m3.setStringProperty("myLong", String.valueOf(myLong));
   	m3.setStringProperty("myFloat", String.valueOf(myFloat));
   	m3.setStringProperty("myDouble", String.valueOf(myDouble));   
   	m3.setStringProperty("myIllegal", "xyz123");
   	
   	assertEquals(myBool, m3.getBooleanProperty("myBool"));
   	assertEquals(myByte, m3.getByteProperty("myByte"));
   	assertEquals(myShort, m3.getShortProperty("myShort"));
   	assertEquals(myInt, m3.getIntProperty("myInt"));
   	assertEquals(myLong, m3.getLongProperty("myLong"));
   	assertEquals(myFloat, m3.getFloatProperty("myFloat"), 0);
   	assertEquals(myDouble, m3.getDoubleProperty("myDouble"), 0);
  
   	m3.getBooleanProperty("myIllegal");
  
   	try
   	{
   		m3.getByteProperty("myIllegal");
   		fail();
   	}
   	catch (NumberFormatException e) {}
   	try
   	{
   		m3.getShortProperty("myIllegal");
   		fail();
   	}
   	catch (NumberFormatException e) {}
   	try
   	{
   		m3.getIntProperty("myIllegal");
   		fail();
   	}
   	catch (NumberFormatException e) {}
   	try
   	{
   		m3.getLongProperty("myIllegal");
   		fail();
   	}
   	catch (NumberFormatException e) {}
   	try
   	{
   		m3.getFloatProperty("myIllegal");
   		fail();
   	}
   	catch (NumberFormatException e) {}
   	try
   	{
   		m3.getDoubleProperty("myIllegal");
   		fail();
   	}
   	catch (NumberFormatException e) {}
   	
  
   
   	
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
