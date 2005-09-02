
package org.jboss.test.messaging.jms.managed;

import javax.naming.InitialContext;
import javax.rmi.PortableRemoteObject;

import junit.framework.TestCase;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.ServerManagement;


public class ManagedTest extends TestCase
{   
   protected InitialContext initialContext;
   
   private static Logger log = Logger.getLogger(ManagedTest.class);
   
   public void setUp() throws Exception
   {
      super.setUp();                  
      
      ServerManagement.setRemote(true);
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      
      
      log.debug("Done setup()");

   }
   
   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      super.tearDown();
   }
   
   
   public void test1() throws Exception
   {
      log.trace("Looking up EJB reference");
      Object objref = 
            initialContext.lookup("/jms-test-ejbs/JMSTestHome");
      log.trace("Looked up home");
         
      JMSTestHome home = (JMSTestHome)PortableRemoteObject.narrow(objref, JMSTestHome.class);      
         
      JMSTest testBean = home.create();
        
      testBean.test1();
      testBean.remove(); 
      log.trace("Done");
   }
}
