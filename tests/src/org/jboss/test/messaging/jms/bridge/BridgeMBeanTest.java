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
package org.jboss.test.messaging.jms.bridge;

import javax.management.ObjectName;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A BridgeMBeanTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeMBeanTest extends MessagingTestCase
{

   public BridgeMBeanTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   /*
    
    Example config
    
      String config = 
         "<mbean code=org.jboss.jms.server.bridge.BridgeService " +
                 "name=jboss.messaging:service=Bridge,name=exampleBridge" +
                 "xmbean-dd=\"xmdesc/Bridge-xmbean.xml\">" +      
            "<attribute name=\"SourceConnectionFactoryLookup\">/ConnectionFactory</attribute>"+      
            "<attribute name=\"TargetConnectionFactoryLookup\">/ConnectionFactory</attribute>"+     
            "<attribute name=\"SourceDestinationLookup\">/topic/sourceTopic</attribute>"+     
            "<attribute name=\"TargetDestinationLookup\">/queue/targetQueue</attribute>"+     
            "<attribute name=\"SourceUsername\">bob</attribute>"+      
            "<attribute name=\"SourcePassword\">pwd1</attribute>"+      
            "<attribute name=\"TargetUsername\">jane</attribute>"+      
            "<attribute name=\"TargetPassword\">pwd2</attribute>"+      
            "<attribute name=\"QualityOfServiceMode\">2</attribute>"+      
            "<attribute name=\"Selector\">vegetable='marrow'</attribute>"+      
            "<attribute name=\"MaxBatchSize\">100</attribute>"+           
            "<attribute name=\"MaxBatchTime\">5000</attribute>"+      
            "<attribute name=\"SubName\">mySubscription</attribute>"+      
            "<attribute name=\"ClientID\">clientid-123</attribute>"+      
            "<attribute name=\"FailureRetryInterval\">5000</attribute>"+      
            "<attribute name=\"MaxRetries\">-1</attribute>"+      
            "<attribute name=\"SourceJNDIProperties\"><![CDATA["+
"java.naming.factory.initial=org.jnp.interfaces.NamingContextFactory\n"+
"java.naming.provider.url=jnp://server1:1099\n"+
"java.naming.factory.url.pkgs=org.jboss.naming:org.jnp.interfaces\n"+
"]]>"+
            "</attribute>";
            "<attribute name=\"TargetJNDIProperties\"><![CDATA["+
"java.naming.factory.initial=org.jnp.interfaces.NamingContextFactory\n"+
"java.naming.provider.url=jnp://server1:1099\n"+
"java.naming.factory.url.pkgs=org.jboss.naming:org.jnp.interfaces\n"+
"]]>"+
            "</attribute>"+
         "</mbean>";    

    
    */
   
   
   private ObjectName deployBridge(String bridgeName, String sourceCFLookup, String targetCFLookup,
                             String sourceDestLookup, String targetDestLookup,
                             String sourceUsername, String sourcePassword,
                             String targetUsername, String targetPassword,
                             int qos, String selector, int maxBatchSize,
                             long maxBatchTime, String subName, String clientID,
                             long failureRetryInterval, int maxRetries,
                             String sourceJNDIProperties,
                             String targetJNDIProperties) throws Exception
   {
      String config = 
      "<mbean code=org.jboss.jms.server.bridge.BridgeService " +
      "name=jboss.messaging:service=Bridge,name=" + bridgeName +
      "xmbean-dd=\"xmdesc/Bridge-xmbean.xml\">" +      
          "<attribute name=\"SourceConnectionFactoryLookup\">" + sourceCFLookup + "/attribute>"+      
          "<attribute name=\"TargetConnectionFactoryLookup\">" + targetCFLookup + "</attribute>"+     
          "<attribute name=\"SourceDestinationLookup\">" + sourceDestLookup + "</attribute>"+     
          "<attribute name=\"TargetDestinationLookup\">" + targetDestLookup + "</attribute>"+     
          sourceUsername == null ? "" :
          "<attribute name=\"SourceUsername\">" + sourceUsername + "</attribute>"+      
          sourcePassword == null ? "" :
          "<attribute name=\"SourcePassword\">" + sourcePassword +"</attribute>"+     
          targetUsername == null ? "" :
          "<attribute name=\"TargetUsername\">" + targetUsername +"</attribute>"+    
          targetPassword == null ? "" :
          "<attribute name=\"TargetPassword\">" + targetPassword + "</attribute>"+      
          "<attribute name=\"QualityOfServiceMode\">" + qos +"</attribute>"+      
          selector == null ? "" :
          "<attribute name=\"Selector\">" + selector + "</attribute>"+      
          "<attribute name=\"MaxBatchSize\">" + maxBatchSize + "</attribute>"+           
          "<attribute name=\"MaxBatchTime\">" + maxBatchTime +"</attribute>"+    
          subName == null ? "" :
          "<attribute name=\"SubName\">" + subName + "</attribute>"+      
          clientID == null ? "" :
          "<attribute name=\"ClientID\">" + clientID + "</attribute>"+      
          "<attribute name=\"FailureRetryInterval\">" + failureRetryInterval + "</attribute>"+      
          "<attribute name=\"MaxRetries\">" + maxRetries +"</attribute>"+      
          sourceJNDIProperties == null ? "" :
          "<attribute name=\"SourceJNDIProperties\"><![CDATA["+
         sourceJNDIProperties +
         "]]>"+
          "</attribute>"+
          targetJNDIProperties == null ? "" :
          "<attribute name=\"TargetJNDIProperties\"><![CDATA["+
         targetJNDIProperties +
         "]]>"+
          "</attribute>"+
      "</mbean>";
      
      log.info(config);
      
      return ServerManagement.deploy(config);            
   }

}
