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
package org.jboss.messaging.tests.unit.core.deployers.impl;


import junit.framework.TestCase;

import org.jboss.messaging.core.deployers.Deployer;
import org.jboss.messaging.core.deployers.impl.XmlDeployer;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * tests the abstract deployer class
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class DeployerTest  extends TestCase
{
   private String conf1 = "<deployment>\n" +
           "   <test name=\"test1\">content1</test>\n" +
           "   <test name=\"test2\">content2</test>\n" +
           "   <test name=\"test3\">content3</test>\n" +
           "   <test name=\"test4\">content4</test>\n" +
           "</deployment>";

   private String conf2 = "<deployment>\n" +
           "   <test name=\"test1\">content1</test>\n" +
           "   <test name=\"test2\">contenthaschanged2</test>\n" +
           "   <test name=\"test3\">contenthaschanged3</test>\n" +
           "   <test name=\"test4\">content4</test>\n" +
           "</deployment>";

   private String conf3 = "<deployment>\n" +
           "   <test name=\"test1\">content1</test>\n" +
           "   <test name=\"test2\">contenthaschanged2</test>\n" +
           "</deployment>";

   private String conf4 = "<deployment>\n" +
           "   <test name=\"test1\">content1</test>\n" +
           "   <test name=\"test2\">content2</test>\n" +
           "   <test name=\"test3\">content3</test>\n" +
           "   <test name=\"test4\">content4</test>\n" +
           "   <test name=\"test5\">content5</test>\n" +
           "   <test name=\"test6\">content6</test>\n" +
           "</deployment>";

   private URL url;


   protected void setUp() throws Exception
   {
      super.setUp();
      url = new URL("http://thisdoesntmatter");
   }

   public void testDeploy() throws Exception
   {
      Element e = XMLUtil.stringToElement(conf1);
      TestDeployer testDeployer = new TestDeployer();
      testDeployer.setElement(e);
      testDeployer.deploy(url);
      assertEquals(testDeployer.getDeployments(), 4);
      assertNotNull(testDeployer.getNodes().get("test1"));
      assertNotNull(testDeployer.getNodes().get("test2"));
      assertNotNull(testDeployer.getNodes().get("test3"));
      assertNotNull(testDeployer.getNodes().get("test4"));
      assertEquals(testDeployer.getNodes().get("test1").getTextContent(), "content1");
      assertEquals(testDeployer.getNodes().get("test2").getTextContent(), "content2");
      assertEquals(testDeployer.getNodes().get("test3").getTextContent(), "content3");
      assertEquals(testDeployer.getNodes().get("test4").getTextContent(), "content4");
   }

   public void testRedeploy() throws Exception
   {
      Element e = XMLUtil.stringToElement(conf1);
      TestDeployer testDeployer = new TestDeployer();
      testDeployer.setElement(e);
      testDeployer.deploy(url);
      e = XMLUtil.stringToElement(conf2);
      testDeployer.setElement(e);
      testDeployer.redeploy(url);
      assertEquals(testDeployer.getDeployments(), 4);
      assertNotNull(testDeployer.getNodes().get("test1"));
      assertNotNull(testDeployer.getNodes().get("test2"));
      assertNotNull(testDeployer.getNodes().get("test3"));
      assertNotNull(testDeployer.getNodes().get("test4"));
      assertEquals(testDeployer.getNodes().get("test1").getTextContent(), "content1");
      assertEquals(testDeployer.getNodes().get("test2").getTextContent(), "contenthaschanged2");
      assertEquals(testDeployer.getNodes().get("test3").getTextContent(), "contenthaschanged3");
      assertEquals(testDeployer.getNodes().get("test4").getTextContent(), "content4");
   }

   public void testRedeployRemovingNodes() throws Exception
   {
      Element e = XMLUtil.stringToElement(conf1);
      TestDeployer testDeployer = new TestDeployer();
      testDeployer.setElement(e);
      testDeployer.deploy(url);
      e = XMLUtil.stringToElement(conf3);
      testDeployer.setElement(e);
      testDeployer.redeploy(url);
      assertEquals(testDeployer.getDeployments(), 2);
      assertNotNull(testDeployer.getNodes().get("test1"));
      assertNotNull(testDeployer.getNodes().get("test2"));
      assertNull(testDeployer.getNodes().get("test3"));
      assertNull(testDeployer.getNodes().get("test4"));
      assertEquals(testDeployer.getNodes().get("test1").getTextContent(), "content1");
      assertEquals(testDeployer.getNodes().get("test2").getTextContent(), "contenthaschanged2");
   }

   public void testRedeployAddingNodes() throws Exception
   {
      Element e = XMLUtil.stringToElement(conf1);
      TestDeployer testDeployer = new TestDeployer();
      testDeployer.setElement(e);
      testDeployer.deploy(url);
      e = XMLUtil.stringToElement(conf4);
      testDeployer.setElement(e);
      testDeployer.redeploy(url);
      assertEquals(testDeployer.getDeployments(), 6);
      assertNotNull(testDeployer.getNodes().get("test1"));
      assertNotNull(testDeployer.getNodes().get("test2"));
      assertNotNull(testDeployer.getNodes().get("test3"));
      assertNotNull(testDeployer.getNodes().get("test4"));
      assertNotNull(testDeployer.getNodes().get("test5"));
      assertNotNull(testDeployer.getNodes().get("test6"));
      assertEquals(testDeployer.getNodes().get("test1").getTextContent(), "content1");
      assertEquals(testDeployer.getNodes().get("test2").getTextContent(), "content2");
      assertEquals(testDeployer.getNodes().get("test3").getTextContent(), "content3");
      assertEquals(testDeployer.getNodes().get("test4").getTextContent(), "content4");      
      assertEquals(testDeployer.getNodes().get("test5").getTextContent(), "content5");
      assertEquals(testDeployer.getNodes().get("test6").getTextContent(), "content6");
   }

   public void testUndeploy() throws Exception
   {
      Element e = XMLUtil.stringToElement(conf1);
      TestDeployer testDeployer = new TestDeployer();
      testDeployer.setElement(e);
      testDeployer.deploy(url);
      testDeployer.undeploy(url);
      assertEquals(testDeployer.getDeployments(), 0);
      assertNull(testDeployer.getNodes().get("test1"));
      assertNull(testDeployer.getNodes().get("test2"));
      assertNull(testDeployer.getNodes().get("test3"));
      assertNull(testDeployer.getNodes().get("test4"));
   }
   class TestDeployer extends XmlDeployer
   {
      private String elementname = "test";
      Element element = null;
      private int deployments = 0;
      ArrayList<String> contents = new ArrayList<String>();
      HashMap<String, Node> nodes = new HashMap<String, Node>();

      public HashMap<String, Node> getNodes()
      {
         return nodes;
      }

      public ArrayList<String> getContents()
      {
         return contents;
      }

      public int getDeployments()
      {
         return deployments;
      }

      public String getElementname()
      {
         return elementname;
      }

      public void setElementname(String elementname)
      {
         this.elementname = elementname;
      }

      public Element getElement()
      {
         return element;
      }

      public void setElement(Element element)
      {
         this.element = element;
      }

      public String[] getElementTagName()
      {
         return new String[]{elementname};
      }


      public String getConfigFileName()
      {
         return "test";
      }

      public void deploy(Node node) throws Exception
      {
         deployments++;
         contents.add(node.getTextContent());
         nodes.put(node.getAttributes().getNamedItem(NAME_ATTR).getNodeValue(), node);
      }

      public void undeploy(Node node) throws Exception
      {
         deployments--;
         nodes.remove(node.getAttributes().getNamedItem(NAME_ATTR).getNodeValue());
      }

      protected Element getRootElement(URL url)
      {
         return element;
      }

      public int getWeight()
      {
         return 0;
      }
   }
}
