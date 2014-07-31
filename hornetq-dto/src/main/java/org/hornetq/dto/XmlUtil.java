/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.dto;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.InputStream;

public class XmlUtil
{

   public static <T> T decode(Class<T> clazz, File configuration) throws Exception
   {
      JAXBContext jaxbContext = JAXBContext.newInstance("org.hornetq.dto");

      Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
      SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
      sf.setFeature("http://apache.org/xml/features/validation/schema-full-checking", false);
      InputStream xsdStream = XmlUtil.class.getClassLoader().getResourceAsStream("org/hornetq/dto/hornetq.xsd");
      StreamSource xsdSource = new StreamSource(xsdStream);
      Schema schema = sf.newSchema(xsdSource);
      unmarshaller.setSchema(schema);

      return clazz.cast(unmarshaller.unmarshal(configuration));
   }

}
