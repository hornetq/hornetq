package org.hornetq.rest.queue.push.xml;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
@XmlRootElement(name = "digest")
public class DigestAuth extends BasicAuth
{
   private static final long serialVersionUID = 1857805131477468686L;
}
