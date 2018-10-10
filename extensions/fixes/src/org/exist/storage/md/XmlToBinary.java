/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2018 The eXist Project
 * http://exist-db.org
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.storage.md;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import javax.xml.transform.OutputKeys;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.dom.BinaryDocument;
import org.exist.dom.DocumentImpl;
import org.exist.dom.QName;
import org.exist.storage.DBBroker;
import org.exist.storage.serializers.Serializer;
import org.exist.storage.txn.Txn;
import org.exist.util.MimeTable;
import org.exist.util.MimeType;
import org.exist.util.serializer.SAXSerializer;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.fixes.Module;
import org.exist.xquery.value.BooleanValue;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

public class XmlToBinary extends BasicFunction {

  protected static final Logger logger = LogManager.getLogger(XmlToBinary.class);

  public final static FunctionSignature signatures[] = {
      new FunctionSignature(
          new QName("xml-to-binary", Module.NAMESPACE_URI, Module.PREFIX),
          "Force migrate resource type from xml to binary type.",
          new SequenceType[] {
              new FunctionParameterSequenceType(
                  "uri",
                  Type.STRING,
                  Cardinality.EXACTLY_ONE,
                  "The resource URI"
              )
          },
          new SequenceType(Type.BOOLEAN, Cardinality.EXACTLY_ONE)
      )
  };

  private final static Properties outputProps = new Properties();
  static {
    outputProps.setProperty(OutputKeys.INDENT, "yes");
  }

  public XmlToBinary(XQueryContext context, FunctionSignature signature) {
    super(context, signature);
  }

  public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
    if (!context.getSubject().hasDbaRole()) {
      throw new XPathException(
          this,
          "Permission denied, calling user '" + context.getSubject().getName() + "' must be a DBA"
      );
    }

    final XmldbURI url = XmldbURI.create(args[0].itemAt(0).getStringValue());

    MetaData md = MetaData.get();

    String uuid = md.URItoUUID(url);

    if (uuid == null) {
      return BooleanValue.FALSE;
    }

    Metas cc = md.getMetas(uuid);
    if (cc == null) {
      return BooleanValue.FALSE;
    }

    ArrayList<Pair<String, Object>> metas = new ArrayList<>();

    for (Meta meta : cc.metas()) {
      metas.add(Pair.of(meta.getKey(), meta.getValue()));
    }

    DBBroker broker = context.getBroker();

    try {

      DocumentImpl doc = md.getDocument(uuid);

      if (doc.getResourceType() == DocumentImpl.BINARY_FILE) {
        return BooleanValue.TRUE;
      }

      Path tmpFile = Files.createTempFile("fix-",".tmp");

      Date created = new Date( doc.getMetadata().getCreated() );
      Date modified = new Date( doc.getMetadata().getLastModified() );

      Serializer serializer = broker.newSerializer();
      serializer.reset();

      try (Writer out = new OutputStreamWriter(new FileOutputStream(tmpFile.toFile()), "UTF-8")) {
        SAXSerializer sax = new SAXSerializer(out, outputProps);
        serializer.setSAXHandlers(sax, sax);

        serializer.toSAX(doc);
      }

      Collection col = doc.getCollection();

      final MimeTable mimeTable = MimeTable.getInstance();

      final MimeType mimeType = mimeTable.getContentTypeFor(url.lastSegment());

      try (
          Txn txn = broker.beginTx();
          InputStream in = Files.newInputStream(tmpFile);
      ) {
        long size = Files.size(tmpFile);

        BinaryDocument resource = col.validateBinaryResource(
            txn, broker, url.lastSegment(), in, mimeType.getName(), size, created, modified
        );

        col.addBinaryResource(
            txn, broker, resource, in, mimeType.getName(), size , created, modified
        );
      } finally {
        Files.deleteIfExists(tmpFile);
      }

    } catch (Exception e) {
      throw new XPathException(this, e);
    }

    String newUuid = md.URItoUUID(url);

//    System.out.println("old uuid: "+uuid);
//    System.out.println("new uuid: "+newUuid);

    if (!uuid.equals(newUuid)) {
      md.delMetas(url);

      Metas cre = md._addMetas(url.toString(), uuid);

      for (Pair<String, Object> pair : metas) {
        cre.put(pair.getKey(), pair.getValue());
      }
    }

    return BooleanValue.TRUE;
  }
}
