/*
 * Copyright 2013 Tomo Simeonov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.simeont.marklogicconnector.xml.tests

import scala.xml.XML
import scala.xml.Node
import scala.xml.Elem
import java.lang.Double
import java.lang.Integer
import org.scalatest.FunSuite
import com.gigaspaces.document.SpaceDocument
import org.simeont.marklogicconnector.xml._

class XStreamMarshallerTest extends FunSuite {

  val marshaller = new BasicMarshaller

  val docType = "test"

  test("should marshall empty document") {
    val spaceDocument = new SpaceDocument(docType)

    val string = marshaller.toXML(spaceDocument)
    assert(string === "<test xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"></test>")
  }

   test("should marshall document with null value") {
    val spaceDocument = new SpaceDocument(docType)
    spaceDocument.setProperty("test2", null)
    val string = marshaller.toXML(spaceDocument)
    assert(string === "<test xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"><test2 xs:type=\"object\"></test2></test>")
  }

  test("should marshall integer in document") {

    val spaceDocument = new SpaceDocument(docType)
    spaceDocument.setProperty("jint", new Integer(1))

    val string = marshaller.toXML(spaceDocument)
    assert(string === "<test xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">" +
    		"<jint xs:type=\"java.lang.Integer\">1</jint></test>")
  }

  test("should marshall double in document") {

    val spaceDocument = new SpaceDocument(docType)
    spaceDocument.setProperty("double", new Double(1))

    val string = marshaller.toXML(spaceDocument)
    assert(string === "<test xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">" +
    		"<double xs:type=\"java.lang.Double\">1.0</double></test>")
  }

  test("should marshall java list") {
    var list: java.util.LinkedList[Integer] = new java.util.LinkedList[Integer]()
    list.add(1)
    list.add(2)
    val spaceDocument = new SpaceDocument("list-test")
    spaceDocument.setProperty("list", list)

    val string = marshaller.toXML(spaceDocument)
    assert(string === "<list-test xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">" +
    		"<list xs:type=\"object\"><linked-list>\n  <int>1</int>\n  <int>2</int>\n</linked-list></list></list-test>")
  }

  test("should marshall document with other document inside") {
    val spaceDocumentOut = new SpaceDocument("outer")
    val spaceDocumentIn = new SpaceDocument("inner")

    spaceDocumentIn.setProperty("innerInt", 1)
    spaceDocumentOut.setProperty("document", spaceDocumentIn)

    val string = marshaller.toXML(spaceDocumentOut)
    assert(string === "<outer xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">" +
    		"<document xs:type=\"SpaceDocument\"><inner><innerInt xs:type=\"java.lang.Integer\">1</innerInt>" +
    		"</inner></document></outer>")
  }

  test("should unmarshall empty spacedocument") {
    val spaceDocument = new SpaceDocument(docType)
    val uSpaceDocument = marshaller.fromXML(marshaller.toXML(spaceDocument))

    assert(uSpaceDocument === spaceDocument)
  }

  test("should unmarshall non-empty spacedocument") {
    val spaceDocument = new SpaceDocument(docType)

    spaceDocument.setProperty("jint", new Integer(1))

    val uSpaceDocument = marshaller.fromXML(marshaller.toXML(spaceDocument))
    assert(uSpaceDocument === spaceDocument)
  }

  test("should unmarshall scala list inside spacedocument") {
    val list: List[Integer] = List(1, 2, 3)
    val spaceDocument = new SpaceDocument("list-test")

    spaceDocument.setProperty("list", list)

    val uSpaceDocument = marshaller.fromXML(marshaller.toXML(spaceDocument))
    assert(uSpaceDocument === spaceDocument)
  }

  test("should unmarshall  spacedocument with property other spacedocument") {
    val spaceDocumentOut = new SpaceDocument("outer")
    val spaceDocumentIn = new SpaceDocument("inner")

    spaceDocumentIn.setProperty("innerInt", 1)
    spaceDocumentOut.setProperty("document", spaceDocumentIn)

    val uSpaceDocument = marshaller.fromXML(marshaller.toXML(spaceDocumentOut))
    assert(uSpaceDocument === spaceDocumentOut)
  }
}
