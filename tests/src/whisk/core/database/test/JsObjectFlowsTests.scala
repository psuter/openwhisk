/*
 * Copyright 2015-2016 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package whisk.core.database.test

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Try, Success, Failure }

import akka.actor.ActorSystem

import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString

import spray.json._

import org.junit.runner.RunWith

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner

import whisk.core.database.util.JsObjectFlows
import javax.xml.ws.wsaddressing.W3CEndpointReference.Elements

@RunWith(classOf[JUnitRunner])
class JsObjectSourceTests extends
    FlatSpec
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures {

    private implicit val actorSystem = ActorSystem()
    private implicit val executionContext = actorSystem.dispatcher
    private implicit val materializer = ActorMaterializer()

    override def afterAll() {
        println("Shutting down materializer...")
        materializer.shutdown()
        println("Shutting down actor system...")
        actorSystem.terminate()
        Await.result(actorSystem.whenTerminated, Duration.Inf)
    }

    behavior of "JSON object streaming flow"

    val objects = Seq(
        JsObject(
            "key1" -> JsString("value1"),
            "key2" -> JsBoolean(true),
            "key3" -> JsObject("value" -> JsNumber(3)),
            "key4" -> JsArray(JsNumber(42), JsString("forty-two"))
        ),
        JsObject(),
        JsObject("hello" -> JsString("world"))
    )

    val utfObjects = Seq(
        JsObject("clé" -> JsString("数独")),
        JsObject("k" -> JsString("강남스타일"))
    )

    def strToSrc(str: String) : Source[ByteString,akka.NotUsed] = {
        Source.single(ByteString.fromString(str))
    }

    def chunkUp[T](src: Source[ByteString,T]) : Source[ByteString,T] = {
        // probably terribly inefficient way of making a byte-by-byte stream
        src.mapConcat(bs => bs.map(b => ByteString.fromArray(Array[Byte](b))))
    }

    it should "properly parse an array of objects" in {
        val repr = JsArray(objects : _*).compactPrint
        val byteSrc = strToSrc(repr)
        val resObjs = byteSrc.via(JsObjectFlows.byteStringParser).runWith(Sink.seq)
        whenReady(resObjs) { objs => assert(objs == objects) }
    }

    it should "properly parse a pretty-printed array of objects" in {
        val repr = JsArray(objects : _*).prettyPrint
        val byteSrc = strToSrc(repr)
        val resObjs = byteSrc.via(JsObjectFlows.byteStringParser).runWith(Sink.seq)
        whenReady(resObjs) { objs =>
            objs shouldEqual objects
        }
    }

    it should "properly parse a chunked up array of objects" in {
        val repr = JsArray(objects : _*).prettyPrint
        val byteSrc = chunkUp(strToSrc(repr))
        val resObjs = byteSrc.via(JsObjectFlows.byteStringParser).runWith(Sink.seq)
        whenReady(resObjs) { objs =>
            objs shouldEqual objects
        }
    }

    it should "properly parse a chunked up array with tricky strings" in {
        val array = JsArray(JsObject("}key{" -> JsString("\"value\"")))
        val repr = array.compactPrint
        val byteSrc = chunkUp(strToSrc(repr))
        val resObjs = byteSrc.via(JsObjectFlows.byteStringParser).runWith(Sink.seq)
        whenReady(resObjs) { objs =>
            objs shouldEqual array.elements
        }
    }

    it should "produce all objects then fail if the array is not closed" in {
        val repr = JsArray(objects : _*).compactPrint
        val brokenRepr = repr.substring(0, repr.length - 1) // removes the ]
        val byteSrc = strToSrc(brokenRepr)

        val lb = scala.collection.mutable.ListBuffer.empty[JsObject]

        val res = byteSrc.via(JsObjectFlows.byteStringParser).runWith(Sink.foreach { o =>
            lb += o
        })

        whenReady(res.failed) { e =>
            e shouldBe a[spray.json.JsonParser.ParsingException]
            lb.toSeq shouldEqual objects
        }
    }

    it should "properly parse UTF-8 strings" in {
        val repr = JsArray(utfObjects : _*).compactPrint
        val byteSrc = strToSrc(repr)
        val resObjs = byteSrc.via(JsObjectFlows.byteStringParser).runWith(Sink.seq)
        whenReady(resObjs) { objs =>
            objs shouldEqual utfObjects
        }
    }

    it should "properly parse chunked-up UTF-8 bytes" in {
        val repr = JsArray(utfObjects : _*).prettyPrint
        println(repr)
        val byteSrc = chunkUp(strToSrc(repr))
        val resObjs = byteSrc.via(JsObjectFlows.byteStringParser).runWith(Sink.seq)
        whenReady(resObjs) { objs =>
            objs shouldEqual utfObjects
        }
    }
}
