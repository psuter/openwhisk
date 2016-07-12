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

package actionContainers

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import spray.json._

import ActionContainer.withContainer

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class PhpActionContainerTests extends FlatSpec
    with Matchers
    with BeforeAndAfter {

    // Helpers specific to phpAction
    def withPhpContainer(code: ActionContainer => Unit) = withContainer("whisk/phpaction")(code)
    def initPayload(code: String) = JsObject(
        "value" -> JsObject(
            "name" -> JsString("somePhpAction"),
            "code" -> JsString(code)))
    def runPayload(args: JsValue) = JsObject("value" -> args)

    behavior of "whisk/phpaction"

    it should "support valid flows" in {
        val (out, err) = withPhpContainer { c =>
            val code = """
                | <?php
                | function main($args) {
                |     if (array_key_exists("name", $args)) {
                |         $greeting = "Hello " . $args["name"] . "!";
                |     } else {
                |         $greeting = "Hello stranger!";
                |     }
                |
                |     return array(
                |        "greeting" => $greeting
                |     );
                | }
                | ?>
            """.stripMargin

            val (initCode, _) = c.init(initPayload(code))

            val ins = List(
                JsObject("name" -> JsString("John Doe")),
                JsObject())

            val outs = List(
                JsObject("greeting" -> JsString("Hello John Doe!")),
                JsObject("greeting" -> JsString("Hello stranger!"))
            )

            for ((args,expected) <- ins.zip(outs)) {
                val (runCode, runRes) = c.run(runPayload(args))
                runCode should be(200)
                runRes should be(Some(expected))
            }
        }
        err.trim shouldBe empty
    }
}
