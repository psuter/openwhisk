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
package whisk.core.controller.v2

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.http._
import akka.http.scaladsl._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.FromStringUnmarshaller
import akka.stream.Materializer

import whisk.common.TransactionId
import whisk.core.entity.WhiskActivation

import spray.json._

import whisk.core.entitlement.Collection
import whisk.core.entity.EntityName
import whisk.core.entity.types.ActivationStore
import whisk.core.entity.Namespace

trait Activations extends Authentication {

    protected lazy val collection = Collection(Collection.ACTIVATIONS)

    protected val activationStore: ActivationStore

    def activationRoutes = (
        activationListRoute
    )

    private implicit val entityNameUnmarshaller = new FromStringUnmarshaller[EntityName] {
        def apply(value: String)(implicit ec: ExecutionContext, materializer: Materializer) : Future[EntityName] = {
            // FIXME; look into that. Seems reasonable, but check if escaping, encoding, bla bla apply.
            Future.successful(EntityName(value))
        }
    }

    val activationListRoute =
        path("api" / "v2" / "namespace" / "_" / "activations") {
            get {
                authenticateBasicAsync("whisk rest service", validateCredentials) { whiskAuth =>
                    parameters('skip ? 0, 'limit ? collection.listLimit, 'count ? false, 'docs ? false, 'name.as[EntityName].?) {
                        (skip, limit, count, docs, name) =>

                        val cappedLimit = if(limit <= 0 || limit > 200) 200 else limit

                        val activations = WhiskActivation.listCollectionInNamespace(activationStore, Namespace("_"), skip, cappedLimit, docs, None, None)

                        // FIXME handle case where action name is given

                        //complete(JsObject("error" -> JsBoolean(true)))

                        val futureJsArray = activations.map { l =>
                            val jsL = if(docs) {
                                l.right.get.map { _.toExtendedJson }
                            } else {
                                l.left.get
                            }
                            JsArray(jsL : _*)
                        }

                        complete(futureJsArray)
                    }
                }
                    /*

                    parameter('skip ? 0, 'limit ? collection.listLimit, 'count ? false, 'docs ? false, 'name.as[EntityName]?, 'since.as[Instant]?, 'upto.as[Instant]?) {
            (skip, limit, count, docs, name, since, upto) =>
                // regardless of limit, cap at 200 records, client must paginate
                val cappedLimit = if (limit == 0 || limit > 200) 200 else limit
                val activations = name match {
                    case Some(action) =>
                        WhiskActivation.listCollectionByName(activationStore, namespace, action, skip, cappedLimit, docs, since, upto)
                    case None =>
                        WhiskActivation.listCollectionInNamespace(activationStore, namespace, skip, cappedLimit, docs, since, upto)
                }

                listEntities {
                    activations map {
                        l => if (docs) l.right.get map { _.toExtendedJson } else l.left.get
                    }
                }
        }

                }*/
            }
        }
}
