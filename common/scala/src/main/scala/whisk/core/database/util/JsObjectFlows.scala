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
package whisk.core.database.util

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.ByteString

import scala.collection.immutable.Iterable

import spray.json._

object JsObjectFlows {
    /** Turns a streaming source producing bytes encoding a JSON array of objects
     *  into a source streaming said objects.
     *
     *  Caveats:
     *    - will happily gobble up memory if individual objects are large
     */
    def byteStringParser: Flow[ByteString,JsObject,NotUsed] = {
        Flow[ByteString].via(new JsObjectParserStage()).named("jsObjectParser")
    }
}

case class BufferingRequiredException() extends Exception

/** An extension of spray-json ParserInput trait to support reading bytes from
 *  a sliding buffer. There are three main differences when compared to, for
 *  instance, spray.json.ByteArrayParserInput:
 *
 *    1) the next character may not be available because the end of the buffer
 *    has been reached. In these situations, the class throws a BufferingRequiredException
 *
 *    2) parts of the characters that have been processed can be evicted to free
 *    up memory
 *
 *    3) the stream of characters can be reset back to an earlier position
 */
class SlidingBufferParserInput() extends ParserInput.DefaultParserInput {
    private var buffer = ByteString.empty
    private var hasCompleted = false

    private val UTF8 = java.nio.charset.Charset.forName("UTF-8")

    // Number of bytes that have already been evicted.
    private var evicted : Int = 0

    // Position in the buffer we still have, as opposed to cursor which
    // returns the position expressed as total number of bytes read.
    @inline
    def bufferCursor = cursor - evicted

    // Indicates that no more bytes will be appended
    def complete() : Unit = { hasCompleted = true }

    // Appends bytes to the right of the byte string
    def append(byteString: ByteString) : Unit = {
        require(!hasCompleted)
        buffer ++= byteString
    }

    // Deletes to the left of the buffer.
    def evict(n: Int) : Unit = {
        require(n <= bufferCursor) // you shouldn't evict bytes you haven't read
        evicted += n
        buffer = buffer.drop(n)
    }

    // Eviction expressed in terms of all bytes ever read.
    def dropAllBefore(cursorPos: Int) : Unit = {
        evict(cursorPos - evicted)
    }

    // Resets the byte stream to an earlier position.
    def reset(cursorPos: Int) : Char = {
        if(cursorPos - evicted < 0) throw new UnsupportedOperationException("cannot reset cursor beyond eviction point")
        _cursor = cursorPos
        // In the event we were interrupted while parsing a UTF-8 sequence
        byteBuffer.clear()
        charBuffer.clear()
        (buffer(bufferCursor) & 0xFF).toChar
    }

    // Reads the next 7-bit ASCII char
    def nextChar() = {
        if(bufferCursor == buffer.length - 1 && !hasCompleted) throw BufferingRequiredException()
        _cursor += 1
        if(bufferCursor < buffer.length) {
            (buffer(bufferCursor) & 0xFF).toChar
        } else {
            '\uFFFF' // ParserInput.EOI, which is private
        }
    }

    // Ported from spray.json.JsonParser.ByteArrayBasedParserInput
    // Couldn't find a better way of reusing the code, unfortunately, as it does
    // not build on any level of abstraction.
    //
    // The method consumes bytes from the buffer to produce either ASCII characters
    // or to properly combine byte sequences into one or two Chars.
    // There is a possibility that the buffer runs out of bytes in the middle of
    // a sequence. This is not a major issue as long as the consuming parser
    // always resets the cursor to a position that is not in the middle of a sequence.
    private val byteBuffer = java.nio.ByteBuffer.allocate(4)
    private val charBuffer = java.nio.CharBuffer.allocate(2)
    private val decoder    = UTF8.newDecoder()
    def nextUtf8Char() = {
        @scala.annotation.tailrec
        def decode(byte: Byte, n: Int) : Char = {
            byteBuffer.put(byte)
            if(n > 0) {
                val c = nextChar() // this may throw if bytes are not yet available
                if(c == '\uFFFF') { // the stream ended with an incomplete UTF sequence
                    '\uFFFD' // universal UTF-8 replacement character
                } else {
                    decode(buffer(bufferCursor), n-1)
                }
            } else {
                byteBuffer.flip()
                val dr = decoder.decode(byteBuffer, charBuffer, false)
                charBuffer.flip()
                val r = if(dr.isUnderflow() && charBuffer.hasRemaining()) {
                    charBuffer.get()
                } else {
                    '\uFFFD'
                }
                byteBuffer.clear()
                if(!charBuffer.hasRemaining()) charBuffer.clear()
                r
            }
        }

        if(charBuffer.position() > 0) {
            val r = charBuffer.get()
            charBuffer.clear()
            r
        } else {
            val c = nextChar() // may throw
            if(c == '\uFFFF') {
                c
            } else {
                val b = buffer(bufferCursor)
                if(b >= 0) b.toChar // was ASCII all along
                else if ((b & 0xE0) == 0xC0) decode(b, 1) // 2 byte sequence
                else if ((b & 0xF0) == 0xE0) decode(b, 2) // 3 byte sequence
                else if ((b & 0xF8) == 0xF0) decode(b, 3) // 4 byte sequence
                else '\uFFFD'
            }
        }
    }

    // Doesn't actually seem to be used.
    def length = throw new UnsupportedOperationException("cannot compute length of ByteString stream")

    def sliceString(start: Int, end: Int) = {
        if(start - evicted < 0 || end - evicted < 0) throw new UnsupportedOperationException("requesting slice overlapping with evicted stream")
        buffer.slice(start - evicted, end - evicted).decodeString(UTF8.name())
    }

    def sliceCharArray(start: Int, end: Int) = {
        if(start - evicted < 0 || end - evicted < 0) throw new UnsupportedOperationException("requesting slice overlapping with evicted stream")
        UTF8.decode(buffer.slice(start - evicted, end - evicted).toByteBuffer).array()
    }
}

/** An extension of spray.json.JsonParser with two main features:
 *
 *    1) exposes internal methods to support parsing whitespace, JSON objects
 *    (as opposed to general JsValues), throw proper parsing errors, etc.
 *
 *    2) supports cursor reset operations to retry parsing when more data is
 *    made available.
 */
class SlidingBufferJsonParser(val input: SlidingBufferParserInput) extends JsonParser(input) {
    // Helper method to make exceptions in reflection code look like regular ones
    private def surface[T](code: =>T) : T = try {
        code
    } catch {
        case ite : java.lang.reflect.InvocationTargetException =>
            throw ite.getTargetException()
    }

    private val _wsM = classOf[JsonParser].getDeclaredMethod("ws")
    private val _requireM = classOf[JsonParser].getDeclaredMethod("require", classOf[Char])
    private val _advanceM = classOf[JsonParser].getDeclaredMethod("advance")
    private val _objectM = classOf[JsonParser].getDeclaredMethod("object")
    private val _jsValueF = classOf[JsonParser].getDeclaredField("jsValue")
    private val _cursorCharF = classOf[JsonParser].getDeclaredField("cursorChar")

    _wsM.setAccessible(true)
    _requireM.setAccessible(true)
    _advanceM.setAccessible(true)
    _objectM.setAccessible(true)
    _jsValueF.setAccessible(true)
    _cursorCharF.setAccessible(true)

    // Advances until _cursorChar() is on a non-ws character.
    def _ws() : Unit = surface { _wsM.invoke(this) }
    // Checks whether _cursorChar() == char and advances if yes.
    def _require(char: Char) : Unit =  surface {_requireM.invoke(this, char.asInstanceOf[Object]) }
    // Advances. Always returns true
    def _advance() : Boolean = surface { _advanceM.invoke(this).asInstanceOf[java.lang.Boolean] }
    // Attempts to parse a JsObject, and advances as it does. If successful, the
    // result is made available through _jsValue().
    def _object() : Unit = surface { _objectM.invoke(this) }
    // Accesses the last properly parsed JsValue.
    def _jsValue() : JsValue = _jsValueF.get(this).asInstanceOf[JsValue]
    // The character currently under the cursor.
    def _cursorChar() : Char = _cursorCharF.get(this).asInstanceOf[java.lang.Character]
    // Cursor position (total number of bytes read)
    def cursorPosition() : Int = input.cursor
    // Resets the cursor
    def reset(cursorPosition: Int) = {
        val c = input.reset(cursorPosition)
        _cursorCharF.setChar(this, c)
    }
}

/** A flow operator doing on-the-fly parsing of a JSON array of JSON objects.
 *  The operator mimicks the internal logic of spray.json.JsonParser, and throws
 *  similar exceptions. It does not attempt to recover as soon as one object is
 *  invalid.
 */
class JsObjectParserStage() extends GraphStage[FlowShape[ByteString,JsObject]] {
    val in  = Inlet[ByteString]("JsObjectParser.in")
    val out = Outlet[JsObject]("JsObjectParser.out")
    override val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) with InHandler with OutHandler {

        // Laziness is important here, as the parser attempts to read a byte as
        // soon as it is constructed, while the ports may not be ready.
        lazy val parserInput = new SlidingBufferParserInput()
        lazy val parser = new SlidingBufferJsonParser(parserInput)

        override def onPush() : Unit = { parserInput.append(grab(in)); doParse() }

        override def onPull() : Unit = { doParse() }

        override def onUpstreamFinish() : Unit = parserInput.complete()

        // The pattern for this method is that most parser._ methods can fail with
        // BufferingRequiredException if the parser requested more bytes that
        // were received. The state machine implemented by doParse() must ensure
        // that it remains in a consistent state. Because it's not quite possible
        // to preserve the entirety of the JsonParser state, this means in some
        // cases rewinding.
        private val SEEKING_ARRAY_START  = 0
        private val SEEKING_OBJECT       = 1
        private val SEEKING_OBJECT_SEP   = 2
        private val FINISHED_PARSING     = 3

        private var state: Int = SEEKING_ARRAY_START

        private def doParse() : Unit = {
            var resetPoint: Option[Int] = None
            try {
                while(true) {
                    state match {
                        case SEEKING_ARRAY_START =>
                            parser._ws()
                            parser._require('[')
                            state = SEEKING_OBJECT

                        case SEEKING_OBJECT =>
                            parser._ws()
                            // Saving the cursor position in case there are not
                            // enough characters in the buffer to parse the full
                            // object.
                            resetPoint = Some(parser.cursorPosition())
                            parser._require('{')
                            parser._object()
                            val parsedObject = parser._jsValue().asJsObject
                            resetPoint = None
                            emit(out, parsedObject)
                            state = SEEKING_OBJECT_SEP
                            return // can't emit more than one

                        case SEEKING_OBJECT_SEP =>
                            parser._ws()
                            if(parser._cursorChar() == ']') {
                                parser._advance()
                                state = FINISHED_PARSING
                            } else {
                                parser._require(',')
                                state = SEEKING_OBJECT
                            }

                        case FINISHED_PARSING =>
                            parser._ws()
                            parser._require('\uFFFF')
                            completeStage()
                            return
                    }
                }
            } catch {
               case BufferingRequiredException() =>
                   resetPoint.foreach { p => parser.reset(p) }
                   pull(in)
               case t: Throwable =>
                   failStage(t)
            }
        }

        setHandlers(in, out, this)
    }
}
