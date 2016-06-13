package kafka.utils

import scala.annotation.StaticAnnotation

/**
 * @author zhaori
 */

class threadsafe extends StaticAnnotation

class nonthreadsafe extends StaticAnnotation

class immutable extends StaticAnnotation