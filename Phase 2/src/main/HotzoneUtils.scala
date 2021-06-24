package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    // check if the inputs strings are empty, if true then return false
    if (queryRectangle == null || queryRectangle.isEmpty() || pointString == null || pointString.isEmpty()) {
      return false
    }

    val rect = queryRectangle.split(",").map(_.toDouble)
    val point = pointString.split(",").map(_.toDouble)

    if (rect(0) <= point(0) && rect(2) >= point(0) && rect(1) <= point(1) && rect(3) >= point(1)) {
      return true
    }
    else if (rect(2) <= point(0) && rect(0) >= point(0) && rect(3) <= point(1) && rect(1) >= point(1)) {
      return true
    }
    else {
      return false
    }
  }

  // YOU NEED TO CHANGE THIS PART

}
