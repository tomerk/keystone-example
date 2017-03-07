import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

/**
 * Created by tomerk11 on 3/7/17.
 */
package object utils {
  def readFromDisk[T](file: String): T = {
    val fis = new FileInputStream(file)
    val ois = new ObjectInputStream(fis)
    val obj = ois.readObject()
    ois.close()
    obj.asInstanceOf[T]
  }

  def writeToDisk[T](obj: T, file: String): Unit = {
    val fos = new FileOutputStream(file)
    val oos = new ObjectOutputStream(fos)

    oos.writeObject(obj)
    oos.close()
  }
}
