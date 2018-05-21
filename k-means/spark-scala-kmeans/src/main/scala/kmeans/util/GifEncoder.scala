package kmeans.util

import java.awt.image.BufferedImage
import java.io.File

import javax.imageio.stream.FileImageOutputStream
import third_party.GifSequenceWriter

object GifEncoder {

  def write(images: Seq[BufferedImage], outFileName: String): Unit = {
    val firstImage = images(0)
    val output = new FileImageOutputStream(new File(outFileName))
    val writer = new GifSequenceWriter(output, firstImage.getType, 1, false)

    images.foreach(image => {
      writer.writeToSequence(image)
    })
    writer.close()
    output.close()
  }
}
