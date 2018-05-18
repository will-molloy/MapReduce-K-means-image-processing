package mapreduce.kmeans.util

import java.awt.image.BufferedImage

import io.humble.video._
import io.humble.video.awt.MediaPictureConverterFactory

import scala.collection.mutable.ListBuffer

object VideoDecoder {

  def toImageList(inVideoFilePath: String): List[BufferedImage] = {
    val demuxer = Demuxer.make()
    demuxer.open(inVideoFilePath, null, false, true, null, null)

    var streamStartTime = Global.NO_PTS
    var videoStreamId = -1
    for (i <- 0 until demuxer.getNumStreams; if videoStreamId == -1) {
      val stream = demuxer.getStream(i)
      streamStartTime = stream.getStartTime
      val decoder = stream.getDecoder
      if (decoder != null && decoder.getCodecType == MediaDescriptor.Type.MEDIA_VIDEO) {
        videoStreamId = i
      }
    }
    if (videoStreamId == -1) {
      throw new RuntimeException("Could not find video stream in container: %s" format inVideoFilePath)
    }
    val decoder = demuxer.getStream(videoStreamId).getDecoder
    decoder.open(null, null)

    val images = new ListBuffer[BufferedImage]
    val picture = MediaPicture.make(decoder.getWidth, decoder.getHeight, decoder.getPixelFormat)
    val converter = MediaPictureConverterFactory.createConverter(MediaPictureConverterFactory.HUMBLE_BGR_24, picture)

    val packet = MediaPacket.make()
    while (demuxer.read(packet) >= 0) {
      if (packet.getStreamIndex == videoStreamId) {
        var offset = 0
        var bytesRead = 0
        do {
          bytesRead += decoder.decode(picture, packet, offset)
          if (picture.isComplete) {
            images += converter.toImage(null, picture)
          }
          offset += bytesRead
        } while (offset < packet.getSize)
      }
    }
    demuxer.close()
    images.toList
  }

}
