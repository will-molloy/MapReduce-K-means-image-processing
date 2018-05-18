package mapreduce.kmeans.util

import java.awt.image.BufferedImage

import io.humble.video._
import io.humble.video.awt.MediaPictureConverterFactory

object VideoEncoder {

  /**
    * DOESNT WORK
    * java: malloc.c:4023: _int_malloc: Assertion `(unsigned long) (size) >= (unsigned long) (nb)' failed.
    **/
  @Deprecated
  def writeToVideoFile(images: List[BufferedImage], outVideoFilePath: String): Unit = {
    //    val image = images(0)
    //    val recorder = new FFmpegFrameRecorder("out.mp4", image.getWidth(), image.getHeight())
    //    recorder.setVideoCodec(13)
    //    recorder.setFormat("mp4")
    //    recorder.setFormat("PIX_FMT_YUV420P")
    //    recorder.setFrameRate(30)
    //    recorder.setVideoBitrate(10 * 1024 * 1024)
    //    recorder.start()
    //
    //    val converter = new Java2DFrameConverter
    //    images.foreach(image => {
    //      recorder.record(converter.convert(image))
    //    })
    //
    //    recorder.stop()

    val muxer = Muxer.make(outVideoFilePath, null, "avi")
    val format = muxer.getFormat
    val codec = Codec.findEncodingCodec(format.getDefaultVideoCodecId)
    val encoder = Encoder.make(codec)

    val image = images(0)
    encoder.setWidth(image.getWidth)
    encoder.setHeight(image.getHeight)
    encoder.setPixelFormat(PixelFormat.Type.PIX_FMT_YUV420P)
    encoder.setTimeBase(Rational.make(1, 30))

    encoder.open(null, null)
    muxer.addNewStream(encoder)
    muxer.open(null, null)

    val picture = MediaPicture.make(encoder.getWidth, encoder.getHeight, encoder.getPixelFormat)
    picture.setTimeBase(encoder.getTimeBase)

    val packet = MediaPacket.make()
    for (i <- images.indices) {
      val converter = MediaPictureConverterFactory.createConverter(image, picture)
      converter.toPicture(picture, images(i), i)

      do {
        encoder.encode(packet, picture)
        if (packet.isComplete) {
          muxer.write(packet, false)
        }
      } while (packet.isComplete)
    }
    // flush
    do {
      encoder.encode(packet, null)
      if (packet.isComplete) muxer.write(packet, false)
    } while (packet.isComplete)
    muxer.close()
  }

}