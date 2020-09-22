package bdv.server;

import azgracompress.cache.ICacheFile;
import azgracompress.cache.QuantizationCacheManager;
import azgracompress.compression.CompressionOptions;
import azgracompress.compression.ImageCompressor;
import azgracompress.data.V3i;
import azgracompress.io.FileInputData;
import azgracompress.io.FlatBufferInputData;
import azgracompress.io.InputData;
import azgracompress.io.MemoryOutputStream;
import bdv.BigDataViewer;
import bdv.cache.CacheHints;
import bdv.cache.LoadingStrategy;
import bdv.img.cache.VolatileCell;
import bdv.img.cache.VolatileGlobalCellCache;
import bdv.img.cache.VolatileGlobalCellCache.Key;
import bdv.img.cache.VolatileGlobalCellCache.VolatileCellLoader;
import bdv.img.hdf5.Hdf5ImageLoader;
import bdv.img.hdf5.Hdf5VolatileShortArrayLoader;
import bdv.img.remote.AffineTransform3DJsonSerializer;
import bdv.img.remote.RemoteImageLoader;
import bdv.img.remote.RemoteImageLoaderMetaData;
import bdv.spimdata.SequenceDescriptionMinimal;
import bdv.spimdata.SpimDataMinimal;
import bdv.spimdata.XmlIoSpimDataMinimal;
import bdv.util.ThumbnailGenerator;
import com.google.gson.GsonBuilder;
import mpicbg.spim.data.SpimDataException;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileShortArray;
import net.imglib2.realtransform.AffineTransform3D;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.log.Log;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Stack;

public class CellHandler extends ContextHandler {
    private long transferedDataSize = 0;

    private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger(CellHandler.class);

    private int counter = 0;
    private final VolatileGlobalCellCache cache;

    private final Hdf5VolatileShortArrayLoader loader;

    private final CacheHints cacheHints;

    /**
     * Full path of the dataset xml file this {@link CellHandler} is serving.
     */
    private final String xmlFilename;

    /**
     * Full path of the dataset xml file this {@link CellHandler} is serving,
     * without the ".xml" suffix.
     */
    private final String baseFilename;

    private final String dataSetURL;

    /**
     * Cached dataset XML to be send to and opened by {@link BigDataViewer}
     * clients.
     */
    private final String datasetXmlString;

    /**
     * Cached JSON representation of the {@link RemoteImageLoaderMetaData} to be
     * send to clients.
     */
    private final String metadataJson;

    /**
     * Cached dataset.settings XML to be send to clients. May be null if no
     * settings file exists for the dataset.
     */
    private final String settingsXmlString;

    /**
     * Full path to thumbnail png.
     */
    private final String thumbnailFilename;

    /**
     * Compression stuff.
     */
    private final CompressionOptions compressionParams;
    private ImageCompressor compressor = null;
    private Stack<MemoryOutputStream> cachedBuffers = null;
    private ICacheFile compressionCacheFile = null;
    private final int INITIAL_BUFFER_SIZE = 2048;

    public CellHandler(final String baseUrl, final String xmlFilename, final String datasetName, final String thumbnailsDirectory,
                       final CompressionOptions compressionParams) throws SpimDataException, IOException {

        final XmlIoSpimDataMinimal io = new XmlIoSpimDataMinimal();
        final SpimDataMinimal spimData = io.load(xmlFilename);
        final SequenceDescriptionMinimal seq = spimData.getSequenceDescription();
        final Hdf5ImageLoader imgLoader = (Hdf5ImageLoader) seq.getImgLoader();
        this.compressionParams = compressionParams;

        cache = imgLoader.getCacheControl();
        loader = imgLoader.getShortArrayLoader();
        cacheHints = new CacheHints(LoadingStrategy.BLOCKING, 0, false);

        // dataSetURL property is used for providing the XML file by replace
        // SequenceDescription>ImageLoader>baseUrl
        this.xmlFilename = xmlFilename;
        baseFilename = xmlFilename.endsWith(".xml") ? xmlFilename.substring(0, xmlFilename.length() - ".xml".length()) : xmlFilename;
        dataSetURL = baseUrl;

        datasetXmlString = buildRemoteDatasetXML(io, spimData, baseUrl);
        metadataJson = buildMetadataJsonString(imgLoader, seq);
        settingsXmlString = buildSettingsXML(baseFilename);
        thumbnailFilename = createThumbnail(spimData, baseFilename, datasetName, thumbnailsDirectory);


        initializeCompression();
    }

    private void initializeCompression() {
        if (compressionParams == null)
            return;
        this.compressionParams.setInputDataInfo(new FileInputData(this.baseFilename));
        QuantizationCacheManager qcm = new QuantizationCacheManager(compressionParams.getCodebookCacheFolder());
        this.compressionCacheFile = qcm.loadCacheFile(compressionParams);
        if (compressionCacheFile == null) {
            LOG.warn("CellHandler: Didn't find cached codebook for " + this.baseFilename);
            return;
        }
        LOG.info(String.format("CellHandler: Loaded cached codebook file. '%s' for %s", compressionCacheFile, this.baseFilename));

        final int initialCompressionCacheSize = 10;

        compressor = new ImageCompressor(compressionParams, compressionCacheFile);
        cachedBuffers = new Stack<>();
        for (int i = 0; i < initialCompressionCacheSize; i++) {
            cachedBuffers.push(new MemoryOutputStream(INITIAL_BUFFER_SIZE));
        }
    }


    private synchronized MemoryOutputStream getCachedCompressionBuffer() {
        if (!cachedBuffers.empty()) {
            return cachedBuffers.pop();
        } else {
            return new MemoryOutputStream(INITIAL_BUFFER_SIZE);
        }
    }

    private synchronized void returnBufferForReuse(MemoryOutputStream buffer) {
        buffer.reset();
        cachedBuffers.push(buffer);
    }

    private short[] getCachedVolatileCellData(final String[] parts, final int[] cellDims) {
        final int index = Integer.parseInt(parts[1]);
        final int timepoint = Integer.parseInt(parts[2]);
        final int setup = Integer.parseInt(parts[3]);
        final int level = Integer.parseInt(parts[4]);
        final Key key = new VolatileGlobalCellCache.Key(timepoint, setup, level, index);
        VolatileCell<?> cell = cache.getLoadingVolatileCache().getIfPresent(key, cacheHints);

        final long[] cellMin = new long[]{
                Long.parseLong(parts[8]),
                Long.parseLong(parts[9]),
                Long.parseLong(parts[10])};
        if (cell == null) {
            cell = cache.getLoadingVolatileCache().get(key,
                                                       cacheHints,
                                                       new VolatileCellLoader<>(loader, timepoint, setup, level, cellDims, cellMin));
        }
        //noinspection unchecked
        return ((VolatileCell<VolatileShortArray>) cell).getData().getCurrentStorageArray();
    }

    private FlatBufferInputData createInputDataObject(final short[] data, final int[] cellDims) {
        return new FlatBufferInputData(data, new V3i(cellDims[0], cellDims[1], cellDims[2]), InputData.PixelType.Gray16, this.baseFilename);
    }

    @Override
    public void doHandle(final String target,
                         final Request baseRequest,
                         final HttpServletRequest request,
                         final HttpServletResponse response) throws IOException {
        if (target.equals("/settings")) {
            if (settingsXmlString != null)
                respondWithString(baseRequest, response, "application/xml", settingsXmlString);
            return;
        }

        if (target.equals("/png")) {
            provideThumbnail(baseRequest, response);
            return;
        }

        final String cellString = request.getParameter("p");

        if (cellString == null) {
            respondWithString(baseRequest, response, "application/xml", datasetXmlString);
            return;
        }
        final String[] parts = cellString.split("/");
        if (parts[0].equals("cell")) {

            final int[] cellDims = new int[]{
                    Integer.parseInt(parts[5]),
                    Integer.parseInt(parts[6]),
                    Integer.parseInt(parts[7])};

            final short[] data = getCachedVolatileCellData(parts, cellDims);

            final OutputStream responseStream = response.getOutputStream();

            final byte[] buf = new byte[2 * data.length];
            for (int i = 0, j = 0; i < data.length; i++) {
                final short s = data[i];
                buf[j++] = (byte) ((s >> 8) & 0xff);
                buf[j++] = (byte) (s & 0xff);
            }
            response.setContentLength(buf.length);
            responseStream.write(buf);

            responseStream.close();

            response.setContentType("application/octet-stream");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);

        } else if (parts[0].equals("cell_qcmp")) {
            final int[] cellDims = new int[]{Integer.parseInt(parts[5]), Integer.parseInt(parts[6]), Integer.parseInt(parts[7])};

            final short[] data = getCachedVolatileCellData(parts, cellDims);
            assert (compressor != null);


            final FlatBufferInputData inputData = createInputDataObject(data, cellDims);

            MemoryOutputStream cellCompressionStream = getCachedCompressionBuffer();
            final int compressedContentLength = compressor.streamCompressChunk(cellCompressionStream, inputData);

            response.setContentLength(compressedContentLength);
            try (OutputStream responseStream = response.getOutputStream()) {
                responseStream.write(cellCompressionStream.getBuffer(), 0, cellCompressionStream.getCurrentBufferLength());
            }


            assert (cellCompressionStream.getCurrentBufferLength() == compressedContentLength) :
                    "compressor.streamCompressChunk() is not equal to cachedCompressionStream.getCurrentBufferLength()";

            if (cellCompressionStream.getCurrentBufferLength() != compressedContentLength) {
                System.err.printf("stream size\t%d\nreported size\t%d\n\n",
                                  cellCompressionStream.getCurrentBufferLength(),
                                  compressedContentLength);
            }


            response.setContentType("application/octet-stream");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            returnBufferForReuse(cellCompressionStream);
        } else if (parts[0].equals("init")) {
            respondWithString(baseRequest, response, "application/json", metadataJson);
        } else if (parts[0].equals("init_qcmp")) {
            if (compressor == null) {
                LOG.info("QCMP initialization request was refused, QCMP compression is not enabled.");
                respondWithString(baseRequest, response,
                                  "text/plain", "QCMP Compression wasn't enabled on BigDataViewer server.",
                                  HttpServletResponse.SC_BAD_REQUEST);
                return;
            }

            try (DataOutputStream dos = new DataOutputStream(response.getOutputStream())) {
                compressionCacheFile.writeToStream(dos);
            }
            response.getOutputStream().close();

            response.setContentType("application/octet-stream");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
        }
    }

    private void provideThumbnail(final Request baseRequest, final HttpServletResponse response) throws IOException {
        final Path path = Paths.get(thumbnailFilename);
        if (Files.exists(path)) {
            final byte[] imageData = Files.readAllBytes(path);
            if (imageData != null) {
                response.setContentType("image/png");
                response.setContentLength(imageData.length);
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);

                final OutputStream os = response.getOutputStream();
                os.write(imageData);
                os.close();
            }
        }
    }

    public String getXmlFile() {
        return xmlFilename;
    }

    public String getDataSetURL() {
        return dataSetURL;
    }

    public String getThumbnailUrl() {
        return dataSetURL + "png";
    }

    public String getDescription() {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a JSON representation of the {@link RemoteImageLoaderMetaData}
     * (image sizes and resolutions) provided by the given
     * {@link Hdf5ImageLoader}.
     */
    private static String buildMetadataJsonString(final Hdf5ImageLoader imgLoader, final SequenceDescriptionMinimal seq) {
        final RemoteImageLoaderMetaData metadata = new RemoteImageLoaderMetaData(imgLoader, seq);
        final GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(AffineTransform3D.class, new AffineTransform3DJsonSerializer());
        gsonBuilder.enableComplexMapKeySerialization();
        return gsonBuilder.create().toJson(metadata);
    }

    /**
     * Create a modified dataset XML by replacing the ImageLoader with an
     * {@link RemoteImageLoader} pointing to the data we are serving.
     */
    private static String buildRemoteDatasetXML(final XmlIoSpimDataMinimal io,
                                                final SpimDataMinimal spimData,
                                                final String baseUrl) throws IOException, SpimDataException {
        final SpimDataMinimal s = new SpimDataMinimal(spimData, new RemoteImageLoader(baseUrl, false));
        final Document doc = new Document(io.toXml(s, s.getBasePath()));
        final XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
        final StringWriter sw = new StringWriter();
        xout.output(doc, sw);
        return sw.toString();
    }

    /**
     * Read {@code baseFilename.settings.xml} into a string if it exists.
     *
     * @return contents of {@code baseFilename.settings.xml} or {@code null} if
     * that file couldn't be read.
     */
    private static String buildSettingsXML(final String baseFilename) {
        final String settings = baseFilename + ".settings.xml";
        if (new File(settings).exists()) {
            try {
                final SAXBuilder sax = new SAXBuilder();
                final Document doc = sax.build(settings);
                final XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
                final StringWriter sw = new StringWriter();
                xout.output(doc, sw);
                return sw.toString();
            } catch (JDOMException | IOException e) {
                LOG.warn("Could not read settings file \"" + settings + "\"");
                LOG.warn(e.getMessage());
            }
        }
        return null;
    }

    /**
     * Create PNG thumbnail file named "{@code <baseFilename>.png}".
     */
    private static String createThumbnail(final SpimDataMinimal spimData,
                                          final String baseFilename,
                                          final String datasetName,
                                          final String thumbnailsDirectory) {
        final String thumbnailFileName = thumbnailsDirectory + "/" + datasetName + ".png";
        final File thumbnailFile = new File(thumbnailFileName);
        if (!thumbnailFile.isFile()) // do not recreate thumbnail if it already exists
        {
            final BufferedImage bi = ThumbnailGenerator.makeThumbnail(spimData,
                                                                      baseFilename,
                                                                      Constants.THUMBNAIL_WIDTH,
                                                                      Constants.THUMBNAIL_HEIGHT);
            try {
                ImageIO.write(bi, "png", thumbnailFile);
            } catch (final IOException e) {
                LOG.warn("Could not create thumbnail png for dataset \"" + baseFilename + "\"");
                LOG.warn(e.getMessage());
            }
        }
        return thumbnailFileName;
    }

    /**
     * Handle request by sending a UTF-8 string.
     */
    private static void respondWithString(final Request baseRequest,
                                          final HttpServletResponse response,
                                          final String contentType,
                                          final String string) throws IOException {
        respondWithString(baseRequest, response, contentType, string, HttpServletResponse.SC_OK);
    }

    /**
     * Handle request by sending a UTF-8 string.
     */
    private static void respondWithString(final Request baseRequest,
                                          final HttpServletResponse response,
                                          final String contentType,
                                          final String string,
                                          final int httpStatus) throws IOException {
        response.setContentType(contentType);
        response.setCharacterEncoding("UTF-8");
        response.setStatus(httpStatus);
        baseRequest.setHandled(true);

        final PrintWriter ow = response.getWriter();
        ow.write(string);
        ow.close();
    }
}
