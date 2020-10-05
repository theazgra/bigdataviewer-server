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
import azgracompress.utilities.Stopwatch;
import bdv.BigDataViewer;
import bdv.img.cache.VolatileGlobalCellCache;
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
import net.imglib2.cache.CacheLoader;
import net.imglib2.cache.LoaderCache;
import net.imglib2.cache.ref.SoftRefLoaderCache;
import net.imglib2.img.basictypeaccess.volatiles.array.VolatileShortArray;
import net.imglib2.img.cell.Cell;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Stack;
import java.util.concurrent.ExecutionException;

public class CellHandler extends ContextHandler {
    private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger(CellHandler.class);

    /**
     * Key for a cell identified by timepoint, setup, level, and index
     * (flattened spatial coordinate).
     */
    public static class Key {
        private final int timepoint;

        private final int setup;

        private final int level;

        private final long index;

        private final String[] parts;

        /**
         * Create a Key for the specified cell. Note that {@code cellDims} and
         * {@code cellMin} are not used for {@code hashcode()/equals()}.
         *
         * @param timepoint timepoint coordinate of the cell
         * @param setup     setup coordinate of the cell
         * @param level     level coordinate of the cell
         * @param index     index of the cell (flattened spatial coordinate of the
         *                  cell)
         */
        public Key(final int timepoint, final int setup, final int level, final long index, final String[] parts) {
            this.timepoint = timepoint;
            this.setup = setup;
            this.level = level;
            this.index = index;
            this.parts = parts;

            int value = Long.hashCode(index);
            value = 31 * value + level;
            value = 31 * value + setup;
            value = 31 * value + timepoint;
            hashcode = value;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other)
                return true;
            if (!(other instanceof VolatileGlobalCellCache.Key))
                return false;
            final Key that = (Key) other;
            return (this.index == that.index) && (this.timepoint == that.timepoint) && (this.setup == that.setup) && (this.level == that.level);
        }

        final int hashcode;

        @Override
        public int hashCode() {
            return hashcode;
        }
    }

    private final CacheLoader<Key, Cell<?>> loader;

    private final LoaderCache<Key, Cell<?>> cache;

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
    private final BigDataServer.ExtendedCompressionOptions compressionParams;

    private ArrayList<ICacheFile> cachedCodebooks = null;
    private HashMap<Integer, ImageCompressor> compressors = null;
    private ImageCompressor lowestResCompressor = null;

    private Stack<MemoryOutputStream> cachedBuffers = null;
    private final int INITIAL_BUFFER_SIZE = 2048;
    private long accumulation = 0;
    private long uncompressedAccumulation = 0;


    private synchronized long addToAccumulation(final int value) {
        accumulation += value;
        return accumulation;
    }

    private synchronized long addToUncompressedAccumulation(final int value) {
        uncompressedAccumulation += value;
        return uncompressedAccumulation;
    }


    public CellHandler(final String baseUrl,
                       final String xmlFilename,
                       final String datasetName,
                       final String thumbnailsDirectory,
                       final BigDataServer.ExtendedCompressionOptions compressionOps) throws SpimDataException, IOException {
        final XmlIoSpimDataMinimal io = new XmlIoSpimDataMinimal();
        final SpimDataMinimal spimData = io.load(xmlFilename);
        final SequenceDescriptionMinimal seq = spimData.getSequenceDescription();
        final Hdf5ImageLoader imgLoader = (Hdf5ImageLoader) seq.getImgLoader();
        this.compressionParams = compressionOps;

        final Hdf5VolatileShortArrayLoader cacheArrayLoader = imgLoader.getShortArrayLoader();
        loader = key -> {
            final int[] cellDims = new int[]{
                    Integer.parseInt(key.parts[5]),
                    Integer.parseInt(key.parts[6]),
                    Integer.parseInt(key.parts[7])};
            final long[] cellMin = new long[]{
                    Long.parseLong(key.parts[8]),
                    Long.parseLong(key.parts[9]),
                    Long.parseLong(key.parts[10])};
            return new Cell<>(cellDims, cellMin, cacheArrayLoader.loadArray(key.timepoint, key.setup, key.level, cellDims, cellMin));
        };

        cache = new SoftRefLoaderCache<>();

        // dataSetURL property is used for providing the XML file by replace
        // SequenceDescription>ImageLoader>baseUrl
        this.xmlFilename = xmlFilename;
        baseFilename = xmlFilename.endsWith(".xml") ? xmlFilename.substring(0, xmlFilename.length() - ".xml".length()) : xmlFilename;
        dataSetURL = baseUrl;

        datasetXmlString = buildRemoteDatasetXML(io, spimData, baseUrl);
        metadataJson = buildMetadataJsonString(imgLoader, seq);
        settingsXmlString = buildSettingsXML(baseFilename);
        thumbnailFilename = createThumbnail(spimData, baseFilename, datasetName, thumbnailsDirectory);

        final int numberOfMipmapLevels = imgLoader.getSetupImgLoader(0).numMipmapLevels();
        initializeCompression(numberOfMipmapLevels);
    }

    private ImageCompressor getCompressorForMipmapLevel(final int mipmapLevel) {
        assert (compressors != null && !compressors.isEmpty());
        if (compressors.containsKey(mipmapLevel)) {
            return compressors.get(mipmapLevel);
        }
        return lowestResCompressor;
    }

    private void initializeCompression(final int numberOfMipmapLevels) {
        if (compressionParams == null)
            return;
        this.compressionParams.setInputDataInfo(new FileInputData(this.baseFilename));
        final QuantizationCacheManager qcm = new QuantizationCacheManager(compressionParams.getCodebookCacheFolder());

        cachedCodebooks = qcm.loadAvailableCacheFiles(compressionParams);
        if (cachedCodebooks.isEmpty()) {
            LOG.warn("Didn't find any cached codebook for " + this.baseFilename);
            return;
        }
        LOG.info(String.format("Found %d codebooks for %s.", cachedCodebooks.size(), this.baseFilename));

        final int numberOfCompressors = Math.min((numberOfMipmapLevels - compressionParams.getCompressFromMipmapLevel()),
                                                 cachedCodebooks.size());

        cachedCodebooks.sort(Comparator.comparingInt(obj -> obj.getHeader().getBitsPerCodebookIndex()));
        compressors = new HashMap<>(numberOfCompressors);
        for (int compressorIndex = 0; compressorIndex < numberOfCompressors; compressorIndex++) {
            final ICacheFile levelCacheFile = cachedCodebooks.get((cachedCodebooks.size() - 1) - compressorIndex);
            final int bitsPerCodebookIndex = levelCacheFile.getHeader().getBitsPerCodebookIndex();

            final CompressionOptions compressorOptions = compressionParams.createClone();
            assert (compressorOptions != compressionParams);
            compressorOptions.setBitsPerCodebookIndex(bitsPerCodebookIndex);

            final ImageCompressor compressor = new ImageCompressor(compressorOptions, levelCacheFile);
            final int actualKey = compressorIndex + compressionParams.getCompressFromMipmapLevel();
            compressors.put(actualKey, compressor);
            LOG.info(String.format("  Loaded codebook of size %d for mipmap level %d. '%s'",
                                   levelCacheFile.getHeader().getCodebookSize(),
                                   actualKey,
                                   levelCacheFile.klass()));
            lowestResCompressor = compressor;
        }

        final int initialCompressionCacheSize = 10;
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

    private synchronized void returnBufferForReuse(final MemoryOutputStream buffer) {
        buffer.reset();
        cachedBuffers.push(buffer);
    }

    private FlatBufferInputData createInputDataObject(final short[] data, final int[] cellDims) {
        return new FlatBufferInputData(data, new V3i(cellDims[0], cellDims[1], cellDims[2]), InputData.PixelType.Gray16, this.baseFilename);
    }

    private void respondWithShortArray(final HttpServletResponse response, final short[] data) throws IOException {
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
    }

    private short[] getCachedVolatileCellData(final String[] parts, final int level) {
        final int index = Integer.parseInt(parts[1]);
        final int timepoint = Integer.parseInt(parts[2]);
        final int setup = Integer.parseInt(parts[3]);

        final Key key = new Key(timepoint, setup, level, index, parts);
        short[] data;
        try {
            final Cell<?> cell = cache.get(key, loader);
            data = ((VolatileShortArray) cell.getData()).getCurrentStorageArray();
        } catch (final ExecutionException e) {
            data = new short[0];
        }
        return data;
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

            final int level = Integer.parseInt(parts[4]);
            final short[] data = getCachedVolatileCellData(parts, level);
            respondWithShortArray(response, data);

            response.setContentType("application/octet-stream");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
        } else if (parts[0].equals("cell_qcmp")) {
            final Stopwatch stopwatch = Stopwatch.startNew();
            final int mipmapLevel = Integer.parseInt(parts[4]);
            final int[] cellDims = new int[]{Integer.parseInt(parts[5]), Integer.parseInt(parts[6]), Integer.parseInt(parts[7])};

            final short[] data = getCachedVolatileCellData(parts, mipmapLevel);
            assert (compressors != null && !compressors.isEmpty());

            final FlatBufferInputData inputData = createInputDataObject(data, cellDims);
            final MemoryOutputStream cellCompressionStream = getCachedCompressionBuffer();

            final int compressedContentLength = getCompressorForMipmapLevel(mipmapLevel).streamCompressChunk(cellCompressionStream,
                                                                                                             inputData);

            response.setContentLength(compressedContentLength);
            try (final OutputStream responseStream = response.getOutputStream()) {
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
            stopwatch.stop();

            final long currentlySent = addToAccumulation(compressedContentLength);
            final long uncompressedWouldSent = addToUncompressedAccumulation(data.length * 2);

            if (compressionParams.isVerbose()) {

                LOG.info(String.format("Sending %dB instead of %dB. Currently sent %dB instead of %dB. Handler finished in %s",
                                       compressedContentLength,
                                       (data.length * 2),
                                       currentlySent,
                                       uncompressedWouldSent,
                                       stopwatch.getElapsedTimeString()));

            }
        } else if (parts[0].equals("init")) {
            respondWithString(baseRequest, response, "application/json", metadataJson);
        } else if (parts[0].equals("init_qcmp")) {
            respondWithCompressionInfo(baseRequest, response);
        } else if (parts[0].equals("qcmp_summary")) {
            respondWithCompressionSummary(baseRequest, response);
        }
    }

    private void respondWithCompressionSummary(final Request baseRequest, final HttpServletResponse response) throws IOException {
        final long currentlySent = addToAccumulation(0);
        final long uncompressedWouldSent = addToUncompressedAccumulation(0);

        final double sentKB = ((double) currentlySent / 1000.0);
        final double sentMB = ((double) currentlySent / 1000.0) / 1000.0;
        final double wouldSentKB = ((double) uncompressedWouldSent / 1000.0);
        final double wouldSentMB = ((double) uncompressedWouldSent / 1000.0) / 1000.0;
        final double percentage = (double) currentlySent / (double) uncompressedWouldSent;


        respondWithString(baseRequest, response, "text/plain",
                          String.format("Currently sent %d B (%.1f KB, %.1f MB) instead of %d B (%.1f KB, %.1f MB).\nPercentage: %.3f",
                                        currentlySent, sentKB, sentMB, uncompressedWouldSent, wouldSentKB, wouldSentMB, percentage),
                          HttpServletResponse.SC_OK);

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

    private void respondWithCompressionInfo(final Request baseRequest, final HttpServletResponse response) throws IOException {
        if (cachedCodebooks == null || cachedCodebooks.isEmpty()) {
            LOG.info("QCMP initialization request was refused, QCMP compression is not enabled.");
            respondWithString(baseRequest, response,
                              "text/plain", "QCMP Compression wasn't enabled on BigDataViewer server.",
                              HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        try (final DataOutputStream dos = new DataOutputStream(response.getOutputStream())) {
            dos.writeByte(compressionParams.getCompressFromMipmapLevel());
            dos.writeByte(cachedCodebooks.size());
            for (final ICacheFile cacheFile : cachedCodebooks) {
                cacheFile.writeToStream(dos);
            }
        } // Stream gets closed here.
        response.setContentType("application/octet-stream");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
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
            } catch (final JDOMException | IOException e) {
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