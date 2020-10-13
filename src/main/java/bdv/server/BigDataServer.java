package bdv.server;

import bdv.util.OptionWithOrder;
import cz.it4i.qcmp.cli.CliConstants;
import cz.it4i.qcmp.cli.ParseUtils;
import cz.it4i.qcmp.compression.CompressionOptions;
import cz.it4i.qcmp.data.V2i;
import cz.it4i.qcmp.data.V3i;
import cz.it4i.qcmp.fileformat.QuantizationType;
import mpicbg.spim.data.SpimDataException;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Serve XML/HDF5 datasets over HTTP.
 *
 * <pre>
 * usage: BigDataServer [OPTIONS] [NAME XML]...
 * Serves one or more XML/HDF5 datasets for remote access over HTTP.
 * Provide (NAME XML) pairs on the command line or in a dataset file, where
 * NAME is the name under which the dataset should be made accessible and XML
 * is the path to the XML file of the dataset.
 *  -d &lt;FILE&gt;       Dataset file: A plain text file specifying one dataset
 *                  per line. Each line is formatted as "NAME &lt;TAB&gt; XML".
 *  -p &lt;PORT&gt;       Listening port. (default: 8080)
 *  -s &lt;HOSTNAME&gt;   Hostname of the server.
 *  -t &lt;DIRECTORY&gt;  Directory to store thumbnails. (new temporary directory
 *                  by default.)
 *  -m              enable statistics and manager context. EXPERIMENTAL!
 * </pre>
 * <p>
 * To enable the {@code -m} option, build with
 * {@link Constants#ENABLE_EXPERIMENTAL_FEATURES} set to {@code true}.
 *
 * @author Tobias Pietzsch &lt;tobias.pietzsch@gmail.com&gt;
 * @author HongKee Moon &lt;moon@mpi-cbg.quantization.de&gt;
 */
public class BigDataServer {
    private static final org.eclipse.jetty.util.log.Logger LOG = Log.getLogger(BigDataServer.class);

    public final static class ExtendedCompressionOptions extends CompressionOptions {
        private int compressFromMipmapLevel;
        private boolean enableCodebookTraining = false;

        public int getCompressFromMipmapLevel() {
            return compressFromMipmapLevel;
        }

        public void setCompressFromMipmapLevel(final int compressFromMipmapLevel) {
            this.compressFromMipmapLevel = compressFromMipmapLevel;
        }

        public boolean isCodebookTrainingEnabled() {
            return enableCodebookTraining;
        }

        public void setEnableCodebookTraining(final boolean enableCodebookTraining) {
            this.enableCodebookTraining = enableCodebookTraining;
        }

        public ExtendedCompressionOptions copyRequiredParams() {
            final ExtendedCompressionOptions copy = new ExtendedCompressionOptions();
            copy.setQuantizationType(getQuantizationType());
            copy.setQuantizationVector(getQuantizationVector());
            copy.setWorkerCount(getWorkerCount());
            copy.setCodebookType(getCodebookType());
            copy.setCodebookCacheFolder(getCodebookCacheFolder());
            copy.setVerbose(isVerbose());
            copy.setCompressFromMipmapLevel(getCompressFromMipmapLevel());
            return copy;
        }
    }

    final static class BigDataServerDataset {
        private final String xmlFile;
        private final ExtendedCompressionOptions compressionOptions;

        BigDataServerDataset(final String xmlFile,
                             final ExtendedCompressionOptions compressionOptions) {
            this.xmlFile = xmlFile;
            this.compressionOptions = compressionOptions;
        }

        public String getXmlFile() {
            return xmlFile;
        }

        public ExtendedCompressionOptions getCompressionOptions() {
            return compressionOptions;
        }

    }


    static Parameters getDefaultParameters() {

        final int port = 8080;
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            hostname = "localhost";
        }
        final String thumbnailDirectory = null;
        final String baseUrl = null;
        final boolean enableManagerContext = false;
        return new Parameters(port,
                              hostname,
                              new HashMap<String, BigDataServerDataset>(),
                              thumbnailDirectory,
                              baseUrl,
                              enableManagerContext,
                              new ExtendedCompressionOptions());
    }

    public static void main(final String[] args) throws Exception {
        System.setProperty("org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog");

        final Parameters params = processOptions(args, getDefaultParameters());
        if (params == null)
            return;

        final String thumbnailsDirectoryName = getThumbnailDirectoryPath(params);

        // Threadpool for multiple connections
        final Server server = new Server(new QueuedThreadPool(200, 8));

        // ServerConnector configuration
        final ServerConnector connector = new ServerConnector(server);
        connector.setHost(params.getHostname());
        connector.setPort(params.getPort());
        LOG.info("Set connectors: " + connector);
        server.setConnectors(new Connector[]{connector});
        final String baseURL = params.getBaseUrl() != null ? params.getBaseUrl() :
                "http://" + server.getURI().getHost() + ":" + params.getPort();
        System.out.println("baseURL = " + baseURL);

        // Handler initialization
        final HandlerCollection handlers = new HandlerCollection();

        final ContextHandlerCollection datasetHandlers = createHandlers(baseURL,
                                                                        params.getDatasets(),
                                                                        thumbnailsDirectoryName);
        handlers.addHandler(datasetHandlers);
        handlers.addHandler(new JsonDatasetListHandler(server, datasetHandlers));

        Handler handler = handlers;
        if (params.enableManagerContext()) {
            // Add Statistics bean to the connector
            final ConnectorStatistics connectorStats = new ConnectorStatistics();
            connector.addBean(connectorStats);

            // create StatisticsHandler wrapper and ManagerHandler
            final StatisticsHandler statHandler = new StatisticsHandler();
            handlers.addHandler(new ManagerHandler(baseURL, server, connectorStats, statHandler, datasetHandlers, thumbnailsDirectoryName));
            statHandler.setHandler(handlers);
            handler = statHandler;
        }

        LOG.info("Set handler: " + handler);
        server.setHandler(handler);
        LOG.info("Server Base URL: " + baseURL);
        LOG.info("BigDataServer starting");
        server.start();
        server.join();
    }

    /**
     * Server parameters: hostname, port, datasets.
     */
    private static class Parameters {
        private final int port;

        private final String hostname;

        /**
         * maps from dataset name to dataset xml path.
         */
        private final Map<String, BigDataServerDataset> datasetNameToXml;

        private final String thumbnailDirectory;

        private final String baseUrl;

        private final boolean enableManagerContext;

        private final ExtendedCompressionOptions compressionParam;

        Parameters(final int port,
                   final String hostname,
                   final Map<String, BigDataServerDataset> datasetNameToXml,
                   final String thumbnailDirectory,
                   final String baseUrl,
                   final boolean enableManagerContext,
                   final ExtendedCompressionOptions extendedCompressionOptions) {
            this.port = port;
            this.hostname = hostname;
            this.datasetNameToXml = datasetNameToXml;
            this.thumbnailDirectory = thumbnailDirectory;
            this.baseUrl = baseUrl;
            this.enableManagerContext = enableManagerContext;
            this.compressionParam = extendedCompressionOptions;
        }

        public int getPort() {
            return port;
        }

        public String getHostname() {
            return hostname;
        }

        public String getBaseUrl() {
            return baseUrl;
        }

        public String getThumbnailDirectory() {
            return thumbnailDirectory;
        }

        /**
         * Get datasets.
         *
         * @return datasets as a map from dataset name to dataset xml path.
         */
        public Map<String, BigDataServerDataset> getDatasets() {
            return datasetNameToXml;
        }

        public boolean enableManagerContext() {
            return enableManagerContext;
        }

        public ExtendedCompressionOptions getCompressionParams() {
            return compressionParam;
        }
    }

    @SuppressWarnings("static-access")
    static private Parameters processOptions(final String[] args, final Parameters defaultParameters) throws IOException {
        final String ENABLE_COMPRESSION = "qcmp";
        final String CompressFromShortKey = "cf";
        final String CompressFromLongKey = "compress-from";

        // create Options object
        final Options options = new Options();

        final String cmdLineSyntax = "BigDataServer [OPTIONS] [NAME XML] ...\n";

        final String description =
                "Serves one or more XML/HDF5 datasets for remote access over HTTP.\n" +
                        "Provide (NAME XML) pairs on the command line or in a dataset file, where NAME is the name under which the " +
                        "dataset should be made accessible and XML is the path to the XML file of the dataset.\n" +
                        "If -qcmp option is specified, these options are enabled:\u001b[35m-sq,-vq,-b,-cbc\u001b[0m\n";

        options.addOption(OptionBuilder
                                  .withDescription("Hostname of the server.\n(default: " + defaultParameters.getHostname() + ")")
                                  .hasArg()
                                  .withArgName("HOSTNAME")
                                  .create("s"));

        options.addOption(OptionBuilder
                                  .withDescription("Listening port.\n(default: " + defaultParameters.getPort() + ")")
                                  .hasArg()
                                  .withArgName("PORT")
                                  .create("p"));

        // -d or multiple {name name.xml} pairs
        options.addOption(OptionBuilder
                                  .withDescription(
                                          "Dataset file: A plain text file specifying one dataset per line. Each line is formatted as " +
                                                  "\"NAME <TAB> XML\".")
                                  .hasArg()
                                  .withArgName("FILE")
                                  .create("d"));

        options.addOption(OptionBuilder
                                  .withDescription("Directory to store thumbnails. (new temporary directory by default.)")
                                  .hasArg()
                                  .withArgName("DIRECTORY")
                                  .create("t"));

        options.addOption(OptionBuilder
                                  .withDescription("Base URL under which the server will be made visible (e.g., if behind a proxy)")
                                  .hasArg()
                                  .withArgName("BASEURL")
                                  .create("b"));

        int optionOrder = 0;
        options.addOption(new OptionWithOrder(OptionBuilder.withDescription("Enable QCMP compression")
                                                      .create(ENABLE_COMPRESSION), ++optionOrder));


        options.addOption(new OptionWithOrder(CliConstants.createCBCMethod(), ++optionOrder));
        //        options.addOption(new OptionWithOrder(CliConstants.createSQOption(), ++optionOrder));
        options.addOption(new OptionWithOrder(CliConstants.createVQOption(), ++optionOrder));
        options.addOption(new OptionWithOrder(CliConstants.createVerboseOption(false), ++optionOrder));
        options.addOption(new OptionWithOrder(new Option(CompressFromShortKey, CompressFromLongKey, true,
                                                         "Level from which the compression is enabled."), ++optionOrder));
        options.addOption(new OptionWithOrder(new Option(CliConstants.WORKER_COUNT_SHORT, CliConstants.WORKER_COUNT_LONG,
                                                         true, "Count of worker threads, which are used for codebook training."),
                                              ++optionOrder));


        if (Constants.ENABLE_EXPERIMENTAL_FEATURES) {
            options.addOption(OptionBuilder
                                      .withDescription("enable statistics and manager context. EXPERIMENTAL!")
                                      .create("m"));
        }

        try {
            final CommandLineParser parser = new BasicParser();
            final CommandLine cmd = parser.parse(options, args);

            // Getting port number option
            final String portString = cmd.getOptionValue("p", Integer.toString(defaultParameters.getPort()));
            final int port = Integer.parseInt(portString);

            // Getting server name option
            final String serverName = cmd.getOptionValue("s", defaultParameters.getHostname());

            // Getting thumbnail directory option
            final String thumbnailDirectory = cmd.getOptionValue("t", defaultParameters.getThumbnailDirectory());

            // Getting base url option
            final String baseUrl = cmd.getOptionValue("b", defaultParameters.getBaseUrl());

            final HashMap<String, BigDataServerDataset> datasets =
                    new HashMap<String, BigDataServerDataset>(defaultParameters.getDatasets());

            final boolean enableQcmpCompression = cmd.hasOption(ENABLE_COMPRESSION);
            final ExtendedCompressionOptions baseCompressionOptions = new ExtendedCompressionOptions();
            if (enableQcmpCompression) {
                baseCompressionOptions.setWorkerCount(Integer.parseInt(cmd.getOptionValue(CliConstants.WORKER_COUNT_LONG, "1")));
                baseCompressionOptions.setQuantizationType(QuantizationType.Invalid);
                if (cmd.hasOption(CliConstants.SCALAR_QUANTIZATION_LONG))
                    baseCompressionOptions.setQuantizationType(QuantizationType.Scalar);
                else if (cmd.hasOption(CliConstants.VECTOR_QUANTIZATION_LONG)) {
                    final String vqValue = cmd.getOptionValue(CliConstants.VECTOR_QUANTIZATION_LONG);
                    final Optional<V2i> maybeV2 = ParseUtils.tryParseV2i(vqValue, 'x');
                    if (maybeV2.isPresent()) {
                        baseCompressionOptions.setQuantizationType(QuantizationType.Vector2D);
                        baseCompressionOptions.setQuantizationVector(new V3i(maybeV2.get().getX(), maybeV2.get().getY(), 1));
                    } else {
                        final Optional<V3i> maybeV3 = ParseUtils.tryParseV3i(vqValue, 'x');
                        if (maybeV3.isPresent()) {
                            baseCompressionOptions.setQuantizationType(QuantizationType.Vector3D);
                            baseCompressionOptions.setQuantizationVector(maybeV3.get());
                        }
                    }
                }
                if (baseCompressionOptions.getQuantizationType() == QuantizationType.Invalid) {
                    throw new ParseException("Invalid quantization type.");
                }

                baseCompressionOptions.setCodebookType(CompressionOptions.CodebookType.Global);
                baseCompressionOptions.setCodebookCacheFolder(cmd.getOptionValue(CliConstants.CODEBOOK_CACHE_FOLDER_LONG));
                baseCompressionOptions.setVerbose(cmd.hasOption(CliConstants.VERBOSE_LONG));


                if (cmd.hasOption(CompressFromLongKey)) {
                    baseCompressionOptions.setCompressFromMipmapLevel(Integer.parseInt(cmd.getOptionValue(CompressFromLongKey)));
                }

                final StringBuilder compressionReport = new StringBuilder();
                compressionReport.append("\u001b[33m");
                compressionReport.append("Quantization type: ");
                switch (baseCompressionOptions.getQuantizationType()) {
                    case Scalar:
                        compressionReport.append("Scalar");
                        break;
                    case Vector1D:
                        compressionReport.append("Vector1D");
                        break;
                    case Vector2D:
                        compressionReport.append("Vector2D");
                        break;
                    case Vector3D:
                        compressionReport.append("Vector3D");
                        break;
                }
                compressionReport.append(baseCompressionOptions.getQuantizationVector().toString());
                compressionReport.append('\n');
                compressionReport.append("Codebook cache folder: ").append(baseCompressionOptions.getCodebookCacheFolder()).append('\n');
                compressionReport.append("Verbose mode: ").append(baseCompressionOptions.isVerbose() ? "ON" : "OFF").append('\n');
                compressionReport.append("Worker count: ").append(baseCompressionOptions.getWorkerCount()).append('\n');
                compressionReport.append("CompressFromMipmapLevel: ").append(baseCompressionOptions.getCompressFromMipmapLevel()).append(
                        '\n');
                compressionReport.append("\u001b[0m");

                System.out.println(compressionReport.toString());
            }

            boolean enableManagerContext = false;
            if (Constants.ENABLE_EXPERIMENTAL_FEATURES) {
                if (cmd.hasOption("m"))
                    enableManagerContext = true;
            }

            if (cmd.hasOption("d")) {
                // process the file given with "-d"
                final String datasetFile = cmd.getOptionValue("d");

                // check the file presence
                final Path path = Paths.get(datasetFile);

                if (Files.notExists(path))
                    throw new IllegalArgumentException("Dataset list file does not exist.");

                // Process dataset list file
                final List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);

                for (final String str : lines) {
                    final String[] tokens = str.split("\\s*\\t\\s*");

                    if (tokens.length >= 2 && StringUtils.isNotEmpty(tokens[0].trim()) && StringUtils.isNotEmpty(tokens[1].trim())) {
                        final String name = tokens[0].trim();
                        final String xmlPath = tokens[1].trim();

                        final ExtendedCompressionOptions datasetCompressionOptions = baseCompressionOptions.copyRequiredParams();
                        if (tokens.length == 3 && StringUtils.isNotEmpty(tokens[2].trim())) {
                            if (tokens[2].trim().equals("tcb")) {
                                datasetCompressionOptions.setEnableCodebookTraining(true);
                            }
                        }

                        tryAddDataset(datasets, name, xmlPath, (enableQcmpCompression ? datasetCompressionOptions : null));
                    } else {
                        LOG.warn("Invalid dataset file line (will be skipped): {" + str + "}");
                    }
                }
            }

            // process additional {name, name.xml, [`tcb`]} pair or triplets given on the command-line
            final String[] leftoverArgs = cmd.getArgs();

            for (int i = 0; i < leftoverArgs.length; ) {
                final String name = leftoverArgs[i++];
                final String xmlPath = leftoverArgs[i++];

                final ExtendedCompressionOptions datasetCompressionOptions = baseCompressionOptions.copyRequiredParams();
                if ((i < leftoverArgs.length) && leftoverArgs[i].equals("tcb")) {
                    datasetCompressionOptions.setEnableCodebookTraining(true);
                    i++;
                }

                tryAddDataset(datasets, name, xmlPath, (enableQcmpCompression ? datasetCompressionOptions : null));
            }

            if (datasets.isEmpty())
                throw new IllegalArgumentException("Dataset list is empty.");

            return new Parameters(port,
                                  serverName,
                                  datasets,
                                  thumbnailDirectory,
                                  baseUrl,
                                  enableManagerContext,
                                  enableQcmpCompression ? baseCompressionOptions : null);
        } catch (final ParseException | IllegalArgumentException e) {
            LOG.warn(e.getMessage());
            System.out.println();
            final HelpFormatter formatter = new HelpFormatter();
            formatter.setOptionComparator((x, y) -> {
                if (x instanceof OptionWithOrder && y instanceof OptionWithOrder) {
                    return ((OptionWithOrder) x).compareTo((OptionWithOrder) y);
                } else if (x instanceof OptionWithOrder) {
                    return 1;
                } else if (y instanceof OptionWithOrder) {
                    return -1;
                } else {
                    final Option opt1 = (Option) x;
                    final Option opt2 = (Option) y;
                    return opt1.getOpt().compareToIgnoreCase(opt2.getOpt());
                }
            });
            formatter.printHelp(cmdLineSyntax, description, options, null);
        }
        return null;
    }

    private static void tryAddDataset(final HashMap<String, BigDataServerDataset> datasetNameToXML,
                                      final String name,
                                      final String xmlPath,
                                      final ExtendedCompressionOptions extendedCompressionOptions) throws IllegalArgumentException {
        for (final String reserved : Constants.RESERVED_CONTEXT_NAMES)
            if (name.equals(reserved))
                throw new IllegalArgumentException("Cannot use dataset name: \"" + name + "\" (reserved for internal use).");
        if (datasetNameToXML.containsKey(name))
            throw new IllegalArgumentException("Duplicate dataset name: \"" + name + "\"");
        if (Files.notExists(Paths.get(xmlPath)))
            throw new IllegalArgumentException("Dataset file does not exist: \"" + xmlPath + "\"");

        datasetNameToXML.put(name, new BigDataServerDataset(xmlPath, extendedCompressionOptions));
        LOG.info("Dataset added: {" + name + ", " + xmlPath +
                         ", QcmpDatasetTraining: " + ((extendedCompressionOptions.isCodebookTrainingEnabled()) ? "ON" : "OFF") + "}");
    }

    private static String getThumbnailDirectoryPath(final Parameters params) throws IOException {
        final String thumbnailDirectoryName = params.getThumbnailDirectory();
        if (thumbnailDirectoryName != null) {
            Path thumbnails = Paths.get(thumbnailDirectoryName);
            if (!Files.exists(thumbnails)) {
                try {
                    thumbnails = Files.createDirectories(thumbnails);
                    return thumbnails.toFile().getAbsolutePath();
                } catch (final IOException e) {
                    LOG.warn(e.getMessage());
                    LOG.warn("Could not create thumbnails directory \"" + thumbnailDirectoryName + "\".\n Trying to create temporary " +
                                     "directory.");
                }
            } else {
                if (!Files.isDirectory(thumbnails))
                    LOG.warn("Thumbnails directory \"" + thumbnailDirectoryName + "\" is not a directory.\n Trying to create temporary " +
                                     "directory.");
                else
                    return thumbnails.toFile().getAbsolutePath();
            }
        }
        final Path thumbnails = Files.createTempDirectory("thumbnails");
        thumbnails.toFile().deleteOnExit();
        return thumbnails.toFile().getAbsolutePath();
    }

    private static ContextHandlerCollection createHandlers(final String baseURL,
                                                           final Map<String, BigDataServerDataset> dataSet,
                                                           final String thumbnailsDirectoryName)
            throws SpimDataException, IOException {
        final ContextHandlerCollection handlers = new ContextHandlerCollection();

        for (final Entry<String, BigDataServerDataset> entry : dataSet.entrySet()) {

            final String name = entry.getKey();
            final String xmlPath = entry.getValue().getXmlFile();
            final String context = "/" + name;
            final CellHandler ctx = new CellHandler(baseURL + context + "/", xmlPath,
                                                    name, thumbnailsDirectoryName,
                                                    entry.getValue().getCompressionOptions());
            ctx.setContextPath(context);
            handlers.addHandler(ctx);
        }

        return handlers;
    }
}
