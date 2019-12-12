package bdv.server;


import compression.U16;
import compression.quantization.QuantizationValueCache;
import compression.quantization.scalar.LloydMaxU16ScalarQuantization;
import compression.quantization.scalar.ScalarQuantizer;
import compression.utilities.Utils;
import mpicbg.spim.data.SpimDataException;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ConnectorStatistics;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.io.File;
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

    private static ScalarQuantizer quantizer;

    static Parameters getDefaultParameters() {
        final int port = 8080;
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            hostname = "localhost";
        }
        final String thumbnailDirectory = null;
        final boolean enableManagerContext = false;
        return new Parameters(port, hostname, new HashMap<String, String>(), thumbnailDirectory, enableManagerContext,
                new CustomCompressionParameters("", "", 8, false, false, -1));
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
        final String baseURL = "http://" + server.getURI().getHost() + ":" + params.getPort();


        final CustomCompressionParameters compParams = params.getCompressionParams();
        if (compParams.shouldCompressData() || compParams.renderDifference()) {
            //TODO(Moravec): Replace LloydMaxU16ScalarQuantization with some ICompressor.

            QuantizationValueCache quantizationCache = new QuantizationValueCache("D:\\biology\\bdv_cache");
            final int quantizationValueCount = (int) Math.pow(2, compParams.getBitTarget());

            final String trainFilename = new File(compParams.getTrainFile()).getName();
            if (quantizationCache.areQuantizationValueCached(trainFilename, quantizationValueCount)) {
                LOG.info("Found cached quantization values...");
                final int[] centroids = quantizationCache.readCachedValues(trainFilename, quantizationValueCount);
                assert (centroids.length == quantizationValueCount) : "Cache is corrupted";
                quantizer = new ScalarQuantizer(U16.Min, U16.Max, centroids);
                LOG.info("Initialized quantizer...");
            } else {
                LOG.info("Calculating quantization values...");

                LloydMaxU16ScalarQuantization lloydMax = new LloydMaxU16ScalarQuantization(
                        Utils.convertU16ByteArrayToIntArray(Utils.readFileBytes(compParams.getTrainFile())),
                        (int) Math.pow(2, compParams.getBitTarget()));

                lloydMax.train();
                quantizationCache.saveQuantizationValue(trainFilename, lloydMax.getCentroids());
                LOG.info("Saving quantization values...");
                quantizer = new ScalarQuantizer(U16.Min, U16.Max, lloydMax.getCentroids());
            }
        }


        // Handler initialization
        final HandlerCollection handlers = new HandlerCollection();

        final ContextHandlerCollection datasetHandlers = createHandlers(baseURL, params, thumbnailsDirectoryName);
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
        private final Map<String, String> datasetNameToXml;

        private final String thumbnailDirectory;


        private final CustomCompressionParameters compressionParam;

        private final boolean enableManagerContext;

        Parameters(final int port, final String hostname, final Map<String, String> datasetNameToXml,
                   final String thumbnailDirectory, final boolean enableManagerContext,
                   final CustomCompressionParameters customCompressionParameters) {
            this.port = port;
            this.hostname = hostname;
            this.datasetNameToXml = datasetNameToXml;
            this.thumbnailDirectory = thumbnailDirectory;
            this.enableManagerContext = enableManagerContext;
            this.compressionParam = customCompressionParameters;
        }

        public int getPort() {
            return port;
        }

        public String getHostname() {
            return hostname;
        }

        public String getThumbnailDirectory() {
            return thumbnailDirectory;
        }

        /**
         * Get datasets.
         *
         * @return datasets as a map from dataset name to dataset xml path.
         */
        public Map<String, String> getDatasets() {
            return datasetNameToXml;
        }

        public boolean enableManagerContext() {
            return enableManagerContext;
        }

        public CustomCompressionParameters getCompressionParams() {
            return compressionParam;
        }

    }


    @SuppressWarnings("static-access")
    static private Parameters processOptions(final String[] args, final Parameters defaultParameters) throws IOException {
        final String BIT_TARGET = "bits";
        final String ENABLE_COMPRESSION = "compress";
        final String ENABLE_COMPRESSION_DIFF = "diff";
        final String DUMP_FILE = "dump";
        final String TRAIN_FILE = "train";
        final String DIFF_THRESHOLD = "diffthreshold";
        // create Options object
        final Options options = new Options();

        final String cmdLineSyntax = "BigDataServer [OPTIONS] [NAME XML] ...\n";

        final String description =
                "Serves one or more XML/HDF5 datasets for remote access over HTTP.\n" +
                        "Provide (NAME XML) pairs on the command line or in a dataset file, where NAME is the name under which the dataset should be made accessible and XML is the path to the XML file of the dataset.";

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
                .withDescription("Dataset file: A plain text file specifying one dataset per line. Each line is formatted as \"NAME <TAB> XML\".")
                .hasArg()
                .withArgName("FILE")
                .create("d"));

        options.addOption(OptionBuilder
                .withDescription("Directory to store thumbnails. (new temporary directory by default.)")
                .hasArg()
                .withArgName("DIRECTORY")
                .create("t"));


        options.addOption(OptionBuilder
                .withDescription("File in which to store request data dump")
                .hasArg()
                .withArgName("DUMP")
                .create(DUMP_FILE));

        options.addOption(OptionBuilder
                .withDescription("Enable request compression")
                .create(ENABLE_COMPRESSION));

        options.addOption(OptionBuilder
                .withDescription("Compression train file")
                .hasArg()
                .withArgName("TRAINFILE")
                .create(TRAIN_FILE));

        options.addOption(OptionBuilder
                .withDescription("Compression bit target")
                .hasArg()
                .withArgName("BITS")
                .create(BIT_TARGET));

        options.addOption(OptionBuilder
                .withDescription("Send compression difference")
                .create(ENABLE_COMPRESSION_DIFF));

        options.addOption(OptionBuilder
                .withDescription("Render difference above this threshold")
                .hasArg()
                .withArgName(DIFF_THRESHOLD)
                .create(DIFF_THRESHOLD));

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

            final HashMap<String, String> datasets = new HashMap<String, String>(defaultParameters.getDatasets());

            // Custom compression parameters
            //cmd.hasOption()

            final String dumpFile = cmd.getOptionValue(DUMP_FILE, "");
            final boolean enableCompression = cmd.hasOption(ENABLE_COMPRESSION);
            final boolean enableCompressionDiff = cmd.hasOption(ENABLE_COMPRESSION_DIFF);
            final String trainFile = cmd.getOptionValue(TRAIN_FILE, "");
            final int bitTarget = Integer.parseInt(cmd.getOptionValue(BIT_TARGET, "8"));
            final int diffThreshold = Integer.parseInt(cmd.getOptionValue(DIFF_THRESHOLD, "-1"));
            if (diffThreshold > -1) {
                LOG.info("Diff threshold is set to: " + diffThreshold);
            }

            if ((enableCompression || enableCompressionDiff) && (trainFile.isEmpty())) {
                throw new MissingArgumentException(String.format("!!! %s must be specified when %s or %s is specified !!!",
                        TRAIN_FILE, ENABLE_COMPRESSION, ENABLE_COMPRESSION_DIFF));
            }

            final CustomCompressionParameters customCompParams = new CustomCompressionParameters(dumpFile, trainFile, bitTarget,
                    enableCompression, enableCompressionDiff, diffThreshold);

            LOG.info("Compression is " + (enableCompression ? "Matched" : "Not matched"));
            LOG.info("Compression-Diff is " + (enableCompressionDiff ? "Matched" : "Not matched"));

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
                    if (tokens.length == 2 && StringUtils.isNotEmpty(tokens[0].trim()) && StringUtils.isNotEmpty(tokens[1].trim())) {
                        final String name = tokens[0].trim();
                        final String xmlpath = tokens[1].trim();
                        tryAddDataset(datasets, name, xmlpath);
                    } else {
                        LOG.warn("Invalid dataset file line (will be skipped): {" + str + "}");
                    }
                }
            }

            // process additional {name, name.xml} pairs given on the
            // command-line
            final String[] leftoverArgs = cmd.getArgs();
            if (leftoverArgs.length % 2 != 0)
                throw new IllegalArgumentException("Dataset list has an error while processing.");

            for (int i = 0; i < leftoverArgs.length; i += 2) {
                final String name = leftoverArgs[i];
                final String xmlpath = leftoverArgs[i + 1];
                tryAddDataset(datasets, name, xmlpath);
            }

            if (datasets.isEmpty())
                throw new IllegalArgumentException("Dataset list is empty.");

            return new Parameters(port, serverName, datasets, thumbnailDirectory, enableManagerContext,
                    customCompParams);
        } catch (final ParseException | IllegalArgumentException e) {
            LOG.warn(e.getMessage());
            System.out.println();
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(cmdLineSyntax, description, options, null);
        }
        return null;
    }

    private static void tryAddDataset(final HashMap<String, String> datasetNameToXML, final String name, final String xmlpath) throws IllegalArgumentException {
        for (final String reserved : Constants.RESERVED_CONTEXT_NAMES)
            if (name.equals(reserved))
                throw new IllegalArgumentException("Cannot use dataset name: \"" + name + "\" (reserved for internal use).");
        if (datasetNameToXML.containsKey(name))
            throw new IllegalArgumentException("Duplicate dataset name: \"" + name + "\"");
        if (Files.notExists(Paths.get(xmlpath)))
            throw new IllegalArgumentException("Dataset file does not exist: \"" + xmlpath + "\"");
        datasetNameToXML.put(name, xmlpath);
        LOG.info("Dataset added: {" + name + ", " + xmlpath + "}");
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
                    LOG.warn("Could not create thumbnails directory \"" + thumbnailDirectoryName + "\".\n Trying to create temporary directory.");
                }
            } else {
                if (!Files.isDirectory(thumbnails))
                    LOG.warn("Thumbnails directory \"" + thumbnailDirectoryName + "\" is not a directory.\n Trying to create temporary directory.");
                else
                    return thumbnails.toFile().getAbsolutePath();
            }
        }
        final Path thumbnails = Files.createTempDirectory("thumbnails");
        thumbnails.toFile().deleteOnExit();
        return thumbnails.toFile().getAbsolutePath();
    }

    private static ContextHandlerCollection createHandlers(final String baseURL,
                                                           final Parameters params,
                                                           final String thumbnailsDirectoryName) throws SpimDataException, IOException {

        final ContextHandlerCollection handlers = new ContextHandlerCollection();

        final Map<String, String> dataSet = params.getDatasets();
        for (final Entry<String, String> entry : dataSet.entrySet()) {
            final String name = entry.getKey();
            final String xmlpath = entry.getValue();
            final String context = "/" + name;
            final CellHandler ctx = new CellHandler(baseURL + context + "/", xmlpath, name,
                    thumbnailsDirectoryName,
                    params.getCompressionParams(), quantizer);

            ctx.setContextPath(context);
            handlers.addHandler(ctx);
        }

        return handlers;
    }
}
