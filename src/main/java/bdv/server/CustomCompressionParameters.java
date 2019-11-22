package bdv.server;

public class CustomCompressionParameters {
    private final boolean dumpRequestData;
    private final String dumpFile;
    private final String trainFile;
    private final int bitTarget;

    public String getTrainFile() {
        return trainFile;
    }

    public int getBitTarget() {
        return bitTarget;
    }

    private final boolean enableRequestCompression;
    private final boolean renderDifference;

    public CustomCompressionParameters(final String dumpFile,
                                       final String trainFile,
                                       final int bitTarget,
                                       final boolean enableRequestCompression,
                                       final boolean renderDifference) {
        this.dumpFile = dumpFile;
        this.trainFile = trainFile;
        this.bitTarget=bitTarget;
        this.dumpRequestData = !this.dumpFile.isEmpty();
        this.enableRequestCompression = enableRequestCompression;
        this.renderDifference = renderDifference;
    }

    public boolean shouldDumpRequestData() {
        return dumpRequestData;
    }

    public String getDumpFile() {
        return dumpFile;
    }

    public boolean shouldCompressData() {
        return enableRequestCompression;
    }

    public boolean renderDifference() {
        return renderDifference;
    }

}
