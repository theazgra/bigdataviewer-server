package bdv.util;

import org.apache.commons.cli.Option;
import org.jetbrains.annotations.NotNull;

public class OptionWithOrder extends Option implements Comparable<OptionWithOrder> {

    private int order = 0;

    public OptionWithOrder(final Option option, final int order) {
        super(option.getOpt(), option.getLongOpt(), option.hasArg(), option.getDescription());
        this.order = order;
    }

    public OptionWithOrder(String opt, String description) throws IllegalArgumentException {
        super(opt, description);
    }

    public OptionWithOrder(String opt, boolean hasArg, String description) throws IllegalArgumentException {
        super(opt, hasArg, description);
    }

    public OptionWithOrder(String opt, String longOpt, boolean hasArg, String description) throws IllegalArgumentException {
        super(opt, longOpt, hasArg, description);
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    @Override
    public int compareTo(@NotNull OptionWithOrder other) {
        return Integer.compare(order, other.order);
    }
}
