package structures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Binds each current utility direction to a stable index, source and objective weights. */
public final class CurrentUtilityPool {

    /** Origin of a sampled direction, including inherited coordinate-axis provenance. */
    public enum Source {

        HIST,

        SEED_TOP,

        SEED_RANDOM,

        NEW_DIRECTION,

        AXIS,

        FIXED,

        UNIFORM
    }

    /** Pool index corresponds to the same direction index in the scoring table. */
    public static final class Entry {

        public final int id;
        public final double[] vector;
        public final Source source;

        /** Unnormalized objective weight; normalizedWeight stores its share of the total. */
        public double rawWeight = 1.0;
        public double normalizedWeight = 0.0;

        Entry(int id, double[] vector, Source source) {
            this.id = id;
            this.vector = vector;
            this.source = source;
        }

        /** Rounded vector fingerprint used only by diagnostics. */
        public String vectorHash() {
            long h = 1125899906842597L;
            for (double v : vector) {
                h = 31 * h + Double.doubleToLongBits(Math.round(v * 1e6) / 1e6);
            }
            return String.format("%016x", h);
        }
    }

    private final List<Entry> entries = new ArrayList<>();

    public void add(double[] vector, Source source) {
        entries.add(new Entry(entries.size(), vector, source));
    }

    public int size() {
        return entries.size();
    }

    public Entry get(int id) {
        return entries.get(id);
    }

    public List<Entry> entries() {
        return Collections.unmodifiableList(entries);
    }

    /** Applies one weight per direction and normalizes; uses equal weights if the total is nonpositive. */
    public void applyWeights(double[] weights) {
        if (weights.length != entries.size()) {
            throw new IllegalStateException(
                "权重数量与效用池不一致: " + weights.length + " vs " + entries.size());
        }
        double sum = 0.0;
        for (int i = 0; i < entries.size(); i++) {
            entries.get(i).rawWeight = weights[i];
            sum += weights[i];
        }

        double equal = entries.isEmpty() ? 0.0 : 1.0 / entries.size();
        for (Entry e : entries) {
            e.normalizedWeight = sum > 0.0 ? e.rawWeight / sum : equal;
        }
    }

    public int countBySource(Source source) {
        int n = 0;
        for (Entry e : entries) {
            if (e.source == source) {
                n++;
            }
        }
        return n;
    }

    /** Returns the normalized weight share belonging to a given source. */
    public double weightShare(Source source) {
        double share = 0.0;
        for (Entry e : entries) {
            if (e.source == source) {
                share += e.normalizedWeight;
            }
        }
        return share;
    }
}
