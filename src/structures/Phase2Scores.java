package structures;

/** Per-candidate, per-direction score table with an explicit candidate-to-point index mapping. */
public final class Phase2Scores {

    private final int[] pointIndex;
    private final double[][] scores;

    public Phase2Scores(int[] pointIndex, int utilityCount) {
        this.pointIndex = pointIndex;
        this.scores = new double[pointIndex.length][utilityCount];
    }

    public int candidateCount() {
        return pointIndex.length;
    }

    public int utilityCount() {
        return scores.length == 0 ? 0 : scores[0].length;
    }

    public int pointOf(int candidatePos) {
        return pointIndex[candidatePos];
    }

    public double[] row(int candidatePos) {
        return scores[candidatePos];
    }

    public void set(int candidatePos, int utilityId, double value) {
        scores[candidatePos][utilityId] = value;
    }

    public double get(int candidatePos, int utilityId) {
        return scores[candidatePos][utilityId];
    }
}
