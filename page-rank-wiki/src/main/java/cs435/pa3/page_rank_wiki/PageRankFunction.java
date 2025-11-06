package cs435.pa3.page_rank_wiki;

import org.apache.spark.api.java.function.Function;

public class PageRankFunction implements Function<WikiPage, WikiPage> {

    private final int NUMBER_OF_ITERATIONS;

    PageRankFunction(int numberOfIterations) {
        super();
        this.NUMBER_OF_ITERATIONS = numberOfIterations;
    }

    @Override
    public WikiPage call(WikiPage v1) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'call'");
    }

    private void pageRankAlgorithm(WikiPage page) {
        long[][] matrix = createMatrix(page);
        for (int n = 0; n < NUMBER_OF_ITERATIONS; n++) {
multiply(matrix, null);
        }
    }

    private long[][] createMatrix(WikiPage page) {
        int count = page.getLinks().getCount();

        long[][] m = new long[count][count];
        for (int row = 0; row < m.length; row++) {
            for (int col = 0; col < m[row].length; col++) {
                if (row != col) {
                    m[row][col] = 1 / count;
                }
            }
        }
        return m;
    }

    private long[] multiply(long[][] matrix, long[] vector) {
        long[] result = new long[vector.length];
        for (int row = 0; row < matrix.length; row++) {
            long value = 0;
            for (int col = 0; col < matrix[row].length; col++) {
                if (matrix[row][col] == 0) {
                        
                }
            }
            result[row] = value;
        }
        return result;
    }

}
