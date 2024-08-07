import java.util.Random;
import java.util.HashMap;

public class Main {

    public static void main(String[] args) {
        int[][] pollResults = new int[][]{{60, 44, -41, 1},
                {60, -69, 97, -1},
                {0, -27, 34, 6},
                {-58, -5, -62, 96}};

        System.out.println("============================================================");
        System.out.println("The poll results: (In terms tolls of the given area)");
        printMatrix(pollResults, 0, pollResults.length-1, 0, pollResults[0].length-1);

        printBentley(pollResults);

        Coordinate lowerLeftITU = new Coordinate(41.099686,29.015362);
        Coordinate upperRightITU = new Coordinate(41.110912,29.036613);

        Coordinate result = Coordinate.findOptimalPointCoordinates(pollResults, upperRightITU, lowerLeftITU);
        System.out.println("The coordinates to open the shop for maximum customers according to polls; ");
        System.out.println(result.latitude + ", " + result.longitude);
    }

    public static int[][] generatePollResults(int rowCount, int colCount) {
        Random rand = new Random();

        int[][] pollResults = new int[rowCount][colCount];
        int i, j;
        for(i  = 0; i < rowCount; i++) {
            for(j = 0; j < colCount; j++) {
                pollResults[i][j] = (rand.nextInt() % 100);
            }
        }

        return pollResults;
    }

    /*
    This method is an implementation of Bentley's algorithm for finding the maximum sum sub-matrix.

    Returns a HashMap containing 5 key-value pairs:
        -("max", int) denoting the maximum sum that can be found by
        calculating the element-wise sum of any possible contiguous(rectangle) sub-matrix of the initial matrix.
        -("up", int) denoting the uppermost index of the sub-matrix.
        -("down", int) denoting the down-most index of the sub-matrix.
        -("right", int) denoting the rightmost index of the sub-matrix.
        -("left", int) denoting the leftmost index of the sub-matrix.

    This method uses the rowSum and kadaneMax methods.
     */
    public static HashMap<String, Integer> bentleyMax(int[][] matrix) {
        int colCount = matrix[0].length;
        int[] rowSums;
        int maxSum = 0;
        int right = -1, left = -1, up = -1, down = -1;

        int leftBound, rightBound;
        HashMap<String, Integer> kadaneRes;
        for(leftBound = 0; leftBound < colCount; leftBound++) {
            for(rightBound = leftBound; rightBound < colCount; rightBound++){
                rowSums = rowSum(matrix, leftBound, rightBound);
                kadaneRes = kadaneMax(rowSums);

                if(kadaneRes.get("max") > maxSum) {
                    maxSum = kadaneRes.get("max");
                    up = kadaneRes.get("start");
                    down = kadaneRes.get("end");
                    left = leftBound;
                    right = rightBound;
                }

            }
        }

        HashMap<String, Integer> res = new HashMap<>();
        res.put("up", up);
        res.put("down", down);
        res.put("left", left);
        res.put("right", right);
        res.put("max", maxSum);

        return res;
    }

    /*
    This method is an implementation of Kadane's algorithm to find maximum sum contiguous sub-array.

    Returns a HashMap containing 3 key-value pairs:
        -("max", int) denoting the maximum sum that can be found by
         calculating the element-wise sum of any possible contiguous sub-array of the initial array.
        -("start", int) starting index of the resultant sub-array.
        -("end", int) ending index of the resultant sub-array.
    */
    public static HashMap<String, Integer> kadaneMax(int[] arr) {
        int size = arr.length;
        int[] OPT = new int[size]; //OPT
        int maxSum;
        int start = 0;
        int end = 0;
        OPT[0] = arr[0];
        maxSum = OPT[0];

        int i;
        for (i = 1; i < size; i++) {

            if(arr[i] > (arr[i] + OPT[i - 1])){
                OPT[i] = arr[i];
                start = i;
            } else {
                OPT[i] = arr[i] + OPT[i - 1];
            }

            if(OPT[i] > maxSum) {
                maxSum = OPT[i];
                end = i;
            }
        }

        HashMap<String, Integer> res = new HashMap<>();
        res.put("start", start);
        res.put("end", end);
        res.put("max", maxSum);

        return res;
    }

    /*
    This method is a helper method designed to help the bentleyMax method.

    Returns an array with the row-wise sum of a matrix' rows at the corresponding elements.

    leftBound and rightBound parameters are used to bound the columns included in the row-wise sum.
    leftBound and rightBound indexes themselves are both included in the row-wise sum results. (inclusive!)
     */
    public static int[] rowSum(int[][] matrix, int leftBound, int rightBound) {
        int rowCount = matrix.length;
        int[] sumOfRow = new int[rowCount];

        int i, j;
        for(i = 0; i < rowCount; i++) {
            sumOfRow[i] = 0;
            for(j = leftBound; j <= rightBound; j++) {
                sumOfRow[i] += matrix[i][j];
            }
        }

        return sumOfRow;
    }

    /*
    This method is designed as a formatter method to print the results of bentleyMax.
     */
    public static void printBentley(int[][] matrix) {
        System.out.println("=========================================================");
        HashMap<String, Integer> result = bentleyMax(matrix);
        System.out.println("The maximum sum possible is: " + result.get("max"));
        System.out.println("The sub-matrix: \n");
        printMatrix(matrix, result.get("up"), result.get("down"), result.get("left"), result.get("right"));
        System.out.println("=========================================================");
    }
    /*
    This method is designed as a helper method for printBentley.
     */
    public static void printMatrix(int[][] matrix, int boundUp, int boundDown, int boundLeft, int boundRight) {
        for(int i = boundUp; i <= boundDown; i++) {
            for(int j = boundLeft; j <= boundRight; j++) {
                System.out.printf("%5d", matrix[i][j]);
            }
            System.out.println();
        }
        System.out.println();
    }
}
