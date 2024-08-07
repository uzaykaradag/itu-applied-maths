import java.util.HashMap;

public class Coordinate {

    double latitude;
    double longitude;

    public Coordinate(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Coordinate() {

    }

    /*
    This method returns the coordinates for the optimal point to open the shop.
    Needs three parameters to be passed in which are:
        pollResults: A two-dimensional array filled with the results of the polling.
        Every "Yes, I would regularly go" is a +1 and every "No, I wouldn't regularly go" is a -1.
        Every index in pollResults represents the corresponding toll of the +1's and -1 in the area.

        upperRight: Upper right coordinate of the given place (rectangle).
        lowerLeft: Lower left coordinate of the given place (rectangle).

        The place in this case is obviously Istanbul Technical University.

     */
    public static Coordinate findOptimalPointCoordinates(int[][] pollResults, Coordinate upperRight, Coordinate lowerLeft) {
        int sizeRow = pollResults.length;
        int sizeCol = pollResults[0].length;

        HashMap<String, Integer> resBentley = Main.bentleyMax(pollResults);
        int optimalPointRow = Math.floorDiv((resBentley.get("up") + resBentley.get("down")), 2);
        int optimalPointCol = Math.floorDiv((resBentley.get("left") + resBentley.get("right")), 2);

        Coordinate optimalPointCoordinates = new Coordinate();

        double upperLat = upperRight.latitude;
        double rightLong = upperRight.longitude;
        double lowerLat = lowerLeft.latitude;
        double leftLong = lowerLeft.longitude;

        double rowIncrement = (upperLat - lowerLat) / sizeRow;
        double colIncrement = (rightLong - leftLong) / sizeCol;

        optimalPointCoordinates.latitude = lowerLat + optimalPointRow * rowIncrement;
        optimalPointCoordinates.longitude = leftLong + optimalPointCol * colIncrement;

        return optimalPointCoordinates;
    }
}
