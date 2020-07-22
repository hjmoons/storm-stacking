package storm.v3.input;

public class Preprocessor {

    static String printable = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~ ";

    public int[][] convert(String url) {
        int[][] result = new int[1][80];

        if (url.length() < 80) {
            for (int i = 80 - url.length(), j = 0; i < 80; i++, j++)
                result[0][i] = printable.indexOf(url.charAt(j)) + 1;
            for (int i = 0; i < 79 - url.length(); i++)
                result[0][i] = 0;
        } else {
            for (int j = 0; j < 80; j++)
                result[0][j] = printable.indexOf(url.charAt(j + url.length() - 75)) + 1;
        }
        return result;
    }
}
