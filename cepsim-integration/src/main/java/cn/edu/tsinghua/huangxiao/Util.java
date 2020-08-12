package cn.edu.tsinghua.huangxiao;

import java.io.*;

public class Util {

    public static String fileToString(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        StringBuilder sb = new StringBuilder();

        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line);
            sb.append('\n');
        }

        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        File file = new File("test");

        String content = fileToString(file);
        System.out.println(content);
    }

}
