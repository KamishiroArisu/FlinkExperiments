package flink.experiments.utils;

import java.nio.file.Files;
import java.nio.file.Paths;

public class ResourcesUtil {
    public static String readResourceFile(String file) throws Exception {
        byte[] bytes = Files.readAllBytes(Paths.get(ResourcesUtil.class.getClassLoader().getResource(file).getPath()));
        return new String(bytes);
    }
}
