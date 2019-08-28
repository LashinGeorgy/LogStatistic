import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StatisticUtil {
    private static final String ERROR_TYPE = "ERROR";
    private static final String HOURLY_INTERVAL = "h";
    private static final String SPLITTER = ";";
    private static final String STATISTICS_FILE_NAME = "/Statistics";
    private static final Integer DATE = 0;
    private static final Integer TYPE = 1;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    public static void buildStatistic(String directoryPath, String interval) {
        try (Stream<Path> paths = Files.walk(Paths.get(directoryPath));
             FileOutputStream outputStream = new FileOutputStream(directoryPath + STATISTICS_FILE_NAME)) {
            paths.parallel()
                    .filter(Files::isRegularFile)
                    .flatMap(filePath -> collectErrorTime(filePath, interval))
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                    .forEach((localDateTime, count) -> {
                        String line = localDateTime + " ERROR count: " + count + "\n";
                        try {
                            outputStream.write(line.getBytes());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Stream<LocalDateTime> collectErrorTime(Path filePath, String interval) {
        List<LocalDateTime> errorTimeList = new ArrayList<>();
        try (Stream<String> stream = Files.lines(filePath)) {
            errorTimeList = stream.map(entry -> entry.split(SPLITTER))
                    .filter(entry -> entry[TYPE].equals(ERROR_TYPE))
                    .map(entry -> {
                        LocalDateTime date = LocalDateTime.parse(entry[DATE], FORMATTER);
                        if (interval.equals(HOURLY_INTERVAL))
                            date = date.minusMinutes(date.getMinute());
                        return date;
                    }).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return errorTimeList.stream();
    }
}
