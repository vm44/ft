import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

class Time{

    public enum TimeRange{
        SECOND(19),HOUR(13),DAY(10),MONTH(7);

        TimeRange(int offset) {
            this.offset=offset;
        }
        public int offset;
    };

    public static HashMap<TimeRange,String> timeRangeStringHashMap;

    Time(){
        timeRangeStringHashMap=new HashMap<TimeRange,String>() {
            {
                put(TimeRange.SECOND, "uuuu-MM-dd'T'HH:mm:ss");
                put(TimeRange.HOUR, "uuuu-MM-dd'T'HH");
                put(TimeRange.DAY, "uuuu-MM-dd");
                put(TimeRange.MONTH, "uuuu-MM");
            }
        };
    };

    public static BiFunction<String, TimeRange, LocalDateTime> getTime = (str, tr) ->
            LocalDateTime.parse(str, DateTimeFormatter.ofPattern(Time.timeRangeStringHashMap.get(tr)));
}

@Parameters(commandNames = "filter", commandDescription = "Filtering input content")
class Filtering {

    @Parameter(names = { "-u", "--user"}, description = "user name")
    String user = "";

    @Parameter(names = { "-p", "--pattern"}, description = "message pattern")
    String pattern = "";

    @Parameter(names = { "-f", "--from"}, description = "from time")
    String stTime = "";

    @Parameter(names = { "-t", "--to"}, description = "to time")
    String endTime = "";
}


class FilteringPredicates{

    public static Predicate<LogRecord> testUser(String user) {
        return x -> x.getUser().contains(user);}

    public static Predicate<LogRecord> testMsg(String msg) {
        return x -> x.getMsg().contains(msg);}

    public static Predicate<LogRecord> testTimeAfter(LocalDateTime stTime) {
        return x -> x.getDateTime(Time.TimeRange.SECOND).isAfter(stTime);
    }
    public static Predicate<LogRecord> testTimeBefore(LocalDateTime endTime) {
        return x -> x.getDateTime(Time.TimeRange.SECOND).isBefore(endTime);
    }


    public List<Predicate<LogRecord>> predicates = new ArrayList<>();

    FilteringPredicates(Filtering filtering) {
//        System.out.println(Time.getTime.apply(filtering.stTime,Time.TimeRange.HOUR));
        if(!filtering.user.isEmpty())predicates.add(testUser(filtering.user));
        if(!filtering.pattern.isEmpty())predicates.add(testMsg(filtering.pattern));
        if(!filtering.stTime.isEmpty())predicates.add(testTimeAfter(Time.getTime.apply(filtering.stTime,Time.TimeRange.SECOND)));
        if(!filtering.endTime.isEmpty())predicates.add(testTimeBefore(Time.getTime.apply(filtering.endTime,Time.TimeRange.SECOND)));
    }

}

class LogRecord{

    String logRecord;
    private int[] delimiterPos;

    LogRecord(String logRecord){
        this.logRecord=logRecord;
        delimiterPos = new int[4];
        delimiterPos[1]=logRecord.indexOf(' ');
        delimiterPos[2]=logRecord.indexOf(' ',delimiterPos[1]+1);
    }

    public LocalDateTime getDateTime(Time.TimeRange timeRange) {
        String timeStr = logRecord.substring(0, delimiterPos[1]);
        DateTimeFormatter dateFormat = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        return LocalDateTime.parse(timeStr, dateFormat);
    }

    public String getDateTimeAsString(Time.TimeRange timeRange){
        String timeStr=logRecord.substring(0, delimiterPos[1]);
        return  timeStr.substring(0,timeRange.offset);
    }

    public String getUser(){
        return logRecord.substring(delimiterPos[1], delimiterPos[2]);
    }

    public String getMsg(){
        return logRecord.substring(delimiterPos[2]);
    }
}


class FileFilter implements Callable<Optional<List<LogRecord>>> {

    private Path path;
    private FilteringPredicates lineFilter;

    FileFilter(Path path, FilteringPredicates lineFilter){
        this.path=path;
        this.lineFilter = lineFilter;
    }

    @Override
    public Optional<List<LogRecord>> call() throws Exception{
        try (Stream<String> content = Files.lines(path)){
            return Optional.of(content
                    .map(LogRecord::new)
                    .filter(t -> lineFilter.predicates.stream().allMatch(f -> f.test(t)))
                    .collect(Collectors.toList()));
        }catch (IOException e){
            e.printStackTrace();
        }
        return Optional.empty();
    }
}


class Main {
    @Parameter(names={"--threads", "-t"})
    int threads=1;
    @Parameter(names={"--output", "-o"})
    String outFilePath;
    @Parameter(names={"--time", "-a"})
    String groupByTime;
    @Parameter(names={"--user", "-u"})
    String groupByUser;
    @Parameter(names = { "-h", "--help"}, description = "Print this help message and exit", help = true)
    private boolean help;

    Time time=new Time();
    private Filtering filtering = new Filtering();

    public static void main(String ... argv) {
        Main main = new Main(argv);
        main.run();
        System.exit(0);
    }

    private Main(String[] args) {
        JCommander jc = new JCommander(this);
        jc.addCommand("filter", filtering);
//        jc.addCommand("group",grouping);
//        jc.addCommand("test",test);

        try {
            jc.parse(args);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            jc.usage();
            System.exit(1);
        }

        if (help || jc.getParsedCommand() == null) {
            jc.usage();
            System.exit(0);
        }
    }

    private void run() {

        File file=new File(".");
        String currentWorkingDir=file.getAbsolutePath();
        System.out.println("CWD: " + currentWorkingDir);

        try {
            List<Path> fileList=
                    Files.list(Paths.get(currentWorkingDir))
                    .filter(Files::isRegularFile)
                    .filter(name -> name.toString().endsWith("log"))
                    .collect(Collectors.toList());

            ThreadPoolExecutor executor=(ThreadPoolExecutor) Executors.newFixedThreadPool(threads);

            List<Future<Optional<List<LogRecord>>>> filteredFiles =
                    fileList.stream()
                    .map(path -> executor.submit(new FileFilter(path, new FilteringPredicates(filtering))))
                    .collect(Collectors.toList());

            Path outPath = Paths.get(outFilePath);
            List<LogRecord> outList=new ArrayList<>();

//            List<ArrayList<String>> agg=new ArrayList<>();
//            Consumer<ArrayList<String>> appendToAgg = x -> agg.add(x);

            try (BufferedWriter writer = Files.newBufferedWriter(outPath)) {
                for (Future<Optional<List<LogRecord>>> future : filteredFiles) {
                    try {
                        Optional<List<LogRecord>> lst = future.get();
                        lst.ifPresent(outList::addAll);
                        lst.ifPresent(x -> {
                            x.forEach(System.out::println);
                            x.forEach(s ->{
                                try{
                                    writer.write('\n'+s.logRecord);
                                }catch (IOException e){
                                    e.printStackTrace();
                                }});
                        });
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }catch (IOException e){
                e.printStackTrace();
            }

            HashMap<String,Time.TimeRange> trMap=
                    new HashMap<String,Time.TimeRange>(){
                    {
                        put("h",Time.TimeRange.HOUR);
                        put("d",Time.TimeRange.DAY);
                        put("m",Time.TimeRange.MONTH);
                }};


            trMap.forEach((k,v)->{
                System.out.println();
                outList.stream().collect(groupingBy(x->x.getDateTimeAsString(trMap.get(k)),counting()))
                        .forEach((k1,v1)->System.out.printf("\n%s %d",k1,v1));
            });

            if(!groupByTime.isEmpty()){
                Time.TimeRange timeRange=trMap.get(groupByTime);
                Map<String, Long> groupedByTime=outList.stream().collect(groupingBy(x->x.getDateTimeAsString(timeRange),counting()));
                groupedByTime.forEach((k,v)->System.out.printf("%s %d",k,v));

            }

            FilteringPredicates fp=new FilteringPredicates(filtering);
            List<LogRecord> logRecords=outList;//.stream().map(LogRecord::new).collect(Collectors.toList());
            outList.forEach(x->System.out.println(x.getDateTimeAsString(Time.TimeRange.SECOND)));
            List<LogRecord> l3=logRecords.stream().filter(t -> fp.predicates.stream().allMatch(f -> f.test(t))).collect(toList());


//            Map<String, List<LogRecord>> l1=logRecords.stream().collect(groupingBy(LogRecord::getUser));
//            Map<String, Long> l2=logRecords.stream().collect(groupingBy(LogRecord::getUser,counting()));
//            Map<String, Long> tm2=logRecords.stream().collect(groupingBy(x->x.getDateTimeAsString(Time.TimeRange.HOUR),counting()));
//            Map<String, Long> tm3=logRecords.stream().collect(groupingBy(x->x.getDateTimeAsString(Time.TimeRange.DAY),counting()));
//            Map<String, Long> tm4=logRecords.stream().collect(groupingBy(x->x.getDateTimeAsString(Time.TimeRange.MONTH),counting()));
//            tm2.forEach((k,v) -> {System.out.print(k);System.out.println(v);});


//            outList.forEach(System.out::println);
//            outList.stream().map(x->x.getDateTimeAsString(Time.TimeRange.HOUR)).forEach(System.out::println);




        } catch (IOException e) {
            e.printStackTrace();
        }

    }



}


