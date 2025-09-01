package com.example.cep;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.StreamCallback;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

@SpringBootApplication
public class CepApplication {

    public static void main(String[] args) {
    	// 이 앱은 기본적으로 "kafka" 프로파일로 실행
        SpringApplication app = new SpringApplication(CepApplication.class);
        app.setAdditionalProfiles("kafka");
        app.run(args);
    }

    @Bean(destroyMethod = "shutdown", initMethod = "start")
    @Profile("kafka")   // ★ 이 빈은 kafka 프로파일에서만 등록
    public SiddhiAppRuntime siddhiAppRuntime() {
        SiddhiManager manager = new SiddhiManager();

        String app = """
            @App:name('FilebeatKafkaCEP')
            @App:description('Read Filebeat JSON from Kafka and print')

            @source(type='kafka',
                    topic.list='syslog-topic',
                    group.id='siddhi-cepsvc',
                    bootstrap.servers='127.0.0.1:9092',
                    threading.option='single.thread',
                    @map(type='json', fail.on.missing.attribute='false',
                         @attributes(
                             message='$.message',
                             host='$.host.name',
                             log_path='$.log.file.path',
                             level='$.log.level',
                             ts='$.@timestamp'
                         )
                    )
            )
            define stream FilebeatStream (
                message string, host string, log_path string, level string, ts string
            );

            define stream PrintStream (ts string, level string, host string, message string);

            @info(name='passthru')
            from FilebeatStream
            select ts, level, host, message
            insert into PrintStream;
            """;

        SiddhiAppRuntime runtime = manager.createSiddhiAppRuntime(app);

        // 요청사항: System.out.println 출력
        runtime.addCallback("PrintStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                for (Event e : events) {
                    Object[] d = e.getData();
                    System.out.println(String.format(
                        "[CEP] ts=%s level=%s host=%s msg=%s",
                        d[0], d[1], d[2], d[3]
                    ));
                }
            }
        });

        return runtime; // initMethod="start"가 runtime.start()를 호출합니다.
    }
}
