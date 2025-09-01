package com.example.cep;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

@SpringBootApplication
public class CepApplicationConsole {

    public static void main(String[] args) {
        // 이 앱은 기본적으로 "direct" 프로파일로 실행
        SpringApplication app = new SpringApplication(CepApplicationConsole.class);
        app.setAdditionalProfiles("direct");
        app.run(args);
    }

    /**
     * Kafka 없이 "직접 주입" 모드의 Siddhi 앱 런타임.
     */
    @Bean(destroyMethod = "shutdown", initMethod = "start")
    @Profile("direct")  // ★ 이 빈은 direct 프로파일에서만 등록
    public SiddhiAppRuntime siddhiDirectRuntime() {
        SiddhiManager m = new SiddhiManager();

        // ── Siddhi 앱 (소스 없음: 스트림 정의 + 규칙만) ─────────────────────────────
        String app = """
        		@App:name('DirectCEP')

        		/* 1) 모든 define을 먼저 */
        		define stream LogStream (
        		    ts string,
        		    user string,
        		    event string,
        		    result string,
        		    ip string,
        		    service string
        		);
        		define stream PrintStream (ts string, user string, event string, result string, ip string, service string);
        		define stream FailAggStream (user string, failCount long);
        		define stream AlertStream (user string, failCount long, risk string);

        		/* 2) 그 다음부터 쿼리들 */
        		@info(name='passthru')
        		from LogStream
        		select ts, user, event, result, ip, service
        		insert into PrintStream;

        		@info(name='fail_agg')
        		from LogStream[result == 'fail']#window.time(30 sec)
        		select user, count() as failCount
        		group by user
        		insert into FailAggStream;

        		@info(name='alert_rule')
        		from FailAggStream[failCount >= 3]
        		select user, failCount, 'HIGH' as risk
        		insert into AlertStream;
        		""";
        // ───────────────────────────────────────────────────────────────────────

        SiddhiAppRuntime r = m.createSiddhiAppRuntime(app);

        // 보기 좋게 PrintStream/AlertStream 둘 다 콘솔 출력
        r.addCallback("PrintStream", new StreamCallback() {
            @Override public void receive(Event[] events) {
                for (Event e : events) {
                    Object[] d = e.getData();
                    System.out.printf("[EVENT] ts=%s user=%s event=%s result=%s ip=%s svc=%s%n",
                            d[0], d[1], d[2], d[3], d[4], d[5]);
                }
            }
        });
        r.addCallback("AlertStream", new StreamCallback() {
            @Override public void receive(Event[] events) {
                for (Event e : events) {
                    Object[] d = e.getData();
                    System.out.printf(">>> [ALERT] user=%s failCount=%s risk=%s%n", d[0], d[1], d[2]);
                }
            }
        });

        return r; // start()는 initMethod로 자동 호출
    }

    /**
     * 애플리케이션 기동 시 샘플 이벤트를 직접 주입하여 규칙을 검증.
     * (원하시면 주석 처리하고, 아래 주석의 예처럼 원하는 데이터를 보내세요)
     */
    @Bean(name = "pushSamplesDirect") // ★ 이름 변경
    @Profile("direct")
    public CommandLineRunner pushSamples(SiddhiAppRuntime siddhiDirectRuntime) {
        return args -> {
            InputHandler in = siddhiDirectRuntime.getInputHandler("LogStream");

            // 샘플 데이터: james 사용자가 30초 내 실패 3회 → ALERT 발생
            String now = java.time.Instant.now().toString();
            in.send(new Object[]{ now, "james", "login", "fail", "192.168.0.10", "ssh" });
            in.send(new Object[]{ now, "james", "login", "fail", "192.168.0.10", "ssh" });
            in.send(new Object[]{ now, "james", "login", "fail", "192.168.0.10", "ssh" });

            // 정상 이벤트 예시
            in.send(new Object[]{ now, "alice", "login", "success", "192.168.0.11", "ssh" });

            // ▷ 여기서 원하는 만큼 이벤트를 추가로 주입하면 됩니다.
            //   예)
            //   in.send(new Object[]{ ts, "john", "file_access", "fail", "10.0.0.1", "samba" });
        };
    }
}
