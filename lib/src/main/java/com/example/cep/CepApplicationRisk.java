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
public class CepApplicationRisk {

    public static void main(String[] args) {
        // 이 앱은 기본적으로 "risk" 프로파일로 실행
        SpringApplication app = new SpringApplication(CepApplicationRisk.class);
        app.setAdditionalProfiles("risk");
        app.run(args);
    }

    /**
     * Kafka 없이, 직접 주입으로 테스트하는 "위험 점수화" Siddhi 런타임.
     */
    @Bean(destroyMethod = "shutdown", initMethod = "start")
    @Profile("risk")  // ★ risk 프로파일에서만 활성
    public SiddhiAppRuntime siddhiRiskRuntime() {
        SiddhiManager m = new SiddhiManager();

        // 환경변수로 윈도우/임계치 조절 (기본값: 60초, MED=3, HIGH=5)
        String winSec  = System.getenv().getOrDefault("WIN_SEC",  "60");
        String highThr = System.getenv().getOrDefault("HIGH_THR","5");
        String medThr  = System.getenv().getOrDefault("MED_THR", "3");

        // ── 모든 define → 그 다음 쿼리 순서로 작성 ───────────────────────────────
        String app = """
            @App:name('DirectCEPRisk')

            /* 입력/중간/알림 스트림 정의 */
            define stream LogStream (
                ts string, user string, event string, result string, ip string, service string
            );
            define stream PrintStream (ts string, user string, event string, result string, ip string, service string);
            define stream FailAggStream (user string, failCount long);
            define stream AlertStream (user string, failCount long, risk string);

            /* 원본 이벤트 보기 좋게 그대로 출력 */
            @info(name='passthru')
            from LogStream
            select ts, user, event, result, ip, service
            insert into PrintStream;

            /* 실패 누적: 최근 %WIN%초 윈도우에서 사용자별 fail 카운트 */
            @info(name='fail_agg')
            from LogStream[result == 'fail']#window.time(%WIN% sec)
            select user, count() as failCount
            group by user
            insert into FailAggStream;

            /* 위험 등급 산정: HIGH/MEDIUM/LOW */
            @info(name='risk_scoring')
            from FailAggStream
            select user, failCount,
                   ifThenElse(failCount >= %HIGH%, 'HIGH',
                     ifThenElse(failCount >= %MED%, 'MEDIUM', 'LOW')) as risk
            insert into AlertStream;
            """
            .replace("%WIN%",  winSec)
            .replace("%HIGH%", highThr)
            .replace("%MED%",  medThr);

        SiddhiAppRuntime r = m.createSiddhiAppRuntime(app);

        // 보기 용 콘솔 출력
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
                    System.out.printf(">>> [ALERT] user=%s failCount=%s risk=%s%n",
                            d[0], d[1], d[2]);
                }
            }
        });

        return r; // initMethod="start"로 자동 start()
    }

    /**
     * 기동 시 샘플 이벤트 자동 주입 (DEMO용).
     * 필요 없으면 @Bean을 주석 처리하세요.
     */
    @Bean(name = "pushSamplesRisk") // ★ 이름 변경
    @Profile("risk")
    public CommandLineRunner pushSamples(SiddhiAppRuntime siddhiRiskRuntime) {
        return args -> {
            InputHandler in = siddhiRiskRuntime.getInputHandler("LogStream");
            String now = java.time.Instant.now().toString();

            // james: fail 3회 → 기본 MEDIUM, 이어서 5회가 되면 HIGH
            in.send(new Object[]{ now, "james", "login", "fail",    "192.168.0.10", "ssh" });
            in.send(new Object[]{ now, "james", "login", "fail",    "192.168.0.10", "ssh" });
            in.send(new Object[]{ now, "james", "login", "fail",    "192.168.0.10", "ssh" });
            in.send(new Object[]{ now, "james", "login", "fail",    "192.168.0.10", "ssh" });
            in.send(new Object[]{ now, "james", "login", "fail",    "192.168.0.10", "ssh" });

            // 성공 이벤트 한 건 (알림엔 영향 없음)
            in.send(new Object[]{ now, "alice", "login", "success", "192.168.0.11", "ssh" });
        };
    }
}