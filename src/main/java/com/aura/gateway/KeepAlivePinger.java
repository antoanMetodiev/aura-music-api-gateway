package com.aura.gateway;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Keeps the free-tier deployment alive.
 *
 * <ul>
 *   <li>Render spins a free web service down after 15 minutes without inbound traffic, so every
 *       service — the gateway itself included, through its public URL — gets a cheap liveness
 *       ping well inside that window. Liveness does not touch the database.</li>
 *   <li>Supabase pauses a free project after a week without activity, so every 12 hours each
 *       service's full {@code /actuator/health} runs, which includes its database check.</li>
 * </ul>
 *
 * Failures are only logged: a sleeping or redeploying service must never break the gateway.
 */
@Component
class KeepAlivePinger {

    private static final Logger log = LoggerFactory.getLogger(KeepAlivePinger.class);

    // A cold start on Render takes up to about a minute, so wait longer than that.
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private final WebClient webClient;
    private final List<String> serviceUrls;
    private final List<String> allUrls;
    private final boolean enabled;

    KeepAlivePinger(
                    @Value("${keepalive.enabled:false}") boolean enabled,
                    @Value("${keepalive.self-url:}") String selfUrl,
                    @Value("${keepalive.service-urls:}") List<String> serviceUrls) {
        this.webClient = WebClient.create();
        this.enabled = enabled;
        this.serviceUrls = serviceUrls.stream().map(KeepAlivePinger::trimSlash).filter(u -> !u.isBlank()).toList();
        List<String> all = new ArrayList<>(this.serviceUrls);
        if (!selfUrl.isBlank()) {
            all.add(trimSlash(selfUrl));
        }
        this.allUrls = List.copyOf(all);
    }

    @Scheduled(initialDelayString = "${keepalive.services-initial-delay:PT1M}",
               fixedDelayString = "${keepalive.services-interval:PT10M}")
    void pingServices() {
        ping(allUrls, "/actuator/health/liveness", "keep-alive");
    }

    @Scheduled(initialDelayString = "${keepalive.database-initial-delay:PT2M}",
               fixedDelayString = "${keepalive.database-interval:PT12H}")
    void pingDatabase() {
        ping(serviceUrls, "/actuator/health", "database");
    }

    private void ping(List<String> baseUrls, String path, String kind) {
        if (!enabled || baseUrls.isEmpty()) {
            return;
        }
        Flux.fromIterable(baseUrls)
                .flatMap(base -> webClient.get().uri(base + path)
                        .retrieve()
                        .toBodilessEntity()
                        .timeout(TIMEOUT)
                        .doOnNext(r -> log.info("{} ping {}{} -> {}", kind, base, path, r.getStatusCode().value()))
                        .onErrorResume(e -> {
                            log.warn("{} ping {}{} failed: {}", kind, base, path, e.toString());
                            return Mono.empty();
                        }))
                .subscribe();
    }

    private static String trimSlash(String url) {
        String u = url.trim();
        return u.endsWith("/") ? u.substring(0, u.length() - 1) : u;
    }
}
