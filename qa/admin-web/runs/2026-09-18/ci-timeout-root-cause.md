# Web CI 타임아웃 근본 원인 — 2026-09-18

## 관측된 실패

| 항목 | PR #802 | PR #803 |
|---|---|---|
| run / job | 35263994593 / 105346444454 | 35264193825 / 105347115912 |
| head | 970ff5d7a03a33d50fcf6a1c8ee3961cea5ec90d | 7b7a7fc06373da23c5ac323b572060068338db92 |
| runner pod | cc-lb-1-nzzpb-runner-8stjg | cc-lb-1-nzzpb-runner-6w2gl |
| 배치 노드 | rpi4 (Cortex-A72 1.5GHz) | rock5bp (RK3588 Cortex-A76 2.4GHz) |
| vitest 소요 | 1216.87s | 485.56s |
| 실패 | 8개 파일 20건 전부 `Test timed out in 5000ms` | 1개 파일 2건 전부 동일 |
| CFS throttled periods | +7342 / 14193 periods | +3986 / 5461 periods |

명령은 두 실행 모두 동일하다: `bunx --bun vitest run --passWithNoTests --maxWorkers=1 --no-file-parallelism`.

## 확정된 기여 원인

`bun-checks`는 `cc-lb-1`에서 실행되고, 해당 AutoscalingRunnerSet의 컨테이너 자원은 `{"limits":{"cpu":1,"memory":"2Gi"},"requests":{"cpu":1,"memory":"2Gi"}}`이다. vitest는 단일 worker지만 bun 런타임, GC, jsdom 타이머가 함께 1코어를 넘겨 요구하므로 cgroup이 대부분의 스케줄링 주기에서 throttle된다. 그 결과 개별 테스트의 wall clock이 5000ms 예산을 넘는다.

고정 이미지(`sha256:c2f09887…`), 동일 CI 명령, PR #802의 web 트리(`970ff5d7`)로 측정한 quota 곡선:

| quota | vitest wall | throttled | throttled periods | 결과 |
|---|---|---|---|---|
| 1 core | 170.6s | 156.6s | 1314/1707 (77%) | 775 passed |
| 2 cores | 70.5s | 20.2s | 39% | 775 passed |
| 4 cores | 59.0s | 0.8s | 3% | 775 passed |

같은 quota에서 4코어 대비 1코어는 wall clock이 2.9배로 늘어난다. CPU 사용량은 4코어에서 75.8s이고 wall은 59.0s이므로 평균 요구 병렬성은 약 1.3이고 순간 요구는 2를 넘는다.

## 별개의 인프라 결함

러너 이미지는 busybox tar(1.38.0)만 제공하고 `-P`를 거부한다. `actions/cache`가 `-P`를 넘기므로 Bun 캐시 restore와 save가 매 실행 실패한다(#802 `Cache Bun install store` 49s 소요 후 미스, `bun install` 66s). `gnutar` 설치 시 `/usr/bin/tar`는 GNU tar 1.35.90이 되고 `-P`를 받는다.

## 적용한 수정

PR #805 (`fix/ci-web-runner-capacity`, 커밋 `1c2f8636`):
- `.github/workflows/web.yml`: `runs-on: cc-lb-1` → `cc-lb-2`, 측정 근거를 주석으로 기록.
- `.github/runner-image/Dockerfile`: `gnutar` 추가와 빌드 시 GNU tar 단정.

테스트 삭제, `#[ignore]` 류 우회, testTimeout 완화, 실패 재실행은 하지 않았다.

## 검증

- 러너 패키지 레이어 빌드 성공, 빌드 내 GNU tar 단정 통과.
- `actionlint .github/workflows/web.yml` 통과.
- `hadolint`는 현재 master 파일 대비 새 지적 없음.
- 위 quota 3회는 고정 이미지 안에서 실제 CI 명령으로 실행.

## 한계

- quota 곡선은 arm64 워크스테이션에서 측정했고 rpi4/rock5bp 노드 자체는 아니다. 해당 노드의 절대 시간은 더 느리다.
- 이미지 변경은 러너 이미지 재발행과 CI가 참조하는 digest 갱신 후에 효력이 생긴다.
- `cc-lb-1` 사용을 유지하는 `cargo-deny`는 이번 범위가 아니다.
- 2코어에서도 주기 39%는 여전히 throttle된다. 노드 성능 차이를 없애려면 별도 배치 정책이 필요하고, 그것은 클러스터 변경이므로 이번에 적용하지 않았다.
