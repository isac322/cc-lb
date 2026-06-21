# BDD Scenario English Translation Queue

This queue lists all 321 BDD v5.2 scenarios with their Korean source titles and target English title slots for translation.

## Invariant Statement
The total number of scenarios in this queue is exactly 321. Every milestone gate must satisfy the following equation:
`pending + OoS-manual == 321`
Currently, there are 319 pending scenarios and 2 OoS-manual scenarios.

## Source Language Policy
All step text, titles, descriptions, and panic messages must be written in English. Korean text is only allowed as a backlink doc comment in the generated Rust test code, formatted exactly as:
`/// Original (Korean human-readable report): <path>#L<from>-L<to>`

## Domain Vocabulary Allowed List
Only the following domain-specific terms are allowed in the translated step text and titles:
- upstream
- warmup
- replica
- callback
- readyz
- drain
- sweep
- lease
- token
- quota
- principal
- audit
- signer
- dialect
- plugin
- chain
- terminal
- filter
- shape
- observability

## Forbidden Rust Jargon Summary
Do not use any implementation-level Rust jargon in the step text. This restriction is enforced by a build-time lint check. The forbidden terms include:
- Types: Vec, Arc, Result, HashMap, BTreeMap, BTreeSet, Option, etc.
- Async runtime: tokio::, await, JoinHandle, JoinSet, select!, join!, mpsc, oneshot, broadcast, Semaphore, Runtime, block_on, sleep, task::yield_now, spawn_blocking, etc.
- Database: sqlx, SELECT, Transaction, PgPool, SqlitePool, Migrator, etc.
- HTTP: axum, tower, hyper, http, Json(, Response::, Request::, Method::, Uri, etc.
- Serialization: serde, serde_json::Value, Deserialize, Serialize, json!, etc.
- Identifiers and errors: uuid, Uuid, unwrap, ?, ?;, panic!, bail!, ensure!, Err(, thiserror::, etc.
- Signatures and syntax: pub fn, pub async fn, pub struct, pub enum, impl, where, 'a, 'static, etc.

## Writer W1 (team/traffic/dashboard/cache/health)

| ID | Persona | fn_name | Korean source title (verbatim from v5.2) | Source file#Lrange | Target EN title | Status |
|---|---|---|---|---|---|---|
| F1.1a | Alice | `f1_1a` | F1.1a: 새 팀 등록은 활성 상태와 첫 키를 같은 화면에서 한 번에 마무리한다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L29-L38](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L29-L38) | (pending) | pending |
| F1.1b | Alice | `f1_1b` | F1.1b: 새 팀 등록은 누가 언제 만들었는지를 감사 기록에 남긴다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L39-L47](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L39-L47) | (pending) | pending |
| F1.2 | Alice | `f1_2` | F1.2: 발급된 키는 등록 직후 한 번만 보여진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L48-L57](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L48-L57) | (pending) | pending |
| F1.3 | Alice | `f1_3` | F1.3: 다른 운영자가 그 사이 같은 팀을 바꿨다면 두 번째 저장은 멈춰진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L58-L67](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L58-L67) | (pending) | pending |
| F1.4 | Alice | `f1_4` | F1.4: 비활성화된 팀의 호출은 받아들여지지 않는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L68-L77](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L68-L77) | (pending) | pending |
| F1.5 | Alice | `f1_5` | F1.5: 팀에 허용된 모델 범위를 벗어나는 호출은 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L78-L87](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L78-L87) | (pending) | pending |
| F1.7 | Alice | `f1_7` | F1.7: 팀을 삭제해도 그 팀이 했던 일의 감사 흔적은 사라지지 않는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L88-L97](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L88-L97) | (pending) | pending |
| F1.8 | Alice | `f1_8` | F1.8: 식별자에 금지 문자나 예약된 머리말이 들어가면 등록이 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L98-L107](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L98-L107) | (pending) | pending |
| F1.10 | Alice | `f1_10` | F1.10: 비활성 팀을 다시 활성으로 되돌리면 같은 비밀로 곧 받아들여진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L108-L117](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L108-L117) | (pending) | pending |
| F1.11 | Alice | `f1_11` | F1.11: 비활성화 직전에 시작된 호출은 마무리되고 새 호출만 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L118-L133](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L118-L133) | (pending) | pending |
| F2.1 | Alice | `f2_1` | F2.1: 새 키 발급 시 비밀은 단 한 번만 보여진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L134-L143](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L134-L143) | (pending) | pending |
| F2.2 | Alice | `f2_2` | F2.2: 키 회전 시 새 키와 옛 키가 짧은 겹침 기간을 갖는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L144-L153](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L144-L153) | (pending) | pending |
| F2.3 | Alice | `f2_3` | F2.3: 키 회수 즉시 다음 호출부터 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L154-L163](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L154-L163) | (pending) | pending |
| F2.4a | Alice | `f2_4a` | F2.4a: 정해진 시점에 자동으로 만료되는 키는 그 시점에 끊긴다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L164-L172](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L164-L172) | (pending) | pending |
| F2.4b | Alice | `f2_4b` | F2.4b: 만료 임박 알림이 운영자에게 미리 도착한다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L173-L181](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L173-L181) | (pending) | pending |
| F2.5 | Alice | `f2_5` | F2.5: 키 목록 화면에는 비밀이 결코 함께 보이지 않는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L182-L191](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L182-L191) | (pending) | pending |
| F2.6 | Alice | `f2_6` | F2.6: 키 보유자 이름과 메모를 운영자가 나중에 고칠 수 있다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L192-L201](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L192-L201) | (pending) | pending |
| F2.8a | Alice | `f2_8a` | F2.8a: 사용량 보고는 키 보유자 단위로 호출 수와 거부 사유를 나눠 보여준다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L202-L210](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L202-L210) | (pending) | pending |
| F2.8b | Alice | `f2_8b` | F2.8b: 보유자 이름을 고치면 옛 사용량도 새 이름으로 통일되어 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L211-L219](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L211-L219) | (pending) | pending |
| F2.10 | Alice | `f2_10` | F2.10: 한 보유자가 동시에 가질 수 있는 활성 키 수가 정해져 있다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L220-L229](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L220-L229) | (pending) | pending |
| F2.11 | Alice | `f2_11` | F2.11: 키의 마지막 몇 자리를 다시 들여다본 사실도 감사 대상이 된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L230-L239](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L230-L239) | (pending) | pending |
| F2.12a | Alice | `f2_12a` | F2.12a: 키 보유자가 사람인지 머신인지가 목록 화면에서 한눈에 구분된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L240-L248](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L240-L248) | (pending) | pending |
| F2.12b | Alice | `f2_12b` | F2.12b: 사용량 보고에서도 사람/머신 단위가 따로 합산되어 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L249-L257](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L249-L257) | (pending) | pending |
| F2.13a | Alice | `f2_13a` | F2.13a: 키 상태는 활성→일시중지→회수의 순서로 다뤄진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L258-L266](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L258-L266) | (pending) | pending |
| F2.13b | Alice | `f2_13b` | F2.13b: 키 상태 전이마다 누가 언제 바꿨는지가 감사 기록에 남는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L267-L275](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L267-L275) | (pending) | pending |
| F2.14 | Alice | `f2_14` | F2.14: 회전을 지원하지 않는 저장 백엔드에서 키 회전을 시도하면 친절히 거부된다 (신규 v5) | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L276-L292](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L276-L292) | (pending) | pending |
| F3.1 | Bob | `f3_1` | F3.1: 정상 키로 정상 모델을 부르면 응답이 그대로 도착한다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L293-L302](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L293-L302) | (pending) | pending |
| F3.2 | Bob | `f3_2` | F3.2: 스트리밍 응답이 끊김 없이 끝까지 흐른다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L303-L312](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L303-L312) | (pending) | pending |
| F3.3 | Bob | `f3_3` | F3.3: 알 수 없는 키로 보낸 호출은 친절히 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L313-L322](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L313-L322) | (pending) | pending |
| F3.4 | Bob | `f3_4` | F3.4: 비활성 팀의 키로 보낸 호출은 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L323-L332](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L323-L332) | (pending) | pending |
| F3.5a | Bob | `f3_5a` | F3.5a: 자기 팀에 허용되지 않은 모델을 부르면 그 호출이 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L333-L340](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L333-L340) | (pending) | pending |
| F3.5b | Bob | `f3_5b` | F3.5b: 허용되지 않은 모델 거부 메시지는 Claude 형식의 오류 봉투로 전달된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L341-L349](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L341-L349) | (pending) | pending |
| F3.5c | Bob | `f3_5c` | F3.5c: 허용되지 않은 모델 거부가 사용량 보고와 감사 묶음에 함께 더해진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L350-L358](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L350-L358) | (pending) | pending |
| F3.8 | Bob | `f3_8` | F3.8: 응답 도중에 cc-lb 안에서 갑작스런 문제가 생기면 Claude 형식의 오류로 마감된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L359-L368](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L359-L368) | (pending) | pending |
| F3.9 | Bob | `f3_9` | F3.9: Bob은 파일을 올리고 받고 지우는 흐름을 cc-lb를 통해 마무리한다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L369-L378](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L369-L378) | (pending) | pending |
| F3.11a | Bob | `f3_11a` | F3.11a: 알 수 없는 경로나 알 수 없는 동작으로 보낸 호출은 Claude 형식의 오류 봉투로 돌아온다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L379-L387](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L379-L387) | (pending) | pending |
| F3.11b | Bob | `f3_11b` | F3.11b: 알 수 없는 경로/동작의 거부가 사용량 보고의 거부 사유 종류별에 더해진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L388-L396](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L388-L396) | (pending) | pending |
| F3.12 | Bob | `f3_12` | F3.12: 한도로 거부될 때 "언제 다시 시도하면 되는지"가 같은 응답에 들어 있다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L397-L406](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L397-L406) | (pending) | pending |
| F3.13 | Bob | `f3_13` | F3.13: 응답 본문이 미리 정한 상한을 넘기면 친절히 끊긴다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L407-L416](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L407-L416) | (pending) | pending |
| F3.14 | Bob | `f3_14` | F3.14: 등록된 통로 외의 외부 호스트로 곧장 나가려 하면 막힌다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L417-L426](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L417-L426) | (pending) | pending |
| F3.15 | Bob | `f3_15` | F3.15: 한 단계만 머무는 전달 표식들은 응답 경계에서 정리된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L427-L436](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L427-L436) | (pending) | pending |
| F3.16 | Bob | `f3_16` | F3.16: 격리 시험 환경에서 인증을 임시로 끈 사실은 분명히 표시된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L437-L452](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L437-L452) | (pending) | pending |
| F4.1a | Alice | `f4_1a` | F4.1a: 어제 가장 많이 쓴 팀이 비용 순서로 한눈에 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L453-L461](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L453-L461) | (pending) | pending |
| F4.1b | Alice | `f4_1b` | F4.1b: 같은 표에 호출 수 대 거부 비율이 함께 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L462-L470](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L462-L470) | (pending) | pending |
| F4.1c | Alice | `f4_1c` | F4.1c: 한 팀 줄을 누르면 그 팀의 자세한 보기로 이어진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L471-L479](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L471-L479) | (pending) | OoS-manual |
| F4.2 | Alice | `f4_2` | F4.2: 기간·팀·키 보유자·모델 중 골라서 좁혀 본다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L480-L489](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L480-L489) | (pending) | pending |
| F4.3 | Alice | `f4_3` | F4.3: 한 달 비용이 정해진 한도에 가까워지면 같은 화면에서 미리 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L490-L499](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L490-L499) | (pending) | pending |
| F4.4a | Alice | `f4_4a` | F4.4a: 모델별 호출 점유율과 비용 점유율이 같은 화면에서 한눈에 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L500-L508](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L500-L508) | (pending) | pending |
| F4.4b | Alice | `f4_4b` | F4.4b: 모델 점유율 표에서 한 모델을 누르면 시간대별 흐름으로 이어진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L509-L517](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L509-L517) | (pending) | pending |
| F4.5 | Alice | `f4_5` | F4.5: 시간대별 사용 흐름이 끊김 없이 이어진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L518-L527](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L518-L527) | (pending) | pending |
| F4.6 | Alice | `f4_6` | F4.6: 거부 사유가 종류별로 한눈에 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L528-L537](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L528-L537) | (pending) | pending |
| F4.7 | Alice | `f4_7` | F4.7: 보고 화면을 표 형식으로 내려받을 수 있다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L538-L547](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L538-L547) | (pending) | pending |
| F4.8 | Alice | `f4_8` | F4.8: 실시간 흐름이 끊겼다는 사실이 같은 화면에서 분명히 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L548-L557](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L548-L557) | (pending) | pending |
| F4.10 | Alice | `f4_10` | F4.10: 로그 페이지에서 종류·팀·보유자를 골라 좁혀 본다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L558-L567](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L558-L567) | (pending) | pending |
| F4.11a | Alice | `f4_11a` | F4.11a: "0건"과 "아직 모름"이 같은 표 안에서 분명히 구분된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L568-L576](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L568-L576) | (pending) | pending |
| F4.11b | Alice | `f4_11b` | F4.11b: 두 표식의 의미가 사용자 인지 도움말로 함께 안내된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L577-L585](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L577-L585) | (pending) | OoS-manual |
| F4.11c | Alice | `f4_11c` | F4.11c: 같은 구분이 시간대별 흐름 그림 위에도 그대로 표시된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L586-L594](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L586-L594) | (pending) | pending |
| F4.12 | Alice | `f4_12` | F4.12: 한 호출의 단계별 머문 시간을 자세히 본다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L595-L610](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L595-L610) | (pending) | pending |
| F6.1 | Alice | `f6_1` | F6.1: 분당 한도를 넘으면 그 팀의 다음 호출이 잠시 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L611-L620](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L611-L620) | (pending) | pending |
| F6.2a | Alice | `f6_2a` | F6.2a: 일일 한도가 차면 그날의 나머지 호출이 거부되고 다음 날 같은 시각에 다시 비워진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L621-L629](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L621-L629) | (pending) | pending |
| F6.2b | Alice | `f6_2b` | F6.2b: 일일 한도 거부 안내에는 한도가 다시 비워지는 시점이 들어 있다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L630-L638](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L630-L638) | (pending) | pending |
| F6.3 | Alice | `f6_3` | F6.3: 월 비용 한도가 가까워지면 새 호출이 미리 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L639-L648](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L639-L648) | (pending) | pending |
| F6.4 | Alice | `f6_4` | F6.4: 허용 모델 묶음 밖으로 부르면 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L649-L658](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L649-L658) | (pending) | pending |
| F6.5 | Alice | `f6_5` | F6.5: 한도 편집 직후의 호출에 새 한도가 곧 반영된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L659-L668](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L659-L668) | (pending) | pending |
| F6.8 | Alice | `f6_8` | F6.8: 한도 위반이 거부 사유 종류별 보고에서 따로 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L669-L678](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L669-L678) | (pending) | pending |
| F6.9 | Alice | `f6_9` | F6.9: 외부 호스트 허용 목록 밖의 곳으로는 cc-lb가 나가지 않는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L679-L688](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L679-L688) | (pending) | pending |
| F6.10a | Alice | `f6_10a` | F6.10a: 특정 경로의 본문 크기 한도가 그 경로의 호출에 적용된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L689-L697](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L689-L697) | (pending) | pending |
| F6.10b | Alice | `f6_10b` | F6.10b: 한 경로의 본문 한도 변경이 다른 경로의 본문 한도에 영향을 주지 않는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L698-L706](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L698-L706) | (pending) | pending |
| F6.12 | Alice | `f6_12` | F6.12: 한도 위반으로 잠시 거부되던 팀이 시간이 지나 회복한다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L707-L716](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L707-L716) | (pending) | pending |
| F6.13 | Alice | `f6_13` | F6.13: 일일 한도를 다 쓴 팀은 다음 날 같은 시각에 자동으로 다시 받아들여진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L717-L726](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L717-L726) | (pending) | pending |
| F6.14a | Alice | `f6_14a` | F6.14a: 한 팀의 분당 한도 위반이 다른 팀의 호출 받아들임에 영향을 주지 않는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L727-L735](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L727-L735) | (pending) | pending |
| F6.14b | Alice | `f6_14b` | F6.14b: 한 팀의 폭주가 다른 팀의 회복 시점을 늦추지 않는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L736-L744](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L736-L744) | (pending) | pending |
| F6.15 | Alice | `f6_15` | F6.15: 동시에 처리 중인 호출 수에 따로 한도를 정할 수 있다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L745-L754](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L745-L754) | (pending) | pending |
| F6.16 | Alice | `f6_16` | F6.16: 한도가 "팀"인지 "보유자"인지 "키"인지에 따라 적용 단위가 달라진다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L755-L764](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L755-L764) | (pending) | pending |
| F6.17 | Alice | `f6_17` | F6.17: 한 키에만 더 작은 한도를 따로 정해 둘 수 있다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L765-L780](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L765-L780) | (pending) | pending |
| F19.1 | Alice | `f19_1` | F19.1: 같은 의미의 호출이 두 번째에는 같은 곳으로 모인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L781-L790](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L781-L790) | (pending) | pending |
| F19.2 | Alice | `f19_2` | F19.2: 캐시 적중률이 같은 화면에서 한눈에 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L791-L800](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L791-L800) | (pending) | pending |
| F19.3a | Alice | `f19_3a` | F19.3a: 캐시 적중으로 아낀 비용이 비용 보고에 따로 표시된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L801-L809](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L801-L809) | (pending) | pending |
| F19.3b | Alice | `f19_3b` | F19.3b: "캐시 없이라면 들었을 비용"이 가상 비용 라벨로 함께 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L810-L818](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L810-L818) | (pending) | pending |
| F19.3c | Alice | `f19_3c` | F19.3c: 실제 비용과 가상 비용이 시간대별 흐름으로 비교되어 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L819-L827](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L819-L827) | (pending) | pending |
| F19.4 | Alice | `f19_4` | F19.4: 캐시 친화가 끊기는 경우에는 새로 처리된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L828-L837](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L828-L837) | (pending) | pending |
| F19.5 | Alice | `f19_5` | F19.5: 캐시 친화는 팀 경계를 넘지 않는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L838-L847](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L838-L847) | (pending) | pending |
| F19.6a | Alice | `f19_6a` | F19.6a: 캐시 친화가 작동하는 동안에도 한도와 모델 묶음 위반은 그대로 거부된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L848-L856](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L848-L856) | (pending) | pending |
| F19.6b | Alice | `f19_6b` | F19.6b: 캐시 적중률은 거부된 호출을 빼고 계산된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L857-L865](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L857-L865) | (pending) | pending |
| F19.7 | Alice | `f19_7` | F19.7: 짧게 유지되는 캐시와 길게 유지되는 캐시가 등급으로 분리되어 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L866-L875](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L866-L875) | (pending) | pending |
| F19.8 | Alice | `f19_8` | F19.8: 비활성 팀의 호출은 캐시 적중을 통해서도 받아들여지지 않는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L876-L885](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L876-L885) | (pending) | pending |
| F19.9 | Alice | `f19_9` | F19.9: 한 호출의 캐시 상태가 분명한 분류로 한눈에 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L886-L895](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L886-L895) | (pending) | pending |
| F19.10 | Alice | `f19_10` | F19.10: 캐시 적중인데 비용 단위가 처음과 어긋난 경우가 따로 표시된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L896-L911](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L896-L911) | (pending) | pending |
| F26.1 | Charlie | `f26_1` | F26.1: 살아 있음과 처리 가능이 분리되어 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L912-L921](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L912-L921) | (pending) | pending |
| F26.2a | Charlie | `f26_2a` | F26.2a: 처리 가능 상태가 회복 불가일 때는 계속 "준비 안 됨"으로 답한다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L922-L930](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L922-L930) | (pending) | pending |
| F26.2b | Charlie | `f26_2b` | F26.2b: 처리 가능이 "준비 안 됨"인 동안에도 살아 있음 신호는 분리되어 유지된다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L931-L939](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L931-L939) | (pending) | pending |
| F26.2c | Charlie | `f26_2c` | F26.2c: 무대 뒤의 자동 복귀 시도가 운영 화면에 표식으로 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L940-L948](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L940-L948) | (pending) | pending |
| F26.3 | Charlie | `f26_3` | F26.3: 살아 있음 본문에 버전·가동 시간·빌드 표식이 함께 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L949-L958](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L949-L958) | (pending) | pending |
| F26.4 | Charlie | `f26_4` | F26.4: 알림 구독이 끊겼다가 다시 붙으면 그 사이의 변경분을 따라잡는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L959-L968](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L959-L968) | (pending) | pending |
| F26.5a | Charlie | `f26_5a` | F26.5a: cc-lb가 재시작했음을 분명한 표식으로 알린다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L969-L977](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L969-L977) | (pending) | pending |
| F26.5b | Charlie | `f26_5b` | F26.5b: 재시작 표식이 시간과 함께 감사 기록에 남는다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L978-L986](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L978-L986) | (pending) | pending |
| F26.6 | Charlie | `f26_6` | F26.6: 지금 진행 중인 호출 수가 같은 화면에서 게이지로 보인다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L987-L996](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L987-L996) | (pending) | pending |
| F26.7 | Charlie | `f26_7` | F26.7: 점진적 종료 중에는 살아 있음과 처리 가능이 따로 답한다 | [cc-lb-true-bdd-1-team-traffic-v5.2.md#L997-L1005](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-1-team-traffic-v5.2.md#L997-L1005) | (pending) | pending |

## Writer W2 (credential/incident)

| ID | Persona | fn_name | Korean source title (verbatim from v5.2) | Source file#Lrange | Target EN title | Status |
|---|---|---|---|---|---|---|
| F5.1 | Charlie | `f5_1` | F5.1: 만료가 임박한 자격증명을 cc-lb이 자동으로 갱신한다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L29-L36](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L29-L36) | (pending) | pending |
| F5.2 | Charlie | `f5_2` | F5.2: 자동 갱신이 반복 실패하면 운영자에게 알린다 (v4 F5.2 split — 알림 규칙) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L37-L42](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L37-L42) | (pending) | pending |
| F5.3 | Charlie | `f5_3` | F5.3: 자동 갱신이 반복 실패하면 cc-lb은 백오프 간격을 늘린다 (v4 F5.2 split — 백오프 규칙) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L43-L48](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L43-L48) | (pending) | pending |
| F5.4 | Charlie | `f5_4` | F5.4: 잘못된 자격증명은 등록 단계에서 거부된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L49-L55](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L49-L55) | (pending) | pending |
| F5.5 | Charlie | `f5_5` | F5.5: 자격증명을 회수하면 그 자격증명을 쓰던 모든 호출이 즉시 멈춘다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L56-L63](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L56-L63) | (pending) | pending |
| F5.6 | Charlie | `f5_6` | F5.6: 회수된 자격증명에 대한 감사 기록은 그대로 남는다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L64-L70](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L64-L70) | (pending) | pending |
| F5.7 | Charlie | `f5_7` | F5.7: 자격증명 보호 키의 권한이 흐트러지면 운영자가 알아챈다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L71-L77](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L71-L77) | (pending) | pending |
| F5.8 | Charlie | `f5_8` | F5.8: 자격증명의 상태가 한 화면에서 사람이 읽을 수 있게 보인다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L78-L84](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L78-L84) | (pending) | pending |
| F5.9 | Charlie | `f5_9` | F5.9: 같은 자격증명을 두 명이 동시에 고치려 하면 한쪽만 받아들여진다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L85-L97](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L85-L97) | (pending) | pending |
| F7.1 | Charlie | `f7_1` | F7.1: 비상 차단을 켜면 모든 호출이 거부된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L98-L104](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L98-L104) | (pending) | pending |
| F7.2 | Charlie | `f7_2` | F7.2: 비상 차단을 풀면 정상 상태로 돌아온다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L105-L111](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L105-L111) | (pending) | pending |
| F7.3 | Charlie | `f7_3` | F7.3: cc-lb이 재시작되어도 비상 차단 상태는 유지된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L112-L118](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L112-L118) | (pending) | pending |
| F7.4 | Charlie | `f7_4` | F7.4: 비상 차단 중에도 관리 화면과 대시보드는 작동한다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L119-L125](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L119-L125) | (pending) | pending |
| F7.5 | Charlie | `f7_5` | F7.5: 차단으로 거부된 응답에는 "운영자가 일시 차단" 의미가 명시된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L126-L132](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L126-L132) | (pending) | pending |
| F7.6 | Charlie | `f7_6` | F7.6: 발동과 해제는 두 단계 확인을 요구한다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L133-L140](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L133-L140) | (pending) | pending |
| F7.7 | Charlie | `f7_7` | F7.7: 비상 차단의 발동·해제 사유가 감사 대상이 된다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L141-L153](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L141-L153) | (pending) | pending |
| F8.1 | Charlie | `f8_1` | F8.1: 위로가 일시적으로 느려지면 사용자에게 지연을 알린다 (v4 F8.1 split — 사용자 UX 규칙) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L154-L159](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L154-L159) | (pending) | pending |
| F8.2 | Charlie | `f8_2` | F8.2: 위로가 느려지면 운영자 대시보드에 사유가 표시된다 (v4 F8.1 split — 운영자 가시성 규칙) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L160-L165](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L160-L165) | (pending) | pending |
| F8.3 | Charlie | `f8_3` | F8.3: Anthropic의 시간당 한도 초과 응답은 사용자에게 그대로 전달된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L166-L172](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L166-L172) | (pending) | pending |
| F8.4 | Charlie | `f8_4` | F8.4: 같은 자격증명이 연속으로 실패하면 잠시 그 자격증명만 막는다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L173-L179](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L173-L179) | (pending) | pending |
| F8.5 | Charlie | `f8_5` | F8.5: 잠시 막아둔 동안의 새 호출은 빠르게 거부된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L180-L186](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L180-L186) | (pending) | pending |
| F8.6 | Charlie | `f8_6` | F8.6: Anthropic이 5xx로 답하면 사용자에게 일시 장애를 안내한다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L187-L193](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L187-L193) | (pending) | pending |
| F8.7 | Charlie | `f8_7` | F8.7: 한 위로가 죽으면 다른 위로로 자동 우회한다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L194-L200](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L194-L200) | (pending) | pending |
| F8.8 | Charlie | `f8_8` | F8.8: 모든 위로가 동시에 죽으면 일관된 응답을 돌려준다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L201-L207](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L201-L207) | (pending) | pending |
| F8.9 | Charlie | `f8_9` | F8.9: 라우팅 추적이 너무 길어지면 잘림 표시와 함께 보인다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L208-L214](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L208-L214) | (pending) | pending |
| F8.10 | Charlie | `f8_10` | F8.10: backpressure가 걸리면 새 호출은 친절히 거부된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L215-L221](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L215-L221) | (pending) | pending |
| F8.11 | Charlie | `f8_11` | F8.11: 위로 별로 동시 호출 격벽이 따로 적용된다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L222-L228](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L222-L228) | (pending) | pending |
| F8.12 | Charlie | `f8_12` | F8.12: 안전한 호출만 자동으로 다시 시도된다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L229-L235](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L229-L235) | (pending) | pending |
| F8.13 | Charlie | `f8_13` | F8.13: Anthropic이 보낸 한도 안내 헤더가 사용자에게 그대로 전달된다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L236-L248](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L236-L248) | (pending) | pending |
| F10.1 | Alice | `f10_1` | F10.1: 운영자가 브라우저로 Anthropic에 동의한다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L249-L255](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L249-L255) | (pending) | pending |
| F10.2 | Alice | `f10_2` | F10.2: 되돌아옴은 cc-lb이 검증한 뒤에만 받아들인다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L256-L262](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L256-L262) | (pending) | pending |
| F10.3 | Alice | `f10_3` | F10.3: 동의가 끝나면 자격증명이 등록되고 활성으로 표시된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L263-L269](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L263-L269) | (pending) | pending |
| F10.4 | Alice | `f10_4` | F10.4: 잘못된 되돌아옴 주소는 거부된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L270-L276](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L270-L276) | (pending) | pending |
| F10.5 | Alice | `f10_5` | F10.5: 동의 세션을 가리키는 표식이 빠지거나 바뀌면 거부된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L277-L283](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L277-L283) | (pending) | pending |
| F10.6 | Alice | `f10_6` | F10.6: 운영자가 동의를 취소하면 자격증명은 만들어지지 않는다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L284-L290](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L284-L290) | (pending) | pending |
| F10.7 | Alice | `f10_7` | F10.7: 두 운영자가 동시에 OAuth 동의를 진행해도 서로 섞이지 않는다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L291-L297](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L291-L297) | (pending) | pending |
| F10.8 | Alice | `f10_8` | F10.8: 동의 세션은 일정 시간이 지나면 만료된다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L298-L310](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L298-L310) | (pending) | pending |
| F11A.1 | Charlie | `f11a_1` | F11A.1: 5시간 한도의 현재 사용량이 보인다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L311-L317](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L311-L317) | (pending) | pending |
| F11A.2 | Charlie | `f11a_2` | F11A.2: 7일 한도의 현재 사용량이 보인다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L318-L324](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L318-L324) | (pending) | pending |
| F11A.3 | Charlie | `f11a_3` | F11A.3: 기본 한도와 초과 한도가 서로 구분되어 보인다 (v4 F11.14 split — 구분 표시 규칙) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L325-L330](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L325-L330) | (pending) | pending |
| F11A.4 | Charlie | `f11a_4` | F11A.4: 초과 한도에 들어선 사실이 운영자에게 명시된다 (v4 F11.14 split — 진입 상태 규칙) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L331-L337](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L331-L337) | (pending) | pending |
| F11A.5 | Charlie | `f11a_5` | F11A.5: 사용량이 80%에 임박하면 화면에서 경고로 보인다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L338-L344](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L338-L344) | (pending) | pending |
| F11A.6 | Charlie | `f11a_6` | F11A.6: 운영자가 한도 메타 정보를 즉시 새로고침한다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L345-L351](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L345-L351) | (pending) | pending |
| F11A.7 | Charlie | `f11a_7` | F11A.7: 한도 합치기 방식을 운영자가 고른다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L352-L358](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L352-L358) | (pending) | pending |
| F11A.8 | Charlie | `f11a_8` | F11A.8: 5시간 창 안에서도 시간대별 사용량이 따로 보인다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L359-L365](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L359-L365) | (pending) | pending |
| F11A.9 | Charlie | `f11a_9` | F11A.9: 자격증명에 붙은 사용 제한 조건이 사람이 읽을 수 있게 보인다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L366-L372](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L366-L372) | (pending) | pending |
| F11A.10 | Charlie | `f11a_10` | F11A.10: 한도가 모자라면 부족분이 운영자에게 보인다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L373-L379](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L373-L379) | (pending) | pending |
| F11A.11 | Charlie | `f11a_11` | F11A.11: 자격증명을 지워도 마지막으로 본 한도 값은 잠시 남는다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L380-L392](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L380-L392) | (pending) | pending |
| F11B.1 | Charlie | `f11b_1` | F11B.1: Anthropic 한도가 활성 상태로 유지되도록 주기적으로 알린다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L393-L400](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L393-L400) | (pending) | pending |
| F11B.2 | Charlie | `f11b_2` | F11B.2: 미리 데움 호출은 사용량·비용에 반영되지 않는다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L401-L407](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L401-L407) | (pending) | pending |
| F11B.3 | Charlie | `f11b_3` | F11B.3: Anthropic이 자주 보내지 말라고 하면 폴링 간격을 늘린다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L408-L414](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L408-L414) | (pending) | pending |
| F11B.4 | Charlie | `f11b_4` | F11B.4: 여러 복제 노드 중 한 곳만 미리 데움을 수행한다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L415-L421](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L415-L421) | (pending) | pending |
| F11B.5 | Charlie | `f11b_5` | F11B.5: 미리 데움이 실패하면 백오프를 두고 다시 시도한다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L422-L428](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L422-L428) | (pending) | pending |
| F11B.6 | Charlie | `f11b_6` | F11B.6: 미리 데움 상태를 운영자가 화면에서 본다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L429-L435](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L429-L435) | (pending) | pending |
| F11B.7 | Charlie | `f11b_7` | F11B.7: 끊긴 동안의 변경은 다시 붙은 뒤 reconciler가 따라잡는다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L436-L442](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L436-L442) | (pending) | pending |
| F11B.8 | Charlie | `f11b_8` | F11B.8: 비상 차단 중에는 미리 데움이 멈춘다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L443-L449](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L443-L449) | (pending) | pending |
| F11B.9 | Charlie | `f11b_9` | F11B.9: 미리 데움 대상 위로와 다음 시도 시각을 운영자가 본다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L450-L456](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L450-L456) | (pending) | pending |
| F11B.10 | Charlie | `f11b_10` | F11B.10: 미리 데움은 OAuth 자격증명에서만 동작한다는 사실이 운영자에게 보인다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L457-L463](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L457-L463) | (pending) | pending |
| F11B.11 | Charlie | `f11b_11` | F11B.11: Anthropic 위로 주소 변동이 운영자에게 보이고 호출은 끊기지 않는다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L464-L476](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L464-L476) | (pending) | pending |
| F11C.1 | Charlie | `f11c_1` | F11C.1: 호환성 캐시가 한 시간 주기로 자동 갱신된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L477-L483](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L477-L483) | (pending) | pending |
| F11C.2 | Charlie | `f11c_2` | F11C.2: 조직 메타가 한도 차이를 추적할 수 있게 보관된다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L484-L490](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L484-L490) | (pending) | pending |
| F11C.3 | Charlie | `f11c_3` | F11C.3: 운영자가 구독 메타를 직접 새로고침한다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L491-L497](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L491-L497) | (pending) | pending |
| F11C.4 | Charlie | `f11c_4` | F11C.4: 프로세스 재시작 표식은 한도 spike로 오해되지 않는다 | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L498-L504](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L498-L504) | (pending) | pending |
| F11C.5 | Charlie | `f11c_5` | F11C.5: 호환성 캐시 갱신이 실패하면 옛 값을 그대로 유지한다 (v4 F11.26 split — 옛 값 유지 규칙) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L505-L510](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L505-L510) | (pending) | pending |
| F11C.6 | Charlie | `f11c_6` | F11C.6: 호환성 캐시의 마지막 성공 갱신 시각이 운영자에게 보인다 (v4 F11.26 split — 신선도 표시 규칙) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L511-L516](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L511-L516) | (pending) | pending |
| F11C.7 | Charlie | `f11c_7` | F11C.7: 마지막 시도 시각과 마지막 성공 시각이 따로 보인다 (신규 P3) | [cc-lb-true-bdd-2-credential-incident-v5.2.md#L517-L523](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-2-credential-incident-v5.2.md#L517-L523) | (pending) | pending |

## Writer W3 (policy/plugin)

| ID | Persona | fn_name | Korean source title (verbatim from v5.2) | Source file#Lrange | Target EN title | Status |
|---|---|---|---|---|---|---|
| F9.1 | Alice | `f9_1` | F9.1 팀에 정책을 부착하면 다음 호출부터 즉시 적용된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L27-L33](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L27-L33) | (pending) | pending |
| F9.2 | Alice | `f9_2` | F9.2 정책을 떼면 그 후 호출에만 영향이 간다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L34-L41](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L34-L41) | (pending) | pending |
| F9.3 | Alice | `f9_3` | F9.3 형식이 잘못된 정책은 저장 단계에서 거부된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L42-L49](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L42-L49) | (pending) | pending |
| F9.4 | Alice | `f9_4` | F9.4 정책 변경은 cc-lb 재시작 없이 다음 호출에 반영된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L50-L56](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L50-L56) | (pending) | pending |
| F9.5 | Alice | `f9_5` | F9.5 한 팀의 정책 변경은 다른 팀의 호출에 영향을 주지 않는다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L57-L63](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L57-L63) | (pending) | pending |
| F9.6 | Alice | `f9_6` | F9.6 전역 규칙이 먼저, 팀 규칙이 나중에 적용된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L64-L71](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L64-L71) | (pending) | pending |
| F9.8 | Alice | `f9_8` | F9.8 운영자는 정책을 부착하기 전 미리 흐름을 검증할 수 있다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L72-L86](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L72-L86) | (pending) | pending |
| F12.1 | Bob | `f12_1` | F12.1 같은 인장의 플러그인을 두 번 올리면 두 번째는 거부된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L87-L93](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L87-L93) | (pending) | pending |
| F12.2 | Bob | `f12_2` | F12.2 플러그인은 라벨과 버전으로 구분되어 관리된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L94-L100](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L94-L100) | (pending) | pending |
| F12.3 | Bob | `f12_3` | F12.3 운영자가 플러그인 줄의 순서를 바꾸면 다음 호출부터 새 순서로 동작한다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L101-L107](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L101-L107) | (pending) | pending |
| F12.4 | Bob | `f12_4` | F12.4 어디선가 쓰이는 플러그인은 삭제할 수 없다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L108-L114](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L108-L114) | (pending) | pending |
| F12.5 | Bob | `f12_5` | F12.5 한 플러그인 줄에 넣을 수 있는 플러그인 수를 넘기면 거부된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L115-L121](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L115-L121) | (pending) | pending |
| F12.6 | Bob | `f12_6` | F12.6 한 분에 올릴 수 있는 플러그인 수는 정해져 있다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L122-L128](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L122-L128) | (pending) | pending |
| F12.7 | Bob | `f12_7` | F12.7 현재 cc-lb은 응답을 다듬는 슬롯의 플러그인만 받는다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L129-L135](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L129-L135) | (pending) | pending |
| F12.8 | Bob | `f12_8` | F12.8 인장이 검증되지 않는 플러그인은 등록 단계에서 거부된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L136-L143](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L136-L143) | (pending) | pending |
| F12.9 | Bob | `f12_9` | F12.9 한 플러그인이 자기 한도 안에서 자원을 다 써도 다른 플러그인은 영향을 받지 않는다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L144-L151](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L144-L151) | (pending) | pending |
| F12.10a | Bob | `f12_10a` | F12.10a 플러그인 줄 변경의 준비 단계에서는 옛 묶음이 그대로 쓰인다 (v5 분리) | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L152-L158](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L152-L158) | (pending) | pending |
| F12.10b | Bob | `f12_10b` | F12.10b 적용 시점에 새 묶음으로 한꺼번에 바뀐다 (v5 분리) | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L159-L165](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L159-L165) | (pending) | pending |
| F12.10c | Bob | `f12_10c` | F12.10c 적용이 끝나면 더 이상 쓰이지 않는 옛 부착이 보관소에서 정리된다 (v5 분리) | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L166-L173](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L166-L173) | (pending) | pending |
| F12.11 | Bob | `f12_11` | F12.11 운영자는 새 플러그인을 플러그인 줄 안에서 위치를 지정해 끼울 수 있다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L174-L180](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L174-L180) | (pending) | pending |
| F12.12 | Bob | `f12_12` | F12.12 적용 전 사전 점검에서 슬롯 종류가 안 맞으면 변경이 거부된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L181-L188](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L181-L188) | (pending) | pending |
| F12.13 | Bob | `f12_13` | F12.13 어디에도 부착되지 않은 플러그인 줄 항목은 별도 목록으로 보인다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L189-L196](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L189-L196) | (pending) | pending |
| F12.14 | Bob | `f12_14` | F12.14 운영자는 플러그인 줄 안의 항목을 한 번에 다시 정렬할 수 있다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L197-L203](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L197-L203) | (pending) | pending |
| F12.15 | Bob | `f12_15` | F12.15 정원이 하나로 정해진 슬롯에는 두 번째 부착이 거부된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L204-L217](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L204-L217) | (pending) | pending |
| F21.1 | Alice | `f21_1` | F21.1 호출 한 건이 누구의 어떤 모델 호출인지 별도로 셈해진다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L218-L224](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L218-L224) | (pending) | pending |
| F21.2 | Alice | `f21_2` | F21.2 한 호출이 시작과 끝, 또는 오류로 끝났음이 한 번씩 기록된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L225-L231](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L225-L231) | (pending) | pending |
| F21.3 | Alice | `f21_3` | F21.3 스트리밍 응답의 부분 전송 횟수가 운영 대시보드에 모인다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L232-L238](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L232-L238) | (pending) | pending |
| F21.4 | Alice | `f21_4` | F21.4 인증 실패는 사유 종류별로 셈해진다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L239-L245](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L239-L245) | (pending) | pending |
| F21.5 | Alice | `f21_5` | F21.5 백프레셔로 호출이 떨궈지면 그 사실이 운영 대시보드에 남는다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L246-L252](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L246-L252) | (pending) | pending |
| F21.7 | Alice | `f21_7` | F21.7 응답을 받은 사람도 그 호출의 식별자를 본다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L253-L259](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L253-L259) | (pending) | pending |
| F21.8 | Alice | `f21_8` | F21.8 운영자가 정한 외부 관찰 도구로 같은 사건이 전송된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L260-L266](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L260-L266) | (pending) | pending |
| F21.9 | Alice | `f21_9` | F21.9 한 호출의 식별자는 감사 기록·운영 로그·외부 추적·응답까지 동일하다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L267-L273](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L267-L273) | (pending) | pending |
| F21.11 | Alice | `f21_11` | F21.11 한 호출의 단계별 소요 시간이 사용량 보고에 함께 들어간다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L274-L280](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L274-L280) | (pending) | pending |
| F21.12 | Alice | `f21_12` | F21.12 외부 관찰 도구로의 사건 전송이 실패해도 호출 처리에는 영향이 없다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L281-L287](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L281-L287) | (pending) | pending |
| F21.13 | Alice | `f21_13` | F21.13 관찰 사건 큐가 차면 떨궈진 묶음이 별도로 보인다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L288-L294](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L288-L294) | (pending) | pending |
| F21.14 | Alice | `f21_14` | F21.14 한 팀의 관찰 사슬이 자원을 다 써도 다른 팀의 관찰 사슬은 영향을 받지 않는다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L295-L308](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L295-L308) | (pending) | pending |
| F25.1 | Bob | `f25_1` | F25.1 cc-lb이 지원하는 플러그인 형식 안에서 만든 플러그인은 받아들여진다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L309-L315](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L309-L315) | (pending) | pending |
| F25.2 | Bob | `f25_2` | F25.2 cc-lb이 지원하지 않는 플러그인 형식은 거부된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L316-L322](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L316-L322) | (pending) | pending |
| F25.3 | Bob | `f25_3` | F25.3 같은 내용의 플러그인은 같은 인장을 가진다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L323-L329](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L323-L329) | (pending) | pending |
| F25.5 | Bob | `f25_5` | F25.5 사전 검사를 통과하지 못한 플러그인은 등록 단계에서 막힌다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L330-L336](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L330-L336) | (pending) | pending |
| F25.7 | Bob | `f25_7` | F25.7 작성자는 함수별로 실패 시 동작을 미리 정해 둘 수 있다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L337-L343](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L337-L343) | (pending) | pending |
| F25.8 | Bob | `f25_8` | F25.8 cc-lb이 지원하는 플러그인 형식이 여러 세대일 때 가장 잘 맞는 세대로 합의된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L344-L351](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L344-L351) | (pending) | pending |
| F25.9 | Bob | `f25_9` | F25.9 작성자는 cc-lb이 제공하는 보조 기능을 통해서만 외부와 통신한다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L352-L358](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L352-L358) | (pending) | pending |
| F25.11 | Bob | `f25_11` | F25.11 등록된 플러그인은 자기 이름·버전·할 수 있는 일을 운영자에게 보인다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L359-L365](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L359-L365) | (pending) | pending |
| F25.12 | Bob | `f25_12` | F25.12 등록된 플러그인이 많아도 cc-lb 재시작 시 부팅을 지연시키지 않는다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L366-L372](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L366-L372) | (pending) | pending |
| F25.13 | Bob | `f25_13` | F25.13 플러그인의 본 단계가 정해진 시간을 넘기면 호출은 대체 동작으로 마무리된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L373-L380](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L373-L380) | (pending) | pending |
| F25.14 | Bob | `f25_14` | F25.14 cc-lb 경계에서 비밀은 플러그인에 닿기 전 가려져 전달된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L381-L388](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L381-L388) | (pending) | pending |
| F25.15 | Bob | `f25_15` | F25.15 플러그인이 더 낮은 세대로 강제 합의를 요청하면 거부된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L389-L396](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L389-L396) | (pending) | pending |
| F25.16 | Bob | `f25_16` | F25.16 능력 선언에서 슬롯이 요구하는 능력이 빠진 플러그인은 거부된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L397-L404](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L397-L404) | (pending) | pending |
| F25.17 | Bob | `f25_17` | F25.17 cc-lb이 받는 형식 세대 범위 바깥의 플러그인은 거부된다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L405-L419](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L405-L419) | (pending) | pending |
| F27.3 | Bob | `f27_3` | F27.3 짧은 시간 안의 플러그인 업로드 폭주는 잠시 멈춰진다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L420-L426](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L420-L426) | (pending) | pending |
| F27.4 | Bob | `f27_4` | F27.4 비상 차단은 두 단계 확인 후에만 효력이 생긴다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L427-L433](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L427-L433) | (pending) | pending |
| F27.5 | Bob | `f27_5` | F27.5 관리자 세션이 일정 시간을 넘기면 위험 동작 전 다시 신원 확인을 요구한다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L434-L441](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L434-L441) | (pending) | pending |
| F27.6 | Bob | `f27_6` | F27.6 다른 출처에서 시작된 관리자 요청은 의도치 않게 실행되지 않는다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L442-L448](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L442-L448) | (pending) | pending |
| F27.7 | Bob | `f27_7` | F27.7 관리자 토큰 값은 운영 화면 어디에서도 평문으로 보이지 않는다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L449-L462](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L449-L462) | (pending) | pending |
| F29.1 | Charlie | `f29_1` | F29.1 갑작스러운 충돌 메시지에도 비밀은 가려진다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L463-L469](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L463-L469) | (pending) | pending |
| F29.2 | Charlie | `f29_2` | F29.2 운영자가 의도적으로 결함을 주입해 회복력을 시험한다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L470-L476](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L470-L476) | (pending) | pending |
| F29.3a | Charlie | `f29_3a` | F29.3a 외부 연결이 갑자기 끊겨도 정해진 대체 동작으로 호출이 마무리된다 (v5.2 분리) | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L477-L483](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L477-L483) | (pending) | pending |
| F29.3b | Charlie | `f29_3b` | F29.3b 외부 연결 끊김으로 인한 대체 동작은 같은 호출 식별자로 운영 로그와 감사 기록에 남는다 (v5.2 분리) | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L484-L490](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L484-L490) | (pending) | pending |
| F29.4 | Charlie | `f29_4` | F29.4 한도 엔진이 차갑게 다시 시작해도 진행 중이던 계산은 이어진다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L491-L497](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L491-L497) | (pending) | pending |
| F29.5 | Charlie | `f29_5` | F29.5 결함 주입을 한 팀에만 한정하면 다른 팀의 호출은 영향을 받지 않는다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L498-L504](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L498-L504) | (pending) | pending |
| F29.6 | Charlie | `f29_6` | F29.6 결함 주입을 켜고 끄는 동작 자체가 감사 기록에 남는다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L505-L511](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L505-L511) | (pending) | pending |
| F29.7 | Charlie | `f29_7` | F29.7 결함 주입은 미리 정해진 지점에서만 일어난다 | [cc-lb-true-bdd-3-policy-plugin-v5.2.md#L512-L517](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-3-policy-plugin-v5.2.md#L512-L517) | (pending) | pending |

## Writer W4 (platform/audit/lifecycle/backend/cost/secret)

| ID | Persona | fn_name | Korean source title (verbatim from v5.2) | Source file#Lrange | Target EN title | Status |
|---|---|---|---|---|---|---|
| F13.1 | Dana | `f13_1` | 감사관이 한 운영자의 한 분기 변경을 시간순으로 본다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L80-L86](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L80-L86) | (pending) | pending |
| F13.2 | Dana | `f13_2` | 감사 기록에는 비밀 정보가 한 줄도 들어가 있지 않다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L87-L93](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L87-L93) | (pending) | pending |
| F13.3 | Dana | `f13_3` | 한 번 적힌 감사 기록은 어떤 운영자도 지우거나 고칠 수 없다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L94-L100](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L94-L100) | (pending) | pending |
| F13.4 | Dana | `f13_4` | 감사관이 시간 창과 페이지 단위로 좁혀 본다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L101-L106](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L101-L106) | (pending) | pending |
| F13.5 | Dana | `f13_5` | 보존 기간이 지난 기록만 정확히 정리된다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L107-L115](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L107-L115) | (pending) | pending |
| F13.6 | Dana | `f13_6` | 한 호출의 시작·끝·오류가 같은 추적 표식으로 묶인다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L116-L121](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L116-L121) | (pending) | pending |
| F13.7 | Dana | `f13_7` | 감사 기록을 외부 감사 시스템으로 내보낸다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L122-L127](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L122-L127) | (pending) | pending |
| F13.8 | Dana | `f13_8` | 한 호출의 추적 표식이 audit, 운영 로그, 응답에 모두 같이 찍힌다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L128-L133](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L128-L133) | (pending) | pending |
| F13.9 | Dana | `f13_9` | 내보낸 감사 파일에 변조 증명이 함께 들어 있다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L134-L140](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L134-L140) | (pending) | pending |
| F13.10 | Dana | `f13_10` | 감사관이 한 호출의 모든 관련 기록을 교차 표로 본다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L141-L146](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L141-L146) | (pending) | pending |
| F13.11 | Dana | `f13_11` | 비용과 한도 위반이 같은 호출 줄에 함께 보인다 (신규 P3, split 1/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L147-L152](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L147-L152) | (pending) | pending |
| F13.12 | Dana | `f13_12` | 비용과 한도 위반이 같은 호출 단위로 분기 보고서에 합산된다 (신규 P3, split 2/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L153-L164](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L153-L164) | (pending) | pending |
| F14.1 | Alice | `f14_1` | 운영자가 초안을 저장해도 실제 동작에는 아직 영향이 없다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L165-L170](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L165-L170) | (pending) | pending |
| F14.2 | Alice | `f14_2` | 운영자가 초안을 검증하면 적용 전에 문제가 드러난다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L171-L176](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L171-L176) | (pending) | pending |
| F14.3 | Alice | `f14_3` | 잘못된 설정은 저장 단계에서 거부된다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L177-L183](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L177-L183) | (pending) | pending |
| F14.4 | Alice | `f14_4` | 검증을 통과한 초안만 적용된다 (split 1/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L184-L188](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L184-L188) | (pending) | pending |
| F14.5 | Alice | `f14_5` | 적용된 설정 변경이 적용 이력에 한 줄로 기록된다 (split 2/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L189-L194](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L189-L194) | (pending) | pending |
| F14.6 | Alice | `f14_6` | 운영자가 적용 이력을 시간순으로 본다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L195-L200](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L195-L200) | (pending) | pending |
| F14.7 | Alice | `f14_7` | 운영자가 이전 버전 설정으로 되돌린다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L201-L206](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L201-L206) | (pending) | pending |
| F14.8 | Alice | `f14_8` | 재시작 없이 적용 가능한 항목은 재시작 없이 설정 적용된다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L207-L212](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L207-L212) | (pending) | pending |
| F14.9 | Alice | `f14_9` | 재시작이 필요한 항목은 적용 전에 그 사실이 표시된다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L213-L218](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L213-L218) | (pending) | pending |
| F14.10 | Alice | `f14_10` | 운영자가 적용된 설정을 파일로 내려받는다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L219-L224](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L219-L224) | (pending) | pending |
| F14.11 | Alice | `f14_11` | bootstrap 설정은 첫 부팅에 한 번만 반영된다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L225-L232](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L225-L232) | (pending) | pending |
| F14.12 | Alice | `f14_12` | 적용 안 된 초안은 정해진 기간 뒤 자동으로 비워진다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L233-L239](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L233-L239) | (pending) | pending |
| F14.13 | Alice | `f14_13` | 재시작이 필요한 항목은 어느 항목인지 항목별로 명확히 표시된다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L240-L245](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L240-L245) | (pending) | pending |
| F14.14 | Alice | `f14_14` | 디스크 위 설정 파일이 바뀌면 cc-lb가 그 변경을 알아챈다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L246-L258](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L246-L258) | (pending) | pending |
| F15.1 | Charlie | `f15_1` | 종료 신호 후에는 새 호출을 안 받는다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L259-L264](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L259-L264) | (pending) | pending |
| F15.2 | Charlie | `f15_2` | 종료 중에도 이미 진행 중 호출은 마지막까지 처리된다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L265-L270](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L265-L270) | (pending) | pending |
| F15.3 | Charlie | `f15_3` | 종료 대기 시간이 초과되면 강제로 끝내고 보고한다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L271-L278](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L271-L278) | (pending) | pending |
| F15.4 | Charlie | `f15_4` | 새 인증서 적용해도 진행 중 스트림 안 끊김 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L279-L284](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L279-L284) | (pending) | pending |
| F15.5 | Charlie | `f15_5` | 잘못된 인증서는 적용 전에 거부된다 (split 1/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L285-L291](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L285-L291) | (pending) | pending |
| F15.6 | Charlie | `f15_6` | 잘못된 인증서 거부 사유가 운영자에게 한 줄로 안내된다 (split 2/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L292-L297](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L292-L297) | (pending) | pending |
| F15.7 | Charlie | `f15_7` | 인증서 만료가 가까워지면 운영자에게 미리 알린다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L298-L303](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L298-L303) | (pending) | pending |
| F15.8 | Charlie | `f15_8` | 임시 디버그 로깅이 정해진 시간 뒤 자동으로 꺼진다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L304-L309](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L304-L309) | (pending) | pending |
| F15.9 | Charlie | `f15_9` | 종료 신호의 종류에 따라 종료 의미가 분리된다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L310-L316](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L310-L316) | (pending) | pending |
| F15.10 | Charlie | `f15_10` | 디버그 로깅은 정해진 운영 신호로만 켜진다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L317-L323](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L317-L323) | (pending) | pending |
| F15.11 | Charlie | `f15_11` | 운영 전용 소켓은 외부 호출자의 접근을 거부한다 (신규 P3, split 1/3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L324-L329](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L324-L329) | (pending) | pending |
| F15.12 | Charlie | `f15_12` | 운영 전용 소켓은 같은 머신 안 운영자의 관리 호출을 정상 수신한다 (신규 P3, split 2/3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L330-L335](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L330-L335) | (pending) | pending |
| F15.13 | Charlie | `f15_13` | 외부 호출용 포트와 운영 전용 소켓은 종료 진행 중에도 분리된 상태로 동작한다 (신규 P3, split 3/3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L336-L342](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L336-L342) | (pending) | pending |
| F15.14 | Charlie | `f15_14` | 복제 노드가 종료 완료된 사실이 다른 복제 노드에 표식으로 남는다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L343-L354](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L343-L354) | (pending) | pending |
| F17.1 | Charlie | `f17_1` | 한 복제 노드의 변경이 다른 복제 노드에 즉시 알려진다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L355-L360](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L355-L360) | (pending) | pending |
| F17.2 | Charlie | `f17_2` | 여러 복제 노드 중 단 하나만 작업을 수행한다 — 자격증명 미리 데움 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L361-L366](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L361-L366) | (pending) | pending |
| F17.3 | Charlie | `f17_3` | 작업을 수행하던 복제 노드가 사라지면 다른 복제 노드가 인계받는다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L367-L372](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L367-L372) | (pending) | pending |
| F17.4 | Charlie | `f17_4` | 설정 변경은 모든 복제 노드가 같은 시점에 같은 값을 본다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L373-L377](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L373-L377) | (pending) | pending |
| F17.5 | Charlie | `f17_5` | 같은 줄을 동시에 고치려 하면 한쪽만 성공한다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L378-L383](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L378-L383) | (pending) | pending |
| F17.6 | Charlie | `f17_6` | 두 복제 노드가 동시에 단일 수행권을 들었다고 믿어도 한쪽만 작업을 계속한다 (신규 P3, split 1/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L384-L389](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L384-L389) | (pending) | pending |
| F17.7 | Charlie | `f17_7` | 단일 수행권 자기 점검 결과가 운영 로그와 지표에 남는다 (신규 P3, split 2/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L390-L395](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L390-L395) | (pending) | pending |
| F17.8 | Charlie | `f17_8` | 운영자가 어느 복제 노드가 살아 있는지 한 화면에서 본다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L396-L401](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L396-L401) | (pending) | pending |
| F17.9 | Charlie | `f17_9` | 복제 노드 식별자가 손상되면 안전하게 새 식별자가 발급된다 (신규 P3, split 1/3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L402-L407](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L402-L407) | (pending) | pending |
| F17.10 | Charlie | `f17_10` | 옛 식별자로 묶여 있던 작업은 다른 복제 노드로 인계되거나 만료된다 (신규 P3, split 2/3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L408-L414](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L408-L414) | (pending) | pending |
| F17.11 | Charlie | `f17_11` | 옛 식별자와 새 식별자가 같은 호출에 동시에 쓰이지 않는다 (신규 P3, split 3/3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L415-L420](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L415-L420) | (pending) | pending |
| F17.12 | Charlie | `f17_12` | 두 저장 백엔드에서 같은 운영자 시나리오가 같은 결과를 낸다 (F22-merged) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L421-L426](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L421-L426) | (pending) | pending |
| F17.13 | Charlie | `f17_13` | 두 저장 백엔드의 일관성 시나리오가 모두 통과한다 (F22-merged) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L427-L431](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L427-L431) | (pending) | pending |
| F17.14 | Charlie | `f17_14` | 저장 백엔드 종류를 잘못 바꾸면 부팅이 명확히 거부된다 (F22-merged) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L432-L437](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L432-L437) | (pending) | pending |
| F17.15 | Charlie | `f17_15` | 두 저장 백엔드에서 같은 운영자 동작이 같은 수와 같은 종류의 감사 줄을 남긴다 (F22-merged, split 1/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L438-L442](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L438-L442) | (pending) | pending |
| F17.16 | Charlie | `f17_16` | 두 저장 백엔드 모두에서 감사 기록에 비밀이 평문으로 보이지 않는다 (F22-merged, split 2/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L443-L448](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L443-L448) | (pending) | pending |
| F17.17 | Charlie | `f17_17` | 다운그레이드 이동은 부팅 단계에서 거부된다 (F22-merged) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L449-L454](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L449-L454) | (pending) | pending |
| F17.18 | Charlie | `f17_18` | 두 운영자가 같은 줄을 동시에 고치는 상황이 두 저장 백엔드 모두에서 같은 결정을 낸다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L455-L467](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L455-L467) | (pending) | pending |
| F18.1 | Dana | `f18_1` | 운영자가 모델별 현재 가격을 한 화면에서 본다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L468-L472](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L468-L472) | (pending) | pending |
| F18.2 | Dana | `f18_2` | 호출당 비용이 카탈로그 가격으로 정확히 계산된다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L473-L478](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L473-L478) | (pending) | pending |
| F18.3 | Dana | `f18_3` | 분기 비용 보고서가 팀별로 정확히 합산된다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L479-L483](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L479-L483) | (pending) | pending |
| F18.4 | Dana | `f18_4` | 가격이 바뀌면 그 시점 이후 새 호출부터 새 가격이 적용된다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L484-L489](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L484-L489) | (pending) | pending |
| F18.5 | Dana | `f18_5` | 캐시가 들어맞은 호출은 비용 절감이 별도 항목으로 보인다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L490-L495](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L490-L495) | (pending) | pending |
| F18.6 | Dana | `f18_6` | 가격 변경 이력이 시간순으로 보존된다 (split 1/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L496-L500](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L496-L500) | (pending) | pending |
| F18.7 | Dana | `f18_7` | 과거 호출 비용 계산이 가격 이력의 그 시점 단가와 정합한다 (split 2/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L501-L506](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L501-L506) | (pending) | pending |
| F18.8 | Dana | `f18_8` | 캐시를 처음 만든 호출과 캐시를 다시 읽은 호출이 단가가 따로 계산된다 (신규 P3, split 1/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L507-L511](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L507-L511) | (pending) | pending |
| F18.9 | Dana | `f18_9` | 캐시 생성·재사용 단가의 합산이 분기 보고서 총합과 한 토큰도 안 어긋난다 (신규 P3, split 2/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L512-L516](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L512-L516) | (pending) | pending |
| F18.10 | Dana | `f18_10` | 토큰 수를 정확히 못 셀 때는 추정 표시와 함께 보인다 (신규 P3, split 1/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L517-L521](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L517-L521) | (pending) | pending |
| F18.11 | Dana | `f18_11` | 추정으로 보고된 호출은 사유 기록과 별도 조회가 따로 제공된다 (신규 P3, split 2/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L522-L527](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L522-L527) | (pending) | pending |
| F18.12 | Dana | `f18_12` | 위로 이름이 바뀌어도 이전 사용량은 그대로 보인다 (F23-merged) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L528-L533](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L528-L533) | (pending) | pending |
| F18.13 | Dana | `f18_13` | 위로를 삭제해도 그 이전 사용량은 보존된다 (F23-merged) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L534-L539](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L534-L539) | (pending) | pending |
| F18.14 | Dana | `f18_14` | 두 위로를 하나로 합치면 사용량이 정확히 합산된다 (F23-merged) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L540-L545](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L540-L545) | (pending) | pending |
| F18.15 | Dana | `f18_15` | 사용량 보고서에 위로 이름 변경이 함께 표시된다 (F23-merged) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L546-L557](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L546-L557) | (pending) | pending |
| F20.1 | Dana | `f20_1` | 저장된 모든 비밀은 평문으로 디스크에 남아 있지 않다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L558-L563](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L558-L563) | (pending) | pending |
| F20.2 | Dana | `f20_2` | 변조하면 즉시 들통난다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L564-L570](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L564-L570) | (pending) | pending |
| F20.3 | Dana | `f20_3` | 마스터 키가 사라지면 비밀은 영영 복구되지 않는다 — 의도된 동작 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L571-L576](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L571-L576) | (pending) | pending |
| F20.4 | Dana | `f20_4` | 잘못된 마스터 키로는 cc-lb가 부팅 자체를 거부한다 (split 1/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L577-L582](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L577-L582) | (pending) | pending |
| F20.5 | Dana | `f20_5` | 잘못된 마스터 키 부팅 시도에서 어떤 비밀도 평문으로 한 번도 읽히지 않는다 (split 2/2) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L583-L588](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L583-L588) | (pending) | pending |
| F20.6 | Dana | `f20_6` | 키 회전 후에도 이전에 저장된 비밀은 그대로 읽힌다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L589-L595](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L589-L595) | (pending) | pending |
| F20.7 | Dana | `f20_7` | 마스터 키 파일의 권한이 너무 느슨하면 부팅이 거부된다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L596-L601](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L596-L601) | (pending) | pending |
| F20.8 | Dana | `f20_8` | 비밀이 알려진 자리 패턴을 따르는 값은 종류별로 가려진 표시로 보인다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L602-L608](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L602-L608) | (pending) | pending |
| F20.9 | Dana | `f20_9` | 비정상 종료 메시지에도 비밀 정보는 가려진 표시로만 나온다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L609-L615](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L609-L615) | (pending) | pending |
| F20.10 | Dana | `f20_10` | 마스터 키 회전이 진행 중 호출을 끊지 않는다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L616-L623](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L616-L623) | (pending) | pending |
| F20.11 | Dana | `f20_11` | 비정상 종료 추적의 변수 값에도 비밀이 가려진 표시로만 나온다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L624-L629](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L624-L629) | (pending) | pending |
| F20.12 | Dana | `f20_12` | 호출자에게 돌아가는 응답 헤더에서 비밀이 알려진 자리 패턴을 따르는 값은 가려진 표시로만 보인다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L630-L636](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L630-L636) | (pending) | pending |
| F20.13 | Dana | `f20_13` | 같은 비밀이 다른 위로에 저장돼 있어도 서로 풀어 쓸 수 없다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L637-L649](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L637-L649) | (pending) | pending |
| F24.1 | Alice | `f24_1` | 외부 가격 소스가 정상이면 새 가격을 가져온다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L650-L655](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L650-L655) | (pending) | pending |
| F24.2 | Alice | `f24_2` | 외부 가격 소스가 일시 장애여도 마지막 가격이 그대로 쓰인다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L656-L662](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L656-L662) | (pending) | pending |
| F24.3 | Alice | `f24_3` | 외부 가격 소스가 오래 죽어 있으면 운영자에게 알린다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L663-L668](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L663-L668) | (pending) | pending |
| F24.4 | Alice | `f24_4` | 가격 소스에 세 번 실패하면 디스크에 남겨 둔 가격으로, 그것도 없으면 비용 보고를 보류한다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L669-L675](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L669-L675) | (pending) | pending |
| F24.5 | Alice | `f24_5` | 가격 카탈로그가 통째로 망가지면 그 사실을 안전하게 알린다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L676-L681](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L676-L681) | (pending) | pending |
| F24.6 | Alice | `f24_6` | 운영자가 가격 카탈로그의 마지막 갱신 시각을 본다 | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L682-L687](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L682-L687) | (pending) | pending |
| F24.7 | Alice | `f24_7` | 가격 카탈로그의 출처 증명이 맞지 않으면 새 가격을 쓰지 않는다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L688-L695](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L688-L695) | (pending) | pending |
| F24.8 | Alice | `f24_8` | 카탈로그에 없는 모델 호출은 정해진 폴백 방식으로 보고된다 (신규 P3) | [cc-lb-true-bdd-4-platform-audit-v5.2.md#L696-L702](file:///home/bhyoo/cc-lb-bdd/cc-lb-true-bdd-4-platform-audit-v5.2.md#L696-L702) | (pending) | pending |

## Summary

- **Per-writer counts**:
  - W1: 98
  - W2: 66
  - W3: 63
  - W4: 94
  - Total: 321
- **Pending count**: 319
- **OoS-manual count**: 2
- **Persona distribution**:
  - Alice: 121
  - Bob: 52
  - Charlie: 108
  - Dana: 40
- **Fast subset count**: 33
